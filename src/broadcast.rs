use std::{
    env,
    io::{self, Error, ErrorKind, Read, Write},
    net::{SocketAddr, TcpStream, UdpSocket},
    sync::{
        mpsc::{sync_channel, Receiver, SyncSender},
        Arc,
    },
    thread,
    time::Duration,
};

use bitcoin::{
    hashes::Hash,
    OutPoint,
    ScriptBuf,
    Txid,
};

use mio::{
    net::{TcpListener, TcpStream as MioTcpStream},
    Events,
    Interest,
    Poll,
    Token,
};

// -----------------------------------------------------------------------------
// Canonical finalized settlement
// -----------------------------------------------------------------------------

pub struct FinalizedSettlement {
    pub receipt: Option<SettlementReceipt>,
    pub transaction: bitcoin::Transaction,
}

impl FinalizedSettlement {
    #[inline(always)]
    pub fn new(
        receipt: Option<SettlementReceipt>,
        transaction: bitcoin::Transaction,
    ) -> Self {
        Self {
            receipt,
            transaction,
        }
    }

    #[inline(always)]
    pub fn txid(&self) -> Txid {
        self.transaction.compute_txid()
    }

    #[inline(always)]
    pub fn script_pubkey(&self) -> Option<&ScriptBuf> {
        self.receipt
            .as_ref()
            .map(|receipt| &receipt.script_pubkey)
    }

    /// Constructs a packet for an existing transaction output.
    ///
    /// The output index determines the value and OutPoint. When a settlement
    /// receipt exists, its script is the canonical settlement script.
    #[inline]
    pub fn wire_packet(
        &self,
        vout: u32,
        commitment: [u8; 32],
    ) -> Option<WirePacket> {
        let output = self.transaction.output.get(vout as usize)?;

        let script_pubkey = self
            .script_pubkey()
            .cloned()
            .unwrap_or_else(|| output.script_pubkey.clone());

        Some(WirePacket::new(
            OutPoint {
                txid: self.txid(),
                vout,
            },
            output.value.to_sat(),
            script_pubkey,
            commitment,
        ))
    }
}

// -----------------------------------------------------------------------------
// Broadcast driver
//
// Retains the existing trait signature for source compatibility.
// -----------------------------------------------------------------------------

pub trait BroadcastDriver: Send + Sync + 'static {
    fn publish(&self, packet: &WirePacket) -> WirePacket;
}

// -----------------------------------------------------------------------------
// Wire protocol — QTMGSP02
//
// TCP framing:
//     [4 bytes payload length, big-endian]
//     [WirePacket payload]
//
// Payload:
//     [8 bytes magic]
//     [32 bytes txid]
//     [4 bytes vout, little-endian]
//     [8 bytes value, little-endian]
//     [4 bytes script length, little-endian]
//     [N bytes script_pubkey]
//     [32 bytes commitment]
//
// Do not add fields to this payload without introducing a negotiated version.
// -----------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct WirePacket {
    pub outpoint: OutPoint,
    pub value: u64,
    pub script_pubkey: ScriptBuf,
    pub commitment: [u8; 32],
}

impl WirePacket {
    pub const MAGIC: &'static [u8; 8] = b"QTMGSP02";

    pub const MIN_SIZE: usize = 8 + 32 + 4 + 8 + 4 + 32;

    pub const MAX_SIZE: usize = 1_048_576;

    #[inline(always)]
    pub fn new(
        outpoint: OutPoint,
        value: u64,
        script_pubkey: ScriptBuf,
        commitment: [u8; 32],
    ) -> Self {
        Self {
            outpoint,
            value,
            script_pubkey,
            commitment,
        }
    }

    #[inline(always)]
    pub fn txid(&self) -> Txid {
        self.outpoint.txid
    }

    #[inline(always)]
    pub fn vout(&self) -> u32 {
        self.outpoint.vout
    }

    /// Stable identifier for logs and downstream indexing.
    ///
    /// This identifies the QTM commitment; it does not change the wire format.
    #[inline(always)]
    pub fn commitment_id(&self) -> String {
        hex::encode(self.commitment)
    }

    #[inline(always)]
    pub fn serialize(&self) -> Vec<u8> {
        let script = self.script_pubkey.as_bytes();

        let mut out = Vec::with_capacity(
            Self::MIN_SIZE + script.len(),
        );

        out.extend_from_slice(Self::MAGIC);
        out.extend_from_slice(self.outpoint.txid.as_byte_array());
        out.extend_from_slice(&self.outpoint.vout.to_le_bytes());
        out.extend_from_slice(&self.value.to_le_bytes());
        out.extend_from_slice(&(script.len() as u32).to_le_bytes());
        out.extend_from_slice(script);
        out.extend_from_slice(&self.commitment);

        out
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < Self::MIN_SIZE {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "wire packet too short",
            ));
        }

        if bytes.len() > Self::MAX_SIZE {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "wire packet exceeds maximum size",
            ));
        }

        let mut offset = 0usize;

        let magic = take(bytes, &mut offset, 8)?;

        if magic != Self::MAGIC {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "invalid wire packet magic",
            ));
        }

        let txid = Txid::from_slice(take(bytes, &mut offset, 32)?)
            .map_err(|_| {
                Error::new(ErrorKind::InvalidData, "invalid txid")
            })?;

        let vout = u32::from_le_bytes(take_array::<4>(
            bytes,
            &mut offset,
        )?);

        let value = u64::from_le_bytes(take_array::<8>(
            bytes,
            &mut offset,
        )?);

        let script_len = u32::from_le_bytes(take_array::<4>(
            bytes,
            &mut offset,
        )?) as usize;

        let remaining = bytes.len().saturating_sub(offset);

        // A packet must still contain its 32-byte commitment after the script.
        if script_len > remaining.saturating_sub(32) {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "script length exceeds packet boundary",
            ));
        }

        let script_pubkey = ScriptBuf::from_bytes(
            take(bytes, &mut offset, script_len)?.to_vec(),
        );

        let commitment = take_array::<32>(bytes, &mut offset)?;

        if offset != bytes.len() {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "trailing bytes in wire packet",
            ));
        }

        Ok(Self {
            outpoint: OutPoint { txid, vout },
            value,
            script_pubkey,
            commitment,
        })
    }
}

// -----------------------------------------------------------------------------
// Listening portal — mio TCP ingress
//
// BIT_MINNER_TCP_BIND=0.0.0.0:PORT
//
// The substrate remains the server host. This setting defines the TCP
// listening address; it is not a QTM commitment or network identifier.
// -----------------------------------------------------------------------------

pub struct TcpReceiver;

impl TcpReceiver {
    pub fn from_env() -> Option<SocketAddr> {
        env::var("BIT_MINNER_TCP_BIND")
            .ok()?
            .parse()
            .ok()
    }

    pub fn spawn(bind: SocketAddr) {
        thread::spawn(move || {
            if let Err(error) = Self::run(bind) {
                eprintln!(
                    "tcp_receiver stopped addr={} error={}",
                    bind, error,
                );
            }
        });
    }

    fn run(bind: SocketAddr) -> Result<(), Error> {
        const LISTENER: Token = Token(0);

        let mut poll = Poll::new()?;
        let mut events = Events::with_capacity(128);

        let mut listener = TcpListener::bind(bind)?;

        poll.registry().register(
            &mut listener,
            LISTENER,
            Interest::READABLE,
        )?;

        println!("tcp_portal=online addr={}", bind);

        loop {
            poll.poll(&mut events, None)?;

            for event in events.iter() {
                if event.token() != LISTENER || !event.is_readable() {
                    continue;
                }

                loop {
                    match listener.accept() {
                        Ok((mut stream, peer)) => {
                            println!("tcp_portal=accepted peer={}", peer);

                            if let Err(error) = poll.registry().deregister(
                                &mut stream,
                            ) {
                                eprintln!(
                                    "tcp_portal=deregister_failed peer={} error={}",
                                    peer, error,
                                );
                                continue;
                            }

                            match stream.into_std() {
                                Ok(stream) => {
                                    thread::spawn(move || {
                                        if let Err(error) =
                                            Self::receive(stream)
                                        {
                                            eprintln!(
                                                "tcp_receive peer={} error={}",
                                                peer, error,
                                            );
                                        }
                                    });
                                }
                                Err(error) => {
                                    eprintln!(
                                        "tcp_portal=stream_conversion_failed \
                                         peer={} error={}",
                                        peer, error,
                                    );
                                }
                            }
                        }

                        Err(error)
                            if error.kind() == ErrorKind::WouldBlock =>
                        {
                            break;
                        }

                        Err(error)
                            if error.kind() == ErrorKind::Interrupted =>
                        {
                            continue;
                        }

                        Err(error) => return Err(error),
                    }
                }
            }
        }
    }

    fn receive(mut stream: std::net::TcpStream) -> Result<(), Error> {
        let peer = stream.peer_addr().ok();

        stream.set_nodelay(true)?;

        loop {
            let mut length = [0u8; 4];

            match stream.read_exact(&mut length) {
                Ok(()) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        ErrorKind::UnexpectedEof
                            | ErrorKind::ConnectionReset
                            | ErrorKind::ConnectionAborted
                    ) =>
                {
                    return Ok(());
                }
                Err(error) => return Err(error),
            }

            let frame_len = u32::from_be_bytes(length) as usize;

            if !(WirePacket::MIN_SIZE..=WirePacket::MAX_SIZE)
                .contains(&frame_len)
            {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("invalid frame length={}", frame_len),
                ));
            }

            let mut bytes = vec![0u8; frame_len];
            stream.read_exact(&mut bytes)?;

            let packet = WirePacket::deserialize(&bytes)?;

            println!(
                "qtm=wire_packet_received \
                 peer={} \
                 txid={} \
                 vout={} \
                 value={} \
                 script={} \
                 commitment_id={}",
                peer.map(|addr| addr.to_string())
                    .unwrap_or_else(|| "<unknown>".into()),
                packet.txid(),
                packet.vout(),
                packet.value,
                short_hex(packet.script_pubkey.as_bytes()),
                packet.commitment_id(),
            );
        }
    }
}

// -----------------------------------------------------------------------------
// Console driver
// -----------------------------------------------------------------------------

pub struct ConsoleBroadcastDriver;

impl BroadcastDriver for ConsoleBroadcastDriver {
    #[inline(always)]
    fn publish(&self, packet: &WirePacket) -> WirePacket {
        println!(
            "qtm=broadcast \
             txid={} \
             vout={} \
             value={} \
             script={} \
             commitment_id={}",
            packet.txid(),
            packet.vout(),
            packet.value,
            short_hex(packet.script_pubkey.as_bytes()),
            packet.commitment_id(),
        );

        packet.clone()
    }
}

// -----------------------------------------------------------------------------
// UDP gossip
//
// BIT_MINNER_UDP_PEERS=host:port,host:port
// -----------------------------------------------------------------------------

pub struct UdpGossipDriver {
    peers: Vec<SocketAddr>,
}

impl UdpGossipDriver {
    pub fn from_env() -> Option<Self> {
        parse_peers("BIT_MINNER_UDP_PEERS")
            .map(|peers| Self { peers })
    }
}

impl BroadcastDriver for UdpGossipDriver {
    #[inline(always)]
    fn publish(&self, packet: &WirePacket) -> WirePacket {
        let bytes = packet.serialize();

        // Bind a suitable local ephemeral port. The listener is configured
        // independently by BIT_MINNER_TCP_BIND.
        match UdpSocket::bind("[::]:0")
            .or_else(|_| UdpSocket::bind("0.0.0.0:0"))
        {
            Ok(socket) => {
                for peer in &self.peers {
                    if let Err(error) = socket.send_to(&bytes, peer) {
                        eprintln!(
                            "udp_gossip=failed peer={} commitment_id={} error={}",
                            peer,
                            packet.commitment_id(),
                            error,
                        );
                    }
                }
            }
            Err(error) => {
                eprintln!("udp_gossip=socket_failed error={}", error);
            }
        }

        packet.clone()
    }
}

// -----------------------------------------------------------------------------
// TCP gossip
//
// BIT_MINNER_TCP_PEERS=host:port,host:port
// -----------------------------------------------------------------------------

pub struct TcpGossipDriver {
    peers: Vec<SocketAddr>,
}

impl TcpGossipDriver {
    pub fn from_env() -> Option<Self> {
        parse_peers("BIT_MINNER_TCP_PEERS")
            .map(|peers| Self { peers })
    }
}

impl BroadcastDriver for TcpGossipDriver {
    #[inline(always)]
    fn publish(&self, packet: &WirePacket) -> WirePacket {
        let bytes = packet.serialize();
        let length = (bytes.len() as u32).to_be_bytes();

        for peer in &self.peers {
            match TcpStream::connect_timeout(
                peer,
                Duration::from_millis(250),
            ) {
                Ok(mut stream) => {
                    if let Err(error) = stream.set_nodelay(true) {
                        eprintln!(
                            "tcp_gossip=failed peer={} error={}",
                            peer, error,
                        );
                        continue;
                    }

                    if let Err(error) = stream
                        .write_all(&length)
                        .and_then(|()| stream.write_all(&bytes))
                    {
                        eprintln!(
                            "tcp_gossip=failed \
                             peer={} \
                             commitment_id={} \
                             error={}",
                            peer,
                            packet.commitment_id(),
                            error,
                        );
                    } else {
                        println!(
                            "tcp_gossip=sent peer={} commitment_id={}",
                            peer,
                            packet.commitment_id(),
                        );
                    }
                }
                Err(error) => {
                    eprintln!(
                        "tcp_gossip=unreachable peer={} error={}",
                        peer, error,
                    );
                }
            }
        }

        packet.clone()
    }
}

// -----------------------------------------------------------------------------
// Broadcaster
// -----------------------------------------------------------------------------

pub struct Broadcaster {
    queue: SyncSender<WirePacket>,
}

impl Broadcaster {
    pub fn init() -> Self {
        if let Some(bind) = TcpReceiver::from_env() {
            TcpReceiver::spawn(bind);
        }

        let mut drivers: Vec<Arc<dyn BroadcastDriver>> = vec![
            Arc::new(ConsoleBroadcastDriver),
        ];

        if let Some(driver) = UdpGossipDriver::from_env() {
            drivers.push(Arc::new(driver));
        }

        if let Some(driver) = TcpGossipDriver::from_env() {
            drivers.push(Arc::new(driver));
        }

        Self::new(drivers, 4096)
    }

    pub fn new(
        drivers: Vec<Arc<dyn BroadcastDriver>>,
        queue_depth: usize,
    ) -> Self {
        let (tx, rx): (SyncSender<WirePacket>, Receiver<WirePacket>) =
            sync_channel(queue_depth.max(1));

        thread::spawn(move || {
            println!(
                "qtm_broadcaster=online drivers={}",
                drivers.len(),
            );

            while let Ok(packet) = rx.recv() {
                for driver in &drivers {
                    // The legacy trait returns WirePacket rather than Result.
                    // Keep the return value for compatibility; the packet is
                    // the same canonical object for every driver.
                    let _ = driver.publish(&packet);
                }
            }

            println!("qtm_broadcaster=stopped");
        });

        Self { queue: tx }
    }

    /// Preserves the existing signature and return value.
    ///
    /// Uses blocking send rather than try_send so a temporarily full queue
    /// applies backpressure instead of dropping or immediately panicking.
    #[inline(always)]
    pub fn publish(&self, packet: WirePacket) -> WirePacket {
        self.queue
            .send(packet.clone())
            .expect("QTM broadcaster worker disconnected");

        packet
    }
}

// -----------------------------------------------------------------------------
// Utilities
// -----------------------------------------------------------------------------

#[inline(always)]
fn short_hex(bytes: &[u8]) -> String {
    let full = hex::encode(bytes);

    if full.len() <= 16 {
        full
    } else {
        format!("{}...{}", &full[..8], &full[full.len() - 8..])
    }
}

#[inline(always)]
fn take<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    len: usize,
) -> Result<&'a [u8], Error> {
    let end = offset.checked_add(len).ok_or_else(|| {
        Error::new(ErrorKind::InvalidData, "packet offset overflow")
    })?;

    if end > bytes.len() {
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            "truncated wire packet",
        ));
    }

    let slice = &bytes[*offset..end];
    *offset = end;

    Ok(slice)
}

#[inline(always)]
fn take_array<const N: usize>(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<[u8; N], Error> {
    take(bytes, offset, N)?
        .try_into()
        .map_err(|_| {
            Error::new(
                ErrorKind::InvalidData,
                "invalid fixed-size field",
            )
        })
}

fn parse_peers(key: &str) -> Option<Vec<SocketAddr>> {
    let raw = env::var(key).ok()?;

    let peers: Vec<SocketAddr> = raw
        .split(',')
        .filter_map(|peer| peer.trim().parse::<SocketAddr>().ok())
        .collect();

    (!peers.is_empty()).then_some(peers)
}