use bit_miner::bitcoinpayload::BitcoinPayload,
sharedtypes::{
    Authorization,
    AuthorizationState,
    SettlementRequest,
    SettlementState,
};

use qtm_graph::graph::{
    frame::{Depth, Direction},
    Scale,
};

use quantom_value::Heritage;

pub enum PayloadKind {
    Bitcoin(BitcoinPayload),
}

pub struct AuthorizedPayload {
    pub request: SettlementRequest,
    pub authorization_state: AuthorizationState,
    pub settlement_state: SettlementState,
}

/// Owns the authentic event direction and its associated depth.
pub struct EventPayload {
    pub direction: Direction,
    pub depth: Depth,
}

impl EventPayload {
    #[inline(always)]
    pub fn from_heritage(
        heritage: Heritage,
        depth: Depth,
        scale: Scale,
    ) -> Self {
        Self {
            direction: Direction::from_heritage(heritage, scale),
            depth,
        }
    }

    #[inline(always)]
    pub fn event(&self) -> &qtm_graph::graph::frame::EventMap {
        self.direction.event()
    }

    #[inline(always)]
    pub fn path(&self) -> &str {
        self.depth.path()
    }

    #[inline(always)]
    pub fn depth(&self) -> &Depth {
        &self.depth
    }
}