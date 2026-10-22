use bit_miner::sharedtypes::{
    Authorization,
    AuthorizationState,
    SettlementRequest,
    SettlementState,
};

pub enum PayloadKind {
    Bitcoin(BitcoinPayload),
    Ethereum(EthereumPayload),
    Solana(SolanaPayload),
    Tron(TronPayload),
}

pub struct AuthorizedPayload {
    pub request: SettlementRequest,
    pub authorization_state: AuthorizationState,
    pub settlement_state: SettlementState,
}