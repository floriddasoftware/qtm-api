use mio; 
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
    pub fn script_pubkey(
        &self,
    ) -> Option<&bitcoin::ScriptBuf> {
        self.receipt
            .as_ref()
            .map(|receipt| &receipt.script_pubkey)
    }
}