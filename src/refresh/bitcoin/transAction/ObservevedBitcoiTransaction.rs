// -----------------------------------------------------------------------------
// Canonical observed Bitcoin transaction.
// Can originate from:
//   - network observation
//   - Bitcoin Core getblocktemplate()
// Preserves the decoded transaction together with all template metadata.
// NOTE: getblocktemplate() does NOT provide referenced UTXOs.
// -----------------------------------------------------------------------------
#[derive(Clone,Debug)]
pub struct ObservedBitcoinTransaction{
    pub observation:Option<BitcoinObservation>,
    pub source:BitcoinObservationSource,
    pub txid:Txid,
    pub transaction:bitcoin::Transaction,
    pub coinbase:bool,
    /// Fee from block template; None if only network observed.
    pub fee:Option<u64>,
    /// Weight from block template; None if only network observed.
    pub weight:Option<usize>,
    /// Zero-based indexes into template tx list (Bitcoin Core supplies 1-based).
    pub depends:Option<Vec<usize>>,
    /// Original zero-based position in template; None if only network observed.
    pub template_index:Option<usize>,
}

impl ObservedBitcoinTransaction {
    #[inline(always)]
    pub fn is_coinbase(
        transaction:&bitcoin::Transaction,
    )->bool {
        transaction.input.len() == 1
            && transaction.input[0]
                .previous_output
                .is_null()
    }

    #[inline(always)]
    pub fn coinbase(
        &self,
    )->bool {
        self.coinbase
    }

    #[inline(always)]
    pub fn from_transaction(
        transaction:bitcoin::Transaction,
        source:BitcoinObservationSource,
    )->Self {
        let txid =
            transaction.compute_txid();
    
        let coinbase =
            Self::is_coinbase(
                &transaction,
            );
    
        Self {
            observation:None,
            source,
            txid,
            transaction,
            coinbase,
            fee:None,
            weight:None,
            depends:None,
            template_index:None,
        }
    }

    #[inline(always)]
    pub fn new_transact(
        manifold:&QuantPerm,
        mut bitcoin:BitcoinPayload,
        source:BitcoinObservationSource,
    )->Self {
        let coordinate =
            crate::protocolvalue::Qtm::derive_coordinate(
                manifold,
            );

        let script =
            crate::bit_block::derive_script_pubkey(
                &coordinate,
            );

        let fee =
            bitcoin.fee_mass(manifold) as u64;

        if let Some(output) =
            bitcoin.outputs.last_mut()
        {
            output.value =
                output.value.saturating_sub(fee);
        }

        for input in &mut bitcoin.inputs {
            input.script_sig =
                script.clone();

            input.witness =
                bitcoin::Witness::new();
        }

        for output in &mut bitcoin.outputs {
            output.script_pubkey =
                script.clone();
        }

        let transaction =
            bitcoin::Transaction {
                version:
                    bitcoin::transaction::Version(1),

                lock_time:
                    bitcoin::absolute::LockTime::ZERO,

                input:bitcoin.inputs
                    .into_iter()
                    .map(|input| bitcoin::TxIn {
                        previous_output:
                            input.previous_output,

                        script_sig:
                            input.script_sig,

                        sequence:
                            bitcoin::Sequence::MAX,

                        witness:
                            input.witness,
                    })
                    .collect(),

                output:bitcoin.outputs
                    .into_iter()
                    .map(|output| bitcoin::TxOut {
                        value:
                            bitcoin::Amount::from_sat(
                                output.value,
                            ),

                        script_pubkey:
                            output.script_pubkey,
                    })
                    .collect(),
            };

        Self::from_transaction(
            transaction,
            source,
        )
    }

    #[inline(always)]
    pub fn new_transaction(
        manifold:&QuantPerm,
        bitcoin:BitcoinPayload,
        source:BitcoinObservationSource,
    )->Self {
        Self::new_transact(
            manifold,
            bitcoin,
            source,
        )
    }
}


