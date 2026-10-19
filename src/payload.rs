use bitcoin::Transaction;
use qp_hd::PQ44wallet;

use crate::{
    BitcoinObservationSource,
    BitcoinPayload,
};
use qp_hd::qp44::QuantPerm;

pub struct ObservedBitcoinTransaction {
    pub observation: Option<usize>,
    pub source: BitcoinObservationSource,
    pub txid: bitcoin::Txid,
    pub transaction: Transaction,
    pub coinbase: bool,
    pub fee: Option<u64>,
    pub weight: Option<u64>,
    pub depends: Option<Vec<bitcoin::Txid>>,
    pub template_index: Option<usize>,
}

impl ObservedBitcoinTransaction {
    #[inline(always)]
    pub fn is_coinbase(
        transaction: &Transaction,
    ) -> bool {
        transaction.input.len() == 1
            && transaction.input[0].previous_output
                == bitcoin::OutPoint::null()
    }

    #[inline(always)]
    pub fn from_transaction(
        transaction: Transaction,
        source: BitcoinObservationSource,
    ) -> Self {
        let txid = transaction.compute_txid();
        let coinbase = Self::is_coinbase(&transaction);

        Self {
            observation: None,
            source,
            txid,
            transaction,
            coinbase,
            fee: None,
            weight: None,
            depends: None,
            template_index: None,
        }
    }

    #[inline(always)]
    pub fn new_transact(
        manifold: &QuantPerm,
        bitcoin: &BitcoinPayload,
        delta: u128,
        source: BitcoinObservationSource,
    ) -> Option<Self> {
        let fee = u64::try_from(
            bitcoin.fee_mass(manifold, delta),
        )
        .ok()?;

        let last = bitcoin.outputs.len().checked_sub(1)?;

        let inputs = bitcoin
            .inputs
            .iter()
            .map(|input| bitcoin::TxIn {
                previous_output: input.previous_output,
                script_sig: input.script_sig.clone(),
                sequence: input.sequence,
                witness: input.witness.clone(),
            })
            .collect::<Vec<_>>();

        let outputs = bitcoin
            .outputs
            .iter()
            .enumerate()
            .map(|(index, output)| bitcoin::TxOut {
                value: bitcoin::Amount::from_sat(
                    if index == last {
                        output.value.checked_sub(fee)?
                    } else {
                        output.value
                    },
                ),
                script_pubkey: output.script_pubkey.clone(),
            })
            .collect::<Vec<_>>();

        let transaction = Transaction {
            version: bitcoin::transaction::Version(1),
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: inputs,
            output: outputs,
        };

        for (index, output) in transaction.output.iter().enumerate() {
            assert_eq!(
                output.script_pubkey,
                bitcoin.outputs[index].script_pubkey,
                "new_transact changed canonical script_pubkey at vout={index}",
            );

            let script = output.script_pubkey.as_bytes();

            if script.len() == 22
                && script[0] == 0x00
                && script[1] == 0x14
            {
                let witness_hash160 = &script[2..22];

                let _address =
                    crate::bit_block::p2wpkh_address(witness_hash160);
            }
        }

        Some(Self::from_transaction(transaction, source))
    }

    #[inline(always)]
    pub fn new_transaction(
        manifold: &QuantPerm,
        bitcoin: &BitcoinPayload,
        delta: u128,
        source: BitcoinObservationSource,
    ) -> Option<Self> {
        Self::new_transact(
            manifold,
            bitcoin,
            delta,
            source,
        )
    }

    #[inline(always)]
    pub fn sign(
        &mut self,
        manifold: &QuantPerm,
        _transaction: &bitcoin::Transaction,
        _input_index: usize,
        _value: u64,
    ) {
        let coordinate = qp_hd::Qtm::derive_coordinate(manifold);

        let (_, hash160) =
            crate::bit_block::derive_public_key(&coordinate);

        let _address =
            crate::bit_block::p2wpkh_address(&hash160);
    }
}