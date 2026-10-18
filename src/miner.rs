use qp_hd{coin};








#[repr(C)]
pub struct QPBlock {
    pub substrate:String,
    pub height:u64,
    pub previous_block_hash:Option<BlockHash>,
    pub finalized_transactions:Vec<FinalizedSettlement>,
    pub coinbase:QPCoinbase,
    pub block_commitment:[u8;32],
    pub block_hash:BlockHash,
    pub bitcoin_settlement_root:[u8;32],
    pub economic:QuantumTransaction,
}

impl QPBlock {

#[inline(always)]
pub fn mine(
    substrate:&str,
    previous_activation:u64,
    height:u64,
    previous_block_hash:Option<BlockHash>,
    finalized:Vec<FinalizedSettlement>,
    coinbase_amount:u64,
    pipeline:&mut BitcoinObservationPipeline,
)->Result<Option<Self>,SettlementError>{
    assert!(
        !finalized.is_empty(),
        "QP block requires at least one settlement",
    );

    let economic=QuantumTransaction::send(
        substrate,
        previous_activation,
    );

    let coinbase_transaction=Transaction{
        version:Version(1),
        lock_time:LockTime::ZERO, oh
        input:vec![
            TxIn{
                previous_output:OutPoint::null(),
                script_sig:build_coinbase_script(
                    previous_activation,
                    substrate,
                ),
                sequence:Sequence::MAX,
                witness:Witness::default(),
            },
        ],
        output:vec![
            TxOut{
                value:bitcoin::Amount::from_sat(
                    coinbase_amount,
                ),
                script_pubkey:miner_script_pubkey(),
            },
        ],
    };

    let coinbase=QPCoinbase::new(
        miner_address(),
        previous_activation,
        coinbase_amount,
    );

    let source=BitcoinObservationSource::new(
        BitcoinObservationSourceKind::Substrate,
        previous_activation,
    );

    let observed=ObservedBitcoinTransaction::from_transaction(
        coinbase_transaction,
        source,
    );

    let Some(coinbase_finalized)=pipeline.process(
        substrate,
        observed,
    )?
    else{
        return Ok(None);
    };

    let mut finalized_transactions=
        Vec::with_capacity(
            finalized.len()+1,
        );

    finalized_transactions.push(
        coinbase_finalized,
    );

    finalized_transactions.extend(
        finalized,
    );

    let bitcoin_settlement_root=merkle_root(
        finalized_transactions
            .iter()
            .map(|finalized|
                finalized
                    .transaction
                    .compute_txid()
                    .to_byte_array()
            )
            .collect(),
    );

    let delta=
        match &economic.constant{
            PersistedEvent::Transit(event)=>
                event
                    .heritage
                    .transition
                    .delta,

            PersistedEvent::Exile(event)=>
                event
                    .heritage
                    .transition
                    .delta,
        };

    let block_commitment=block_commitment(
        substrate,
        height,
        previous_block_hash,
        bitcoin_settlement_root,
        &coinbase,
    );

    let block_hash=block_hash(
        substrate,
        height,
        previous_block_hash,
        block_commitment,
    );

    println!(
        "QP BLOCK BUILD proposal_height={} activation={} delta={} settlements={} coinbase={} sats",
        height,
        economic.amount,
        delta,
        finalized_transactions.len().saturating_sub(1),
        coinbase.amount,
    );

    Ok(Some(Self{
        substrate:substrate.to_string(),
        height,
        economic,
        previous_block_hash,
        finalized_transactions,
        bitcoin_settlement_root,
        coinbase,
        block_commitment,
        block_hash,
    }))
    }

    #[inline(always)]
    pub fn coinbase_amount(
        height: u64,
    ) -> u128 {
        const INITIAL_SUBSIDY: u128 =
            5_000_000_000;

        const HALVING_INTERVAL: u64 =
            210_000;

        let halvings =
            (height / HALVING_INTERVAL)
                .min(63);

        INITIAL_SUBSIDY >> halvings
    }

    #[inline(always)]
    pub fn block_confirmation(
        &self,
        block: &bitcoin::Block,
    ) -> BlockConfirmation {
        BlockConfirmation::new(
            block,
            self.height,
        )
    }
}



let (mut qp_block, schedule, receive) = match builder.build(
        proposal.proposed_height,
        proposal.activation_count,
        Some(proposal.previous_block),
    ) {
        Ok(Some(result)) => result,
        Ok(None) => {
            thread::yield_now();
            continue;
        }
        Err(error) => {
            eprintln!(
                "canonical block construction failed: {:?}",
                error,
            );
            thread::yield_now();
            continue;
        }
    };

    qp_block.receive =
        receive;

    let proof =
        QPProof::from_block(
            &qp_block,
        );

    assert!(
        proof.accepts(),
        "QP deterministic topology rejected",
    );

    let target =
        QPProof::target(
            &qp_block,
        );

    println!(
        "proof=qp_target \
         coordinate={:02x?} \
         mirror={} \
         dimension={} \
         gravity={} \
         delta={} \
         structural_value={} \
         target={}",
        &proof.coordinate[..4],
        proof.mirror,
        proof.dimension,
        proof.gravity,
        proof.delta,
        proof.structural_value,
        target,
    );

    // ---------------------------------------------------------------------
    // Canonical Bitcoin finalized-settlement handoff.
    //
    // The block is the primary archive.
    //
    // Its receive event is the economic post-state boundary and remains
    // attached to the block for downstream consumers.
    // ---------------------------------------------------------------------

    let transactions =
        &qp_block
            .finalized_transactions;

    let mut submitted =
        HashSet::<Txid>::with_capacity(
            transactions.len(),
        );

    for finalized in transactions {

        let parent_txid =
            finalized.parent.txid;

        let is_block_coinbase_child =
            finalized
                .transaction
                .input
                .iter()
                .any(|input| {
                    input
                        .previous_output
                        .is_null()
                });

        if is_block_coinbase_child {
            println!(
                "bitcoin=skip_coinbase \
                 child={} parent={}",
                finalized.txid(),
                parent_txid,
            );

            continue;
        }

        let notification = {
            let mut pipeline =
                builder
                    .pipeline()
                    .lock()
                    .expect("pipeline poisoned");

            pipeline.advance_finalized(
                &substrate,
                finalized,
            )?
        };

        if let Some((
            transaction,
            payload_event,
        )) = notification {

            let computed_txid =
                transaction
                    .compute_txid();

            println!(
                "bitcoin=finalized \
                 txid={} \
                 activation={} \
                 fd={} \
                 payload={}",
                computed_txid,
                payload_event
                    .environment
                    .activation,
                payload_event.fd,
                payload_event
                    .payload
                    .len(),
            );

            submitted.insert(
                computed_txid,
            );
        }
    }

    // ---------------------------------------------------------------------
    // Assemble canonical Bitcoin block.
    //
    // `qp_block` remains the canonical archive. The receive event is part
    // of that archive and is therefore available downstream with the block.
    // ---------------------------------------------------------------------

    println!(
        "miner=start",
    );

    let finalized =
        assembler
            .assemble(
                qp_block,
            );

    println!(
        "miner=assembled \
         height={} \
         txs={}",
        proposal.proposed_height,
        finalized.len(),
    );

    println!(
        "proposal=submitted \
         height={} \
         activation={} \
         events={}",
        proposal.proposed_height,
        proposal.activation_count,
        finalized.len().saturating_sub(1),
    );

    {
        let mut tracker =
            proposal_tracker
                .lock()
                .unwrap();

        tracker.track(
            schedule.clone(),
            &proposal,
        );
    }

    downstream
        .exile(
            finalized,
        )?;
        
        
        // -------------------------------------------------------------------------
// Canonical Bitcoin Core admission.
//
// The finalized settlement is the canonical economic boundary.
//
// For every finalized settlement:
//
//     finalized.parent
//             +
//     finalized settlement
//             ↓
//     BitcoinPayload
//             ↓
//     authoritative Core UTXO check
//             ↓
//     Bitcoin Core
//             ↓
//     advance_finalized()
//             ↓
//     Option<(Transaction, PayloadEvent)>
//
// `advance_finalized()` is the wallet-side earning / withdrawal boundary.
// Its returned Option is a notification primitive, not proposal state.
//
// The observed parent is never re-submitted.
// The block-level QP44 receive event is not reused here.
// -------------------------------------------------------------------------
let transactions =
    &qp_block
        .finalized_transactions;
let mut submitted =
    HashSet::<Txid>::with_capacity(
        transactions.len(),
    );
for finalized in transactions {
    let parent_txid =
        finalized.parent.txid;
    let is_block_coinbase_child =
        finalized
            .transaction
            .input
            .iter()
            .any(|input| {
                input
                    .previous_output
                    .is_null()
            });
    if is_block_coinbase_child {
        println!(
            "bitcoin=skip_coinbase \
             child={} parent={}",
            finalized.txid(),
            parent_txid,
        );
        continue;
    }
    // -----------------------------------------------------------------
    // Construct the Bitcoin payload from the finalized parent and the
    // settlement's canonical QP state.
    //
    // `bitcoin_payload()` is construction only.
    // `advance_materialized()` consumes the QP44 event required to
    // produce the finalized executable settlement.
    // -----------------------------------------------------------------
    let Some(advanced) = ({
        let mut pipeline =
            builder
                .pipeline()
                .lock()
                .expect("pipeline poisoned");
        let bitcoin =
            pipeline.bitcoin_payload(
                &finalized.parent,
                &finalized.event,
            );
        println!(
            "bitcoin=payload \
             parent={} \
             child={} \
             inputs={} \
             outputs={}",
            parent_txid,
            finalized.txid(),
            bitcoin.inputs.len(),
            bitcoin.outputs.len(),
        );
        for input in &bitcoin.inputs {
            println!(
                "bitcoin=payload_input \
                 {}:{} \
                 script_sig={} \
                 witness={}",
                input.previous_output.txid,
                input.previous_output.vout,
                input.script_sig.len(),
                input.witness.len(),
            );
        }
        for output in &bitcoin.outputs {
            println!(
                "bitcoin=payload_output \
                 vout={} \
                 value={} \
                 script={}",
                output.vout,
                output.value,
                hex::encode(
                    output
                        .script_pubkey
                        .as_bytes(),
                ),
            );
        }
        pipeline.advance_materialized(
            &substrate,
            bitcoin,
            finalized.event,
            &finalized.parent,
        )?
    }) else {
        continue;
    };
    // -----------------------------------------------------------------
    // `advanced` is now the canonical finalized settlement whose
    // Bitcoin transaction is eligible for Core admission.
    // -----------------------------------------------------------------
    let transaction =
        &advanced.transaction;
    let computed_txid =
        advanced.txid();
    let parent_txid =
        advanced.parent.txid;
    // -----------------------------------------------------------------
    // Core admission is authoritative.
    // -----------------------------------------------------------------
    let chain =
        confirmation_manager
            .lock()
            .unwrap();
    let spendable =
        transaction
            .input
            .iter()
            .all(|input| {
                chain
                    .core_utxo_spendable(
                        input.previous_output,
                    )
            });
    drop(chain);
    if !spendable {
        println!(
            "bitcoin=skip_unspendable \
             parent={} child={} height={}",
            parent_txid,
            computed_txid,
            proposal.proposed_height,
        );
        continue;
    }
    println!(
        "bitcoin=submit \
         height={} parent={} child={} inputs={} outputs={}",
        proposal.proposed_height,
        parent_txid,
        computed_txid,
        transaction.input.len(),
        transaction.output.len(),
    );
    for (index,input)
        in transaction
            .input
            .iter()
            .enumerate()
    {
        println!(
            "bitcoin=submit_input \
             txid={} index={} previous={}:{}",
            computed_txid,
            index,
            input.previous_output.txid,
            input.previous_output.vout,
        );
    }
    let core_result = {
        let broadcaster =
            broadcaster
                .lock()
                .unwrap();
        broadcaster
            .send_raw_transaction(
                transaction,
            )
    };
    match core_result {
        Ok(txid) => {
            println!(
                "bitcoin=accepted \
                 txid={} parent={}",
                txid,
                parent_txid,
            );
            assert_eq!(
                txid,
                computed_txid,
                "Bitcoin Core returned unexpected transaction id",
            );
            submitted.insert(
                txid,
            );
        }
        Err(err) => {
            panic!(
                "FATAL: Bitcoin Core rejected canonical finalized \
                 settlement transaction: txid={} height={} activation={} error={:?}",
                computed_txid,
                proposal.proposed_height,
                proposal.activation_count,
                err,
            );
        }
    }
    // -----------------------------------------------------------------
    // Finalized settlement advancement.
    //
    // This is the wallet-side earning / withdrawal boundary.
    //
    // `advance_finalized()` consumes the finalized settlement and
    // performs the substrate-side advancement. The returned
    // `(Transaction, PayloadEvent)` is only a notification primitive.
    //
    // It does NOT replace the finalized settlement, mutate proposal
    // activation, or become the next block-level receive event.
    // -----------------------------------------------------------------
    let notification = {
        let mut pipeline =
            builder
                .pipeline()
                .lock()
                .expect("pipeline poisoned");
        pipeline
            .advance_finalized(
                &substrate,
                advanced,
            )?
    };
    if let Some((
        child,
        payload_event,
    )) = notification
    {
        println!(
            "bitcoin=advanced \
             txid={} \
             activation={} \
             fd={} \
             payload={}",
            child.compute_txid(),
            payload_event.environment.activation,
            payload_event.fd,
            payload_event.payload.len(),
        );
        // `payload_event` is a notification primitive.
        //
        // It may be dispatched to the appropriate wallet / earner /
        // withdrawal consumer here. It is not fed back into the
        // proposal loop as block receive state.
    }
}
        