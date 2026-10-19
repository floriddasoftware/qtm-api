use qp_hd{coin};




let budget = BlockBudget {
        min_events,
        max_events,
        max_retained_mass: u128::MAX,
    };
    
pub struct BitcoinUTXOCollector {
broadcaster: Broadcaster,
bitcoin: BitcoinBroadcaster,
}

impl BitcoinUTXOCollector {
#[inline(always)]
pub fn new(
_substrate: &str,
_activation: u64,
) -> Result<Self, SettlementError> {
let config =
BitcoinObservationConfig::defaults(
BitcoinNetwork::Mainnet,
)
.with_substrate_observations(true);

    let mut frontend =
        PeerFrontend::new(config)
            .map_err(|error|
                SettlementError::Backend(
                    error.to_string(),
                )
            )?;
    frontend
        .start_p2p()
        .map_err(|error|
            SettlementError::Backend(
                error.to_string(),
            )
        )?;
    println!(
        "trace=p2p_initialized \
         peers={} \
         sessions={}",
        frontend.peers().len(),
        frontend.sessions.len(),
    );
    let rpc =
        BitcoinBroadcaster::from_env()
            .map_err(|error|
                SettlementError::Backend(
                    error.to_string(),
                )
            )?;
    Ok(Self {
        broadcaster:
            Broadcaster::init(),
        bitcoin:
            BitcoinBroadcaster::new(
                rpc.rpc,
                frontend,
            ),
    })
}
#[inline(always)]
pub fn collect(
    &mut self,
    observed: &ObservedBitcoinTransaction,
    event: QP44Event,
) -> BitcoinPayload {
    let tx = &observed.transaction;
    let manifold = &event.heritage.state;
    let coordinate = event.qtm.coordinate;
    println!(
        "trace=utxo_transaction \
         txid={} \
         inputs={} \
         outputs={}",
        observed.txid,
        tx.input.len(),
        tx.output.len(),
    );
    /*
     * The observed transaction's historical inputs describe how the
     * transaction entered observation.
     *
     * They remain authoritative on the observation boundary, but are
     * deliberately not propagated into the QP44 execution payload.
     */
    for (index, input) in tx.input.iter().enumerate() {
        println!(
            "trace=topology_observed \
             index={} \
             previous_txid={} \
             previous_vout={}",
            index,
            input.previous_output.txid,
            input.previous_output.vout,
        );
    }
    /*
     * Materialize the observed Bitcoin outputs into QP44 outputs.
     *
     * The observed output value is authoritative. The QP44 script
     * representation is derived from the event coordinate.
     */
    let outputs = tx
        .output
        .iter()
        .enumerate()
        .map(|(vout, output)| {
            let mut bitcoin = BitcoinOutput::new(
                manifold,
                vout as u32,
                output.value.to_sat(),
            );
            bitcoin.script_pubkey =
                crate::bit_block::derive_script_pubkey(
                    &coordinate,
                );
            bitcoin
        })
        .collect::<Vec<_>>();
    /*
     * Every output produced by the observed transaction becomes one
     * QP44 execution input.
     *
     * Canonical topology:
     *
     *     observed.txid:0
     *     observed.txid:1
     *     ...
     *
     * The historical transaction inputs above are deliberately not
     * propagated into this execution representation.
     */
    let inputs = tx
        .output
        .iter()
        .enumerate()
        .map(|(vout, _output)| BitcoinInput {
            previous_output: bitcoin::OutPoint {
                txid: observed.txid,
                vout: vout as u32,
            },
            script_sig: bitcoin::ScriptBuf::new(),
            sequence: bitcoin::Sequence::MAX,
            witness: bitcoin::Witness::new(),
        })
        .collect::<Vec<_>>();
    /*
     * Validate the canonical QP44 execution topology before moving
     * `inputs` into BitcoinPayload.
     */
    assert_eq!(
        inputs.len(),
        tx.output.len(),
        "QP44 execution input count mismatch: \
         observed_outputs={} payload_inputs={}",
        tx.output.len(),
        inputs.len(),
    );
    for (index, input) in inputs.iter().enumerate() {
        let expected = bitcoin::OutPoint {
            txid: observed.txid,
            vout: index as u32,
        };
        assert_eq!(
            input.previous_output,
            expected,
            "QP44 execution topology changed at vin={}: \
             expected={}:{} payload={}:{}",
            index,
            expected.txid,
            expected.vout,
            input.previous_output.txid,
            input.previous_output.vout,
        );
        println!(
            "trace=topology_qp44_execution \
             index={} \
             previous_txid={} \
             previous_vout={}",
            index,
            input.previous_output.txid,
            input.previous_output.vout,
        );
    }
    /*
     * BitcoinPayload owns the canonical QP44 execution
     * representation of this observation.
     *
     * Collection ends at this boundary.
     *
     * The caller still owns `observed` and therefore retains access
     * to the complete Bitcoin observation for all downstream
     * settlement semantics.
     */
    BitcoinPayload::new(
        observed.txid,
        inputs,
        outputs,
    )
}

}
    
// -----------------------------------------------------------------------------
// Bitcoin Observation Pipeline
// -----------------------------------------------------------------------------

pub struct BitcoinObservationPipeline {
    utxo: BitcoinUTXOCollector,
    pub miner: Miner,
    chain: ChainStateManager,
}

impl BitcoinObservationPipeline {
    #[inline(always)]
    pub fn new(
        activation: u64,
        substrate: &str,
    ) -> Result<Self, SettlementError> {
        Ok(Self {
            utxo: BitcoinUTXOCollector::new(substrate, activation)?,
            miner: Miner::new(activation.saturating_sub(1)),
            chain: ChainStateManager::new(),
        })
    }

    #[inline(always)]
    pub fn miner_mut(&mut self) -> &mut Miner {
        &mut self.miner
    }

    #[inline(always)]
    pub fn miner(&self) -> &Miner {
        &self.miner
    }

    #[inline(always)]
    pub fn best_tip(
        &self,
    ) -> Option<(BlockHash, u64)> {
        self.chain.best_tip()
    }

    #[inline(always)]
    pub fn process(
        &mut self,
        substrate: &str,
        observed: ObservedBitcoinTransaction,
    ) -> Result<Option<(FinalizedSettlement, Vec<Retain>)>, SettlementError> {
        let (event, _cascade) =
            self.miner
                .submit(substrate);

        let event_receipt =
            EventReceipt::new(
                event,
                SettlementState::Pending,
            );

        let (bitcoin, retained) =
            self.bitcoin_payload(
                &observed,
                &event_receipt,
            )?;

        self.advance_materialized(
            substrate,
            bitcoin,
            retained,
            event_receipt,
            &observed,
        )
    }

    #[inline(always)]
    pub fn bitcoin_payload(
        &mut self,
        observed: &ObservedBitcoinTransaction,
        event: &EventReceipt,
    ) -> Result<(BitcoinPayload, Vec<Retain>), SettlementError> {
        self.utxo
            .collect(
                observed,
                event,
            )
    }

    #[inline(always)]
    pub fn advance_materialized(
        &mut self,
        substrate: &str,
        bitcoin: BitcoinPayload,
        mut retained: Vec<Retain>,
        event: EventReceipt,
        observed: &ObservedBitcoinTransaction,
    ) -> Result<Option<(FinalizedSettlement, Vec<Retain>)>, SettlementError> {
        let Some(tip_height) =
            self.chain
                .tip_height()
        else {
            return Ok(None);
        };

        let manifold =
            &event
                .event
                .heritage
                .state;

        let payload_activation =
            manifold
                .activations();

        if tip_height <= payload_activation {
            return Ok(None);
        }

        let Some(clip) =
            self.miner
                .clips()
                .iter()
                .find(|clip| clip.substrate == substrate)
        else {
            return Ok(None);
        };

        if clip.activation == payload_activation {
            return Ok(None);
        }

        assert_eq!(
            bitcoin.inputs.len(),
            observed.transaction.output.len(),
            "materialized input cardinality changed: inputs={} outputs={}",
            bitcoin.inputs.len(),
            observed.transaction.output.len(),
        );

        let (finalized, settlement_retained) =
            SettlementEngine::new(substrate)
                .process(
                    event,
                    bitcoin,
                    observed.clone(),
                )?;

        retained.extend(settlement_retained);

        Ok(Some((
            finalized,
            retained,
        )))
    }

    #[inline(always)]
    pub fn advance_finalized(
        &mut self,
        substrate: &str,
        finalized: FinalizedSettlement,
    ) -> Result<
        Option<(Transaction, PayloadEvent)>,
        SettlementError,
    > {
        self.utxo
            .advance_finalized(
                substrate,
                finalized,
            )
    }
}

#[inline(always)]
pub fn process(
    &self,
    receipt: EventReceipt,
    mut bitcoin: BitcoinPayload,
    parent: ObservedBitcoinTransaction,
) -> Result<
    (FinalizedSettlement, Retain),
    SettlementError,
> {
    println!(
        "trace=settlement_process_begin \
         parent_txid={} \
         inputs={} \
         outputs={}",
        parent.txid,
        bitcoin.inputs.len(),
        bitcoin.outputs.len(),
    );

    let settled_receipt =
        Self::settle(receipt);

    println!(
        "trace=settlement_receipt_complete \
         parent_txid={}",
        parent.txid,
    );

    println!(
        "trace=settlement_state_begin \
         parent_txid={}",
        parent.txid,
    );

    println!(
        "trace=settlement_state \
         parent_txid={} \
         state={:?}",
        parent.txid,
        settled_receipt.state,
    );

    let manifold =
        &settled_receipt
            .event
            .heritage
            .state;

    let delta =
        settled_receipt
            .event
            .heritage
            .transition
            .delta;

    println!(
        "trace=settlement_state_complete \
         parent_txid={}",
        parent.txid,
    );

    println!(
        "trace=settlement_parameters \
         parent_txid={} \
         activation={} \
         delta={}",
        parent.txid,
        manifold.activations(),
        delta,
    );

    println!(
        "trace=settlement_retained_begin \
         parent_txid={}",
        parent.txid,
    );

    let input_mass =
        bitcoin
            .inputs
            .iter()
            .enumerate()
            .map(|(position, input)| {
                input
                    .deep(
                        manifold,
                        "input",
                        position,
                    )
                    .mass
            })
            .sum::<u128>();

    let output_mass =
        bitcoin
            .outputs
            .iter()
            .enumerate()
            .map(|(position, output)| {
                output
                    .deep(
                        manifold,
                        "output",
                        bitcoin.inputs.len() + position,
                    )
                    .mass
            })
            .sum::<u128>();

    let input_retained =
        manifold.retain(
            input_mass,
            manifold.dimension(),
        );

    let output_retained =
        manifold.retain(
            output_mass,
            manifold.dimension(),
        );

    let retained =
        bitcoin.retained(
            manifold,
        );

    println!(
        "trace=settlement_retained_complete \
         parent_txid={} \
         input_retained_mass={} \
         output_retained_mass={} \
         retained_mass={}",
        parent.txid,
        input_retained.mass,
        output_retained.mass,
        retained.mass,
    );

    println!(
        "trace=settlement_fee_begin \
         parent_txid={}",
        parent.txid,
    );

    let fee =
        bitcoin
            .fee_mass(
                manifold,
                delta,
            ) as u64;

    println!(
        "trace=settlement_fee_complete \
         parent_txid={} \
         fee={}",
        parent.txid,
        fee,
    );

    let last =
        bitcoin
            .outputs
            .len()
            .checked_sub(1)
            .ok_or_else(|| {
                SettlementError::Unsupported(
                    "settlement payload has no outputs",
                )
            })?;

    println!(
        "trace=settlement_output_selected \
         parent_txid={} \
         vout={} \
         outputs={}",
        parent.txid,
        last,
        bitcoin.outputs.len(),
    );

    let input_value =
        parent
            .transaction
            .input
            .iter()
            .enumerate()
            .map(|(input_index, _)| {
                bitcoin
                    .inputs
                    .get(input_index)
                    .map(|_| {
                        parent
                            .transaction
                            .output
                            .iter()
                            .map(|output| output.value.to_sat())
                            .sum::<u64>()
                    })
                    .unwrap_or(0)
            })
            .sum::<u64>();

    let assigned_output_value =
        bitcoin
            .outputs
            .iter()
            .enumerate()
            .filter(|(position, _)| *position != last)
            .map(|(_, output)| output.value)
            .sum::<u64>();

    let final_settlement_value =
        input_value
            .saturating_sub(
                assigned_output_value,
            )
            .saturating_sub(
                fee,
            );

    println!(
        "trace=settlement_value \
         parent_txid={} \
         input_value={} \
         assigned_output_value={} \
         fee={} \
         settlement_value={}",
        parent.txid,
        input_value,
        assigned_output_value,
        fee,
        final_settlement_value,
    );

    println!(
        "trace=settlement_output_update_begin \
         parent_txid={} \
         vout={}",
        parent.txid,
        last,
    );

    let output =
        bitcoin
            .outputs
            .get_mut(last)
            .ok_or_else(|| {
                SettlementError::Unsupported(
                    "settlement output disappeared",
                )
            })?;

    output.value =
        final_settlement_value;

    println!(
        "trace=settlement_output_update_complete \
         parent_txid={} \
         vout={} \
         value={}",
        parent.txid,
        last,
        output.value,
    );

    println!(
        "trace=settlement_transaction_begin \
         parent_txid={}",
        parent.txid,
    );

    let transaction =
        ObservedBitcoinTransaction::new_transaction(
            &bitcoin,
            parent.source.clone(),
        )
        .ok_or_else(|| {
            SettlementError::Unsupported(
                "failed to construct settlement transaction",
            )
        })?;

    println!(
        "trace=settlement_transaction_complete \
         parent_txid={} \
         transaction_txid={}",
        parent.txid,
        transaction
            .transaction
            .compute_txid(),
    );

    println!(
        "trace=settlement_finalized_begin \
         parent_txid={} \
         vout={}",
        parent.txid,
        last,
    );

    let finalized =
        FinalizedSettlement::new(
            Some(settled_receipt),
            parent,
            Some(transaction.transaction),
            last as u32,
        );

    println!(
        "trace=settlement_finalized_complete \
         finalized_txid={} \
         vout={}",
        finalized.txid(),
        finalized.vout,
    );

    println!(
        "trace=settlement_process_complete \
         finalized_txid={} \
         vout={} \
         fee={} \
         settlement_value={} \
         input_retained_mass={} \
         output_retained_mass={} \
         retained_mass={}",
        finalized.txid(),
        finalized.vout,
        fee,
        final_settlement_value,
        input_retained.mass,
        output_retained.mass,
        retained.mass,
    );

    Ok((
        finalized,
        retained,
    ))
}

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
        