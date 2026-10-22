use std::env;

use bit_minner::{
    BitcoinObservationPipeline,
    RenderState,
};

fn main() {
    let mut args = env::args().skip(1);

    let substrate = args
        .next()
        .expect("usage: program <substrate> <activation>");

    let activation = args
        .next()
        .expect("usage: program <substrate> <activation>")
        .parse::<u64>()
        .expect("activation must be a valid u64");

    let pipeline = BitcoinObservationPipeline::new(
        activation,
        &substrate,
    )
    .expect("failed to initialize pipeline");

    let mut windows = Vec::new();

    for clip in &pipeline.miner().clips {
        windows.push(
            RenderState::window(
                clip.substrate.clone(),
            )
        );
    }
}