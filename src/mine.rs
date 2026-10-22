use bit_minner::{
    Miner,
    RenderState,
};

fn main() {
    let miner = Miner::new();

    let mut windows = Vec::new();

    for clip in &miner.clips {
        windows.push(
            RenderState::window(
                clip.substrate.clone(),
            )
        );
    }
}