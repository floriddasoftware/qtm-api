use qp_hd::qtm_graph::graph::{
    Direction,
    Scale,
};
use quantom_value::Heritage;

pub struct RenderState {
    pub substrate: String,
    pub active: bool,
    pub direction: Option<Direction>,
}

impl RenderState {

    #[inline(always)]
    pub fn window(
        substrate: String,
    ) -> Self {
        Self {
            substrate,
            active: true,
            direction: None,
        }
    }

    #[inline(always)]
    pub fn frames(
        &mut self,
        heritage: Heritage,
        scale: Scale,
    ) {
        self.direction =
            Some(
                Direction::new(
                    heritage,
                    scale,
                )
            );

        self.active = true;
    }
}