use common::{Heartbeats, Storage};

#[derive(Clone)]
pub struct AppState {
    pub storage: Storage,
    pub heartbeats: Heartbeats,
}
