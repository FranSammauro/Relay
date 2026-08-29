use common::{Heartbeats, RateLimiter, Storage};

#[derive(Clone)]
pub struct AppState {
    pub storage: Storage,
    pub heartbeats: Heartbeats,
    pub rate_limiter: RateLimiter,
}