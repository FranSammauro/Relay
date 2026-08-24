use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("job not found: {0}")]
    NotFound(uuid::Uuid),

    #[error("invalid job payload: {0}")]
    InvalidPayload(String),

    #[error("job {0} is not in a valid state for this operation")]
    InvalidState(uuid::Uuid),
}
