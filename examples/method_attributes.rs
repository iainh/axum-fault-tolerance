use axum_fault_tolerance::{Error, fault_tolerant};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone, PartialEq, Eq)]
enum UsersError {
    Offline,
}

struct Users {
    attempts: AtomicUsize,
}

#[fault_tolerant]
impl Users {
    #[retry(max_retries = 1, delay_ms = 10, jitter_ms = 2, max_duration_ms = 100)]
    #[timeout(ms = 50)]
    #[fallback(method = "cached_user")]
    #[circuit_breaker(request_volume_threshold = 2, failure_ratio = 1.0, delay_ms = 1000)]
    #[bulkhead(max_concurrent = 4)]
    async fn load_user(&self, id: u64) -> Result<String, UsersError> {
        assert_eq!(id, 42);
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Err(UsersError::Offline)
    }

    async fn cached_user(&self, id: u64, error: Error<UsersError>) -> Result<String, UsersError> {
        assert_eq!(id, 42);
        assert_eq!(error, Error::Operation(UsersError::Offline));
        Ok("cached user 42".to_owned())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), UsersError> {
    let users = Users {
        attempts: AtomicUsize::new(0),
    };

    let user = users.load_user(42).await?;

    assert_eq!(user, "cached user 42");
    assert_eq!(users.attempts.load(Ordering::SeqCst), 2);

    Ok(())
}
