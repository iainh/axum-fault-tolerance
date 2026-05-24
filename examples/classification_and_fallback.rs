use axum_fault_tolerance::{Error, FailureClassifier, FaultTolerance, RetryPolicy};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone, PartialEq, Eq)]
enum UsersError {
    Transient,
    InvalidRequest,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let policy = FaultTolerance::builder()
        .retry(RetryPolicy::new().max_retries(3))
        .build();

    let classifier = FailureClassifier::new()
        .retry_on_operation(|error| matches!(error, UsersError::Transient))
        .abort_on_operation(|error| matches!(error, UsersError::InvalidRequest))
        .fallback_on_operation(|error| matches!(error, UsersError::Transient))
        .skip_fallback_on_operation(|error| matches!(error, UsersError::InvalidRequest));

    let user = policy
        .call_with_classified_fallback(
            classifier,
            || {
                let attempts = Arc::clone(&attempts);
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err::<String, _>(UsersError::Transient)
                }
            },
            |error| async move {
                assert_eq!(error, Error::Operation(UsersError::Transient));
                Ok("cached alice".to_owned())
            },
        )
        .await;

    assert_eq!(user, Ok("cached alice".to_owned()));
    assert_eq!(attempts.load(Ordering::SeqCst), 4);

    let no_fallback = policy
        .call_with_classified_fallback(
            FailureClassifier::new()
                .fallback_on_operation(|error| matches!(error, UsersError::Transient))
                .skip_fallback_on_operation(|error| matches!(error, UsersError::InvalidRequest)),
            || async { Err::<String, _>(UsersError::InvalidRequest) },
            |_error| async { Ok("should not run".to_owned()) },
        )
        .await;

    assert_eq!(
        no_fallback,
        Err(Error::Operation(UsersError::InvalidRequest))
    );
}
