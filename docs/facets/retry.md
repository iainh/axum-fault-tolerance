# Retry

Retry runs a failed operation again before returning the error to the caller.
Use it for transient failures such as brief network interruptions, temporary
service overload, or a database connection that is being re-established.

## How it works

`RetryPolicy` controls how many attempts the policy makes after the initial
call. It can also add a delay, jitter and a maximum total duration for all
attempts. The wrapped operation is called once for the initial attempt and once
for each retry until it succeeds or the retry policy stops.

By default, retries apply to every `Error` produced by the policy set. Use
`FailureClassifier` when only specific operation errors should retry or when
specific errors should abort immediately.

Retries wrap the full attempt. If the policy also has a circuit breaker,
bulkhead or timeout, each retry enters those facets again.

## Example

```rust
use axum_fault_tolerance::{FaultTolerance, RetryPolicy};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() -> axum_fault_tolerance::Result<(), &'static str> {
    let attempts = Arc::new(AtomicUsize::new(0));
    let policy = FaultTolerance::builder()
        .retry(
            RetryPolicy::new()
                .max_retries(2)
                .delay(Duration::from_millis(50))
                .jitter(Duration::from_millis(10))
                .max_duration(Duration::from_secs(1)),
        )
        .build();

    let value = policy
        .call(|| {
            let attempts = Arc::clone(&attempts);
            async move {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                if attempt < 2 {
                    Err("temporary failure")
                } else {
                    Ok("ok")
                }
            }
        })
        .await?;

    assert_eq!(value, "ok");
    assert_eq!(attempts.load(Ordering::SeqCst), 3);

    Ok(())
}
```

## Retry only some errors

```rust
use axum_fault_tolerance::{FailureClassifier, FaultTolerance, RetryPolicy};

#[derive(Debug, PartialEq, Eq)]
enum ClientError {
    Transient,
    InvalidRequest,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let policy = FaultTolerance::builder()
        .retry(RetryPolicy::new().max_retries(3))
        .build();

    let classifier = FailureClassifier::new()
        .retry_on_operation(|error| *error == ClientError::Transient)
        .abort_on_operation(|error| *error == ClientError::InvalidRequest);

    let result = policy
        .call_classified(classifier, || async {
            Err::<(), _>(ClientError::InvalidRequest)
        })
        .await;

    assert!(result.is_err());
}
```

## Method attribute example

Use `#[retry]` inside a `#[fault_tolerant]` inherent `impl` block when the
policy belongs directly to one async method.

```rust
use axum_fault_tolerance::fault_tolerant;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Client {
    attempts: AtomicUsize,
}

#[fault_tolerant]
impl Client {
    #[retry(max_retries = 2, delay_ms = 50, jitter_ms = 10, max_duration_ms = 1000)]
    async fn fetch(&self) -> Result<&'static str, &'static str> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        if attempt < 2 {
            Err("temporary failure")
        } else {
            Ok("ok")
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let client = Client {
        attempts: AtomicUsize::new(0),
    };

    let result = client.fetch().await;

    assert_eq!(result, Ok("ok"));
    assert_eq!(client.attempts.load(Ordering::SeqCst), 3);
}
```
