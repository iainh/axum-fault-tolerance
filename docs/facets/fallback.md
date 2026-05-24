# Fallback

Fallback provides an alternate result when the protected operation fails. Use it
for cached values, degraded responses, static defaults or a secondary data
source.

## How it works

`call_with_fallback` runs the operation through the configured policy set. If
the operation succeeds, the successful value is returned. If the policy returns
an `Error`, the fallback receives that error and can produce the same success
type.

Use `call_with_classified_fallback` with `FailureClassifier` when fallback
should apply only to specific failures. This mirrors MicroProfile's `applyOn`
and `skipOn` behaviour with Rust predicates over typed errors.

## Example

```rust
use axum_fault_tolerance::{Error, FaultTolerance};
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), &'static str> {
    let policy = FaultTolerance::builder()
        .timeout(Duration::from_millis(100))
        .build();

    let value = policy
        .call_with_fallback(
            || async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok::<_, &'static str>("fresh value")
            },
            |error| async move {
                assert_eq!(error, Error::Timeout);
                Ok("cached value")
            },
        )
        .await?;

    assert_eq!(value, "cached value");

    Ok(())
}
```

## Classified fallback

```rust
use axum_fault_tolerance::{Error, FailureClassifier, FaultTolerance};

#[derive(Debug, PartialEq, Eq)]
enum UsersError {
    Offline,
    InvalidRequest,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let policy = FaultTolerance::default();
    let classifier = FailureClassifier::new()
        .fallback_on_operation(|error| *error == UsersError::Offline)
        .skip_fallback_on_operation(|error| *error == UsersError::InvalidRequest);

    let value = policy
        .call_with_classified_fallback(
            classifier,
            || async { Err::<String, _>(UsersError::Offline) },
            |error| async move {
                assert_eq!(error, Error::Operation(UsersError::Offline));
                Ok("cached user".to_owned())
            },
        )
        .await;

    assert_eq!(value, Ok("cached user".to_owned()));
}
```

## Method attribute example

Use `#[fallback]` with another policy attribute when a method has a natural
degraded response. The fallback method receives the original arguments followed
by `axum_fault_tolerance::Error<E>`.

```rust
use axum_fault_tolerance::{Error, fault_tolerant};

#[derive(Debug, PartialEq, Eq)]
enum UsersError {
    Offline,
}

struct Users;

#[fault_tolerant]
impl Users {
    #[retry(max_retries = 1)]
    #[fallback(method = "cached_user")]
    async fn load_user(&self, id: u64) -> Result<String, UsersError> {
        assert_eq!(id, 42);
        Err(UsersError::Offline)
    }

    async fn cached_user(
        &self,
        id: u64,
        error: Error<UsersError>,
    ) -> Result<String, UsersError> {
        assert_eq!(id, 42);
        assert_eq!(error, Error::Operation(UsersError::Offline));
        Ok("cached user 42".to_owned())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), UsersError> {
    let users = Users;
    let value = users.load_user(42).await?;

    assert_eq!(value, "cached user 42");

    Ok(())
}
```
