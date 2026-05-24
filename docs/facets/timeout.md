# Timeout

Timeout fails an operation when it does not finish within a configured duration.
Use it to keep slow dependencies from consuming request time indefinitely.

## How it works

`FaultToleranceBuilder::timeout` wraps the operation with Tokio's timer. If the
future completes before the duration expires, the result is returned. If the
duration expires first, the call returns `Error::Timeout`.

Timeout applies to one attempt. When timeout is combined with retry, each retry
gets a new timeout window.

The crate also exposes a standalone `timeout` helper for code that only needs
timeout handling.

## Example

```rust
use axum_fault_tolerance::{Error, FaultTolerance};
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let policy = FaultTolerance::builder()
        .timeout(Duration::from_millis(100))
        .build();

    let result = policy
        .call(|| async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok::<_, &'static str>("late response")
        })
        .await;

    assert_eq!(result, Err(Error::Timeout));
}
```

## Standalone timeout

```rust
use axum_fault_tolerance::{Error, timeout};
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let result = timeout(Duration::from_millis(100), async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok::<_, &'static str>("late response")
    })
    .await;

    assert_eq!(result, Err(Error::Timeout));
}
```

## Method attribute example

Use `#[timeout]` inside a `#[fault_tolerant]` inherent `impl` block when one
method has a fixed latency budget.

```rust
use axum_fault_tolerance::{Error, fault_tolerant};
use std::time::Duration;

struct Client;

#[fault_tolerant]
impl Client {
    #[timeout(ms = 100)]
    async fn fetch(&self) -> Result<&'static str, &'static str> {
        tokio::time::sleep(Duration::from_secs(1)).await;
        Ok("late response")
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let client = Client;
    let result = client.fetch().await;

    assert_eq!(result, Err(Error::Timeout));
}
```
