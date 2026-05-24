# Bulkhead

Bulkhead limits the number of operations that can run at the same time. Use it
to keep one slow or busy dependency from consuming all available concurrency.

## How it works

`Bulkhead` uses a Tokio semaphore. Each call tries to acquire one permit before
the operation starts. If a permit is available, the operation runs and releases
the permit when it finishes. If all permits are in use, the call returns
`Error::BulkheadRejected`.

Clone the same `Bulkhead` into policies that should share the same concurrency
limit. Create separate bulkheads for independent dependency pools.

When bulkhead is combined with retry, each retry must acquire a permit again.

## Example

```rust
use axum_fault_tolerance::{Bulkhead, Error, FaultTolerance};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let policy = FaultTolerance::builder()
        .bulkhead(Bulkhead::new(1))
        .build();

    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());

    let first_policy = policy.clone();
    let first_started = Arc::clone(&started);
    let first_release = Arc::clone(&release);
    let first = tokio::spawn(async move {
        first_policy
            .call(|| {
                let first_started = Arc::clone(&first_started);
                let first_release = Arc::clone(&first_release);
                async move {
                    first_started.notify_one();
                    first_release.notified().await;
                    Ok::<_, &'static str>("first")
                }
            })
            .await
    });

    started.notified().await;

    let rejected = policy
        .call(|| async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok::<_, &'static str>("second")
        })
        .await;

    release.notify_one();

    assert_eq!(rejected, Err(Error::BulkheadRejected));
    assert_eq!(first.await.unwrap(), Ok("first"));
}
```

## Method attribute example

Use `#[bulkhead]` inside a `#[fault_tolerant]` inherent `impl` block to limit
concurrency for one async method. The generated wrapper stores the shared
bulkhead per method.

```rust
use axum_fault_tolerance::{Error, fault_tolerant};
use std::sync::Arc;
use std::time::Duration;

struct Client;

#[fault_tolerant]
impl Client {
    #[bulkhead(max_concurrent = 1)]
    async fn fetch(&self) -> Result<&'static str, &'static str> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok("ok")
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let client = Arc::new(Client);
    let first = tokio::spawn({
        let client = Arc::clone(&client);
        async move { client.fetch().await }
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    let second = client.fetch().await;

    assert_eq!(second, Err(Error::BulkheadRejected));
    assert_eq!(first.await.unwrap(), Ok("ok"));
}
```
