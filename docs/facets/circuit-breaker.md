# Circuit breaker

Circuit breaker stops sending calls to a dependency after recent failures cross
a threshold. Use it to fail fast while a dependency is unhealthy and to avoid
adding load while it recovers.

## How it works

`CircuitBreaker` owns shared state. Clone the same breaker into each policy that
should share that state. The breaker starts closed and records recent call
outcomes. When the configured request volume is reached and the failure ratio is
high enough, the breaker opens.

While open, calls are rejected with `Error::CircuitOpen`. After the configured
delay, the breaker allows one half-open probe call. A successful probe closes
the breaker and clears the recent outcomes. A failed probe opens it again.

By default, all failures count against the circuit. Use `FailureClassifier` when
only specific failures should count.

## Example

```rust
use axum_fault_tolerance::{
    CircuitBreaker, CircuitBreakerConfig, CircuitBreakerState, Error, FaultTolerance,
};
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let circuit = CircuitBreaker::new(
        CircuitBreakerConfig::new()
            .request_volume_threshold(2)
            .failure_ratio(0.5)
            .delay(Duration::from_secs(30)),
    );

    let policy = FaultTolerance::builder()
        .circuit_breaker(circuit.clone())
        .build();

    let first = policy.call(|| async { Err::<(), _>("failed") }).await;
    let second = policy.call(|| async { Ok::<_, &'static str>(()) }).await;
    let third = policy.call(|| async { Ok::<_, &'static str>(()) }).await;

    assert_eq!(first, Err(Error::Operation("failed")));
    assert_eq!(second, Ok(()));
    assert_eq!(circuit.state(), CircuitBreakerState::Open);
    assert_eq!(third, Err(Error::CircuitOpen));
}
```

## Ignore selected failures

```rust
use axum_fault_tolerance::{
    CircuitBreaker, CircuitBreakerConfig, CircuitBreakerState, FailureClassifier, FaultTolerance,
};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let circuit = CircuitBreaker::new(
        CircuitBreakerConfig::new()
            .request_volume_threshold(1)
            .failure_ratio(1.0),
    );
    let policy = FaultTolerance::builder()
        .circuit_breaker(circuit.clone())
        .build();

    let classifier = FailureClassifier::new()
        .circuit_skip_on_operation(|error: &&'static str| *error == "validation");

    let result = policy
        .call_classified(classifier, || async {
            Err::<(), &'static str>("validation")
        })
        .await;

    assert!(result.is_err());
    assert_eq!(circuit.state(), CircuitBreakerState::Closed);
}
```

## Method attribute example

Use `#[circuit_breaker]` inside a `#[fault_tolerant]` inherent `impl` block when
one method should own shared circuit-breaker state. The generated wrapper stores
that state per method.

```rust
use axum_fault_tolerance::{Error, fault_tolerant};

struct Client;

#[fault_tolerant]
impl Client {
    #[circuit_breaker(request_volume_threshold = 1, failure_ratio = 1.0, delay_ms = 30000)]
    async fn fetch(&self) -> Result<&'static str, &'static str> {
        Err("offline")
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let client = Client;

    let first = client.fetch().await;
    let second = client.fetch().await;

    assert_eq!(first, Err(Error::Operation("offline")));
    assert_eq!(second, Err(Error::CircuitOpen));
}
```
