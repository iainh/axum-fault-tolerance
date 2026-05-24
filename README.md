# axum-fault-tolerance

`axum-fault-tolerance` provides MicroProfile-inspired fault tolerance primitives
for async Rust. It keeps the useful policy model from Eclipse MicroProfile Fault
Tolerance, but exposes it as explicit Rust runtime configuration instead of Java
container annotations and interceptors.

## High-level features

- Retry async operations.
- Fail slow operations with timeouts.
- Provide typed fallbacks.
- Share circuit breaker state across calls.
- Limit concurrency with semaphore-style bulkheads.

## Example

```rust
use axum_fault_tolerance::{FaultTolerance, RetryPolicy};
use std::time::Duration;

# async fn example() -> Result<(), axum_fault_tolerance::Error<&'static str>> {
let policy = FaultTolerance::builder()
    .timeout(Duration::from_secs(1))
    .retry(
        RetryPolicy::new()
            .max_retries(2)
            .delay(Duration::from_millis(100))
            .jitter(Duration::from_millis(25)),
    )
    .build();

let user = policy
    .call(|| async {
        // Call a database, service client, queue, or other fallible async work.
        Ok::<_, &'static str>("alice")
    })
    .await?;

assert_eq!(user, "alice");
# Ok(())
# }
```

## Runnable examples

The `examples/` directory contains direct, runnable examples for the main
concepts:

- `policy_stack.rs` combines retry, timeout, circuit breaker, and bulkhead.
- `classification_and_fallback.rs` shows retry/fallback classification rules.
- `method_attributes.rs` shows `#[fault_tolerant]` method attributes.
- `tower_service.rs` shows wrapping an Axum-compatible Tower service.

Run one with `cargo run --example policy_stack`.

## Failure classification

Use `FailureClassifier` when only some failures should retry, trigger fallback,
or count against the circuit breaker. This is the Rust version of
MicroProfile's `retryOn`, `abortOn`, `applyOn`, `skipOn`, and `failOn`
exception-class attributes.

```rust
use axum_fault_tolerance::{FailureClassifier, FaultTolerance, RetryPolicy};

#[derive(Debug, PartialEq, Eq)]
enum UsersError {
    Transient,
    Validation,
}

# async fn example() {
let policy = FaultTolerance::builder()
    .retry(RetryPolicy::new().max_retries(3))
    .build();

let classifier = FailureClassifier::new()
    .retry_on_operation(|error: &UsersError| *error == UsersError::Transient)
    .abort_on_operation(|error| *error == UsersError::Validation);

let result = policy
    .call_classified(classifier, || async {
        Err::<(), _>(UsersError::Transient)
    })
    .await;
# let _ = result;
# }
```

## Fallback

```rust
use axum_fault_tolerance::{FaultTolerance, fault_tolerant};
use std::time::Duration;

# async fn example() -> Result<(), &'static str> {
let policy = FaultTolerance::builder()
    .timeout(Duration::from_millis(100))
    .build();

let value = policy
    .call_with_fallback(
        || async { Err::<String, _>("upstream unavailable") },
        |_error| async { Ok("cached value".to_owned()) },
    )
    .await?;

assert_eq!(value, "cached value");
# Ok(())
# }
```

## Java-style method attributes

Use `#[fault_tolerant]` on an inherent `impl` block to enable method-level
policy attributes. The annotated method body stays focused on the operation;
the generated wrapper applies the configured policies.

```rust
use axum_fault_tolerance::{Error, fault_tolerant};

struct Users {
    client: UsersClient,
}

#[fault_tolerant]
impl Users {
    #[retry(max_retries = 2, delay_ms = 100)]
    #[timeout(ms = 500)]
    #[fallback(method = "cached_user")]
    async fn load_user(&self, id: String) -> Result<User, UsersError> {
        self.client.get(id).await
    }

    async fn cached_user(
        &self,
        id: String,
        error: Error<UsersError>,
    ) -> Result<User, UsersError> {
        self.client.cached(id, error).await
    }
}
# struct UsersClient;
# struct User;
# struct UsersError;
# impl UsersClient {
#     async fn get(&self, _id: String) -> Result<User, UsersError> { Ok(User) }
#     async fn cached(&self, _id: String, _error: Error<UsersError>) -> Result<User, UsersError> { Ok(User) }
# }
```

Supported method attributes:

- `#[retry(max_retries = 3, delay_ms = 100, jitter_ms = 25, max_duration_ms = 1000)]`
- `#[timeout(ms = 500)]`
- `#[fallback(method = "fallback_method")]`
- `#[circuit_breaker(request_volume_threshold = 20, failure_ratio = 0.5, delay_ms = 5000)]`
- `#[bulkhead(max_concurrent = 32)]`

Without a fallback, generated wrappers return `axum_fault_tolerance::Result<T,
E>` so timeout, circuit-open, and bulkhead-rejection errors are visible. With a
fallback, generated wrappers keep the original `Result<T, E>` return type.

## Circuit breaker and bulkhead

```rust
use axum_fault_tolerance::{
    Bulkhead, CircuitBreaker, CircuitBreakerConfig, FaultTolerance,
};
use std::time::Duration;

let circuit = CircuitBreaker::new(
    CircuitBreakerConfig::new()
        .request_volume_threshold(10)
        .failure_ratio(0.5)
        .delay(Duration::from_secs(30)),
);

let policy = FaultTolerance::builder()
    .circuit_breaker(circuit)
    .bulkhead(Bulkhead::new(32))
    .build();
```

## Tower and Axum

Enable the default `tower` feature to wrap Axum-compatible services:

```rust
use axum_fault_tolerance::tower::FaultToleranceLayer;
use axum_fault_tolerance::{FaultTolerance, RetryPolicy};

let policy = FaultTolerance::builder()
    .retry(RetryPolicy::new().max_retries(1))
    .build();

let layer = FaultToleranceLayer::new(policy);
```

Retries require cloneable requests and services, which matches many Axum/Tower
use cases but keeps the core crate independent of Axum.

## MicroProfile mapping

MicroProfile Fault Tolerance describes Java methods annotated with `@Retry`,
`@Timeout`, `@Fallback`, `@CircuitBreaker`, `@Bulkhead`, and `@Asynchronous`.
This crate maps those concepts to Rust runtime policy objects:

- `@Retry` maps to `RetryPolicy`.
- `@Timeout` maps to `FaultToleranceBuilder::timeout`.
- `@Fallback` maps to `call_with_fallback`.
- `@CircuitBreaker` maps to shared `CircuitBreaker` state.
- `@Bulkhead` maps to `Bulkhead`.
- `@Asynchronous` maps to normal Rust `async` functions and futures.

The crate intentionally does not model CDI, Jakarta interceptors, throwable
class matching, or container-managed thread pools. Those concepts are replaced
with explicit Rust types and async execution.
