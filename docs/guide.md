# Building resilient Axum services with axum-fault-tolerance

Distributed services fail in ordinary ways: a dependency is slow, a network call
fails briefly, or an overloaded service needs time to recover.
`axum-fault-tolerance` lets you handle those failures with MicroProfile-inspired
method attributes while keeping the execution model Rust-native.

This guide builds a small Axum coffee service and adds retry, timeout, fallback,
circuit breaker and bulkhead policies one at a time with the `#[fault_tolerant]`
macro.

## Prerequisites

To follow this guide, you need:

- Rust 1.85 or later
- Cargo
- About 15 minutes
- Basic familiarity with Axum handlers and Tokio async code

## Create a project

Create a new Axum application:

```sh
cargo new coffee-fault-tolerance
cd coffee-fault-tolerance
```

Add the dependencies:

```toml
[dependencies]
axum = "0.8"
axum-fault-tolerance = "0.1"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "sync", "time"] }
```

When you are working from this repository before the crate is published, use a
path dependency instead:

```toml
axum-fault-tolerance = { path = "../axum-fault-tolerance" }
```

## Build the unreliable service

Start with a small service that returns coffee samples. The repository
intentionally fails or slows down some requests so the fault-tolerance policies
have something to handle.

```rust
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
};
use axum_fault_tolerance::{Error, fault_tolerant};
use serde::Serialize;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

#[derive(Clone, Serialize)]
struct Coffee {
    id: u64,
    name: &'static str,
    country_of_origin: &'static str,
    price: u64,
}

#[derive(Clone)]
struct CoffeeRepository {
    coffees: Arc<HashMap<u64, Coffee>>,
    list_attempts: Arc<AtomicU64>,
    recommendation_attempts: Arc<AtomicU64>,
    availability_attempts: Arc<AtomicU64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CoffeeError {
    RepositoryUnavailable,
    CoffeeNotFound,
}

impl CoffeeRepository {
    fn new() -> Self {
        let coffees = HashMap::from([
            (
                1,
                Coffee {
                    id: 1,
                    name: "Fernandez Espresso",
                    country_of_origin: "Colombia",
                    price: 23,
                },
            ),
            (
                2,
                Coffee {
                    id: 2,
                    name: "La Scala Whole Beans",
                    country_of_origin: "Bolivia",
                    price: 18,
                },
            ),
            (
                3,
                Coffee {
                    id: 3,
                    name: "Dak Lak Filter",
                    country_of_origin: "Vietnam",
                    price: 25,
                },
            ),
        ]);

        Self {
            coffees: Arc::new(coffees),
            list_attempts: Arc::new(AtomicU64::new(0)),
            recommendation_attempts: Arc::new(AtomicU64::new(0)),
            availability_attempts: Arc::new(AtomicU64::new(0)),
        }
    }

    async fn list(&self) -> Result<Vec<Coffee>, CoffeeError> {
        let attempt = self.list_attempts.fetch_add(1, Ordering::SeqCst);
        if attempt % 2 == 0 {
            Err(CoffeeError::RepositoryUnavailable)
        } else {
            Ok(self.coffees.values().cloned().collect())
        }
    }

    async fn recommendations(&self, id: u64) -> Result<Vec<Coffee>, CoffeeError> {
        let attempt = self.recommendation_attempts.fetch_add(1, Ordering::SeqCst);
        if attempt % 2 == 0 {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        if !self.coffees.contains_key(&id) {
            return Err(CoffeeError::CoffeeNotFound);
        }

        Ok(self
            .coffees
            .values()
            .filter(|coffee| coffee.id != id)
            .take(2)
            .cloned()
            .collect())
    }

    async fn fallback_recommendations(&self) -> Result<Vec<Coffee>, CoffeeError> {
        Ok(self.coffees.get(&1).cloned().into_iter().collect())
    }

    async fn availability(&self, id: u64) -> Result<u64, CoffeeError> {
        if !self.coffees.contains_key(&id) {
            return Err(CoffeeError::CoffeeNotFound);
        }

        let attempt = self.availability_attempts.fetch_add(1, Ordering::SeqCst);
        if attempt % 4 > 1 {
            Err(CoffeeError::RepositoryUnavailable)
        } else {
            Ok(30 - attempt % 10)
        }
    }
}

#[derive(Clone)]
struct CoffeeService {
    repository: CoffeeRepository,
}

impl CoffeeService {
    async fn list_coffees(&self) -> Result<Vec<Coffee>, CoffeeError> {
        self.repository.list().await
    }

    async fn recommendations(&self, id: u64) -> Result<Vec<Coffee>, CoffeeError> {
        self.repository.recommendations(id).await
    }

    async fn availability(&self, id: u64) -> Result<u64, CoffeeError> {
        self.repository.availability(id).await
    }
}

#[derive(Clone)]
struct AppState {
    service: CoffeeService,
}

#[tokio::main]
async fn main() {
    let state = AppState {
        service: CoffeeService {
            repository: CoffeeRepository::new(),
        },
    };

    let app = Router::new()
        .route("/coffee", get(coffees))
        .route("/coffee/{id}/recommendations", get(recommendations))
        .route("/coffee/{id}/availability", get(availability))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .expect("bind listener");
    axum::serve(listener, app).await.expect("serve app");
}

async fn coffees(State(state): State<AppState>) -> impl IntoResponse {
    match state.service.list_coffees().await {
        Ok(coffees) => Json(coffees).into_response(),
        Err(error) => coffee_operation_error_response(error),
    }
}

async fn recommendations(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> impl IntoResponse {
    match state.service.recommendations(id).await {
        Ok(recommendations) => Json(recommendations).into_response(),
        Err(error) => coffee_operation_error_response(error),
    }
}

async fn availability(State(state): State<AppState>, Path(id): Path<u64>) -> impl IntoResponse {
    match state.service.availability(id).await {
        Ok(availability) => Json(availability).into_response(),
        Err(error) => coffee_operation_error_response(error),
    }
}

fn coffee_operation_error_response(error: CoffeeError) -> axum::response::Response {
    match error {
        CoffeeError::CoffeeNotFound => (StatusCode::NOT_FOUND, "coffee not found").into_response(),
        CoffeeError::RepositoryUnavailable => {
            (StatusCode::BAD_GATEWAY, "repository unavailable").into_response()
        }
    }
}

fn coffee_policy_error_response(error: Error<CoffeeError>) -> axum::response::Response {
    match error {
        Error::Operation(error) => coffee_operation_error_response(error),
        Error::Timeout => (StatusCode::GATEWAY_TIMEOUT, "operation timed out").into_response(),
        Error::CircuitOpen => (StatusCode::SERVICE_UNAVAILABLE, "circuit open").into_response(),
        Error::BulkheadRejected => {
            (StatusCode::TOO_MANY_REQUESTS, "bulkhead rejected request").into_response()
        }
    }
}
```

Run the application:

```sh
cargo run
```

Open `http://127.0.0.1:3000/coffee`. Some requests fail because
`CoffeeRepository::list` returns `RepositoryUnavailable` on alternating
attempts.

## Add retry

Retry is useful for short-lived failures such as a dropped connection or a
dependency restart. Add `#[fault_tolerant]` to the service `impl`, then put
`#[retry]` on the method that should retry.

```rust
#[fault_tolerant]
impl CoffeeService {
    #[retry(max_retries = 4, delay_ms = 25, jitter_ms = 10)]
    async fn list_coffees(&self) -> Result<Vec<Coffee>, CoffeeError> {
        self.repository.list().await
    }

    async fn recommendations(&self, id: u64) -> Result<Vec<Coffee>, CoffeeError> {
        self.repository.recommendations(id).await
    }

    async fn availability(&self, id: u64) -> Result<u64, CoffeeError> {
        self.repository.availability(id).await
    }
}
```

The generated `list_coffees` wrapper returns
`axum_fault_tolerance::Result<Vec<Coffee>, CoffeeError>`, so update the handler
to map policy errors:

```rust
async fn coffees(State(state): State<AppState>) -> impl IntoResponse {
    match state.service.list_coffees().await {
        Ok(coffees) => Json(coffees).into_response(),
        Err(error) => coffee_policy_error_response(error),
    }
}
```

Now refresh `http://127.0.0.1:3000/coffee`. The first attempt may still fail,
but the method wrapper retries before the handler receives an error.

`max_retries = 4` allows up to five total attempts: the initial call plus four
retries. If all attempts fail, the handler receives `Error::Operation` with the
original `CoffeeError`.

## Add timeout

Timeouts keep a slow dependency from consuming the full request budget. The
recommendations endpoint sometimes sleeps for 500 ms, so give it a shorter
latency budget.

```rust
#[fault_tolerant]
impl CoffeeService {
    #[retry(max_retries = 4, delay_ms = 25, jitter_ms = 10)]
    async fn list_coffees(&self) -> Result<Vec<Coffee>, CoffeeError> {
        self.repository.list().await
    }

    #[timeout(ms = 250)]
    async fn recommendations(&self, id: u64) -> Result<Vec<Coffee>, CoffeeError> {
        self.repository.recommendations(id).await
    }

    async fn availability(&self, id: u64) -> Result<u64, CoffeeError> {
        self.repository.availability(id).await
    }
}
```

Update the recommendations handler to map policy errors:

```rust
async fn recommendations(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> impl IntoResponse {
    match state.service.recommendations(id).await {
        Ok(recommendations) => Json(recommendations).into_response(),
        Err(error) => coffee_policy_error_response(error),
    }
}
```

Refresh `http://127.0.0.1:3000/coffee/2/recommendations`. Slow calls now return
`Error::Timeout`, which the handler maps to `504 Gateway Timeout`.

Timeout applies to one attempt. If you combine timeout with retry, each retry
gets a fresh timeout window.

## Add fallback

Fallback returns a degraded value when the protected operation fails. It is a
good fit for optional features such as recommendations, where stale or generic
data is better than failing the whole request.

Add `#[fallback]` to the recommendations method and define the fallback method
in the same `impl` block:

```rust
#[fault_tolerant]
impl CoffeeService {
    #[retry(max_retries = 4, delay_ms = 25, jitter_ms = 10)]
    async fn list_coffees(&self) -> Result<Vec<Coffee>, CoffeeError> {
        self.repository.list().await
    }

    #[timeout(ms = 250)]
    #[fallback(method = "fallback_recommendations")]
    async fn recommendations(&self, id: u64) -> Result<Vec<Coffee>, CoffeeError> {
        self.repository.recommendations(id).await
    }

    async fn fallback_recommendations(
        &self,
        _id: u64,
        error: Error<CoffeeError>,
    ) -> Result<Vec<Coffee>, CoffeeError> {
        match error {
            Error::Operation(CoffeeError::CoffeeNotFound) => Err(CoffeeError::CoffeeNotFound),
            Error::Operation(CoffeeError::RepositoryUnavailable)
            | Error::Timeout
            | Error::CircuitOpen
            | Error::BulkheadRejected => self.repository.fallback_recommendations().await,
        }
    }

    async fn availability(&self, id: u64) -> Result<u64, CoffeeError> {
        self.repository.availability(id).await
    }
}
```

With `#[fallback]`, the generated wrapper keeps the original
`Result<Vec<Coffee>, CoffeeError>` return type. Change the recommendations
handler back to operation-error mapping:

```rust
async fn recommendations(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> impl IntoResponse {
    match state.service.recommendations(id).await {
        Ok(recommendations) => Json(recommendations).into_response(),
        Err(error) => coffee_operation_error_response(error),
    }
}
```

The fallback receives `Error<CoffeeError>`, so it can distinguish policy
failures from domain errors. In this example, a missing coffee stays a `404`,
while timeouts and repository failures return a default recommendation.

Use the builder API with `FailureClassifier` when you need predicate-based
equivalents of MicroProfile's `retryOn`, `abortOn`, `applyOn`, `skipOn` or
`failOn`.

## Add circuit breaker

A circuit breaker fails fast after recent failures cross a threshold. It keeps a
struggling dependency from receiving more traffic while it recovers.

Add `#[circuit_breaker]` to the availability method:

```rust
#[fault_tolerant]
impl CoffeeService {
    #[retry(max_retries = 4, delay_ms = 25, jitter_ms = 10)]
    async fn list_coffees(&self) -> Result<Vec<Coffee>, CoffeeError> {
        self.repository.list().await
    }

    #[timeout(ms = 250)]
    #[fallback(method = "fallback_recommendations")]
    async fn recommendations(&self, id: u64) -> Result<Vec<Coffee>, CoffeeError> {
        self.repository.recommendations(id).await
    }

    async fn fallback_recommendations(
        &self,
        _id: u64,
        error: Error<CoffeeError>,
    ) -> Result<Vec<Coffee>, CoffeeError> {
        match error {
            Error::Operation(CoffeeError::CoffeeNotFound) => Err(CoffeeError::CoffeeNotFound),
            Error::Operation(CoffeeError::RepositoryUnavailable)
            | Error::Timeout
            | Error::CircuitOpen
            | Error::BulkheadRejected => self.repository.fallback_recommendations().await,
        }
    }

    #[circuit_breaker(request_volume_threshold = 4, failure_ratio = 0.5, delay_ms = 5000)]
    async fn availability(&self, id: u64) -> Result<u64, CoffeeError> {
        self.repository.availability(id).await
    }
}
```

Update the availability handler to map policy errors:

```rust
async fn availability(State(state): State<AppState>, Path(id): Path<u64>) -> impl IntoResponse {
    match state.service.availability(id).await {
        Ok(availability) => Json(availability).into_response(),
        Err(error) => coffee_policy_error_response(error),
    }
}
```

Open `http://127.0.0.1:3000/coffee/2/availability` several times. The
repository alternates between two successful calls and two failed calls. After
four calls, the circuit has seen a 50% failure ratio, so it opens and returns
`Error::CircuitOpen` without calling the repository.

After the delay passes, the circuit allows one half-open probe call. If that
call succeeds, the circuit closes. If it fails, the circuit opens again.

## Add bulkhead

Bulkhead limits the number of concurrent calls to a dependency. Use it when one
slow dependency should not consume all request capacity.

Add `#[bulkhead]` to the same availability method:

```rust
#[bulkhead(max_concurrent = 8)]
#[circuit_breaker(request_volume_threshold = 4, failure_ratio = 0.5, delay_ms = 5000)]
async fn availability(&self, id: u64) -> Result<u64, CoffeeError> {
    self.repository.availability(id).await
}
```

With this policy, at most eight availability calls can run at the same time.
Additional calls fail immediately with `Error::BulkheadRejected`, which the
handler maps to `429 Too Many Requests`.

## Use explicit policies

The macro is the closest fit when a fixed policy belongs to one method. Use
`FaultTolerance::builder()` when the policy is selected at runtime, loaded from
configuration, shared across several closures, or needs a `FailureClassifier`.

```rust
use axum_fault_tolerance::{FaultTolerance, RetryPolicy};

let policy = FaultTolerance::builder()
    .retry(RetryPolicy::new().max_retries(2))
    .timeout(Duration::from_millis(250))
    .build();

let recommendations = policy
    .call(|| async { service.recommendations(2).await })
    .await;
```

This is the same runtime engine used by `#[fault_tolerant]`; the macro just
generates the wrapper code for you.

## Wrap Tower services

The default `tower` feature exposes `FaultToleranceLayer` for Tower services,
including Axum-compatible services.

Add Tower if your application does not already depend on it:

```toml
tower = "0.5"
```

```rust
use axum_fault_tolerance::tower::FaultToleranceLayer;
use axum_fault_tolerance::{FaultTolerance, RetryPolicy};
use tower::ServiceBuilder;

let policy = FaultTolerance::builder()
    .retry(RetryPolicy::new().max_retries(1))
    .timeout(Duration::from_secs(1))
    .build();

let app = ServiceBuilder::new()
    .layer(FaultToleranceLayer::new(policy))
    .service(app);
```

Retries require cloneable requests and services because each retry needs a new
service call. For most application code, method attributes or
`FaultTolerance::call` give more precise control than wrapping the whole router.

## Configure policies

Enable the `mp-config` feature when you want to load policy settings from typed
configuration:

```toml
axum-fault-tolerance = { version = "0.1", features = ["mp-config"] }
```

Supported keys include `timeout`, `max-retries`, `retry-delay`,
`retry-jitter`, `retry-max-duration`, `bulkhead-size`,
`circuit-request-volume`, `circuit-failure-ratio` and `circuit-delay`.
Duration values use the parser provided by `mp-config`, such as `250ms`, `2s`
or `1m`.

## Fault-tolerance mapping

`axum-fault-tolerance` keeps the useful MicroProfile concepts but maps them to
Rust APIs:

- `@Retry` maps to `#[retry]` or `RetryPolicy`
- `@Timeout` maps to `#[timeout]` or `FaultToleranceBuilder::timeout`
- `@Fallback` maps to `#[fallback]` or `call_with_fallback`
- `@CircuitBreaker` maps to `#[circuit_breaker]` or shared `CircuitBreaker` state
- `@Bulkhead` maps to `#[bulkhead]` or `Bulkhead`
- `@Asynchronous` maps to normal Rust `async` functions and futures

Annotated methods must be async inherent methods that take `&self` and return
`Result<T, E>`. Method arguments must use simple identifiers and be cloneable
because the generated retry and fallback wrappers may need to call the method
again.

SmallRye Fault Tolerance also has rate limiting. This crate does not currently
include a rate-limit facet; use a Tower rate-limit layer when that belongs at
the HTTP/service boundary.

## Next steps

Read the focused facet guides for details and smaller examples:

- [Retry](facets/retry.md)
- [Timeout](facets/timeout.md)
- [Fallback](facets/fallback.md)
- [Circuit breaker](facets/circuit-breaker.md)
- [Bulkhead](facets/bulkhead.md)
