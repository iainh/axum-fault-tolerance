//! MicroProfile-inspired fault tolerance primitives for async Rust.
//!
//! `axum-fault-tolerance` keeps the useful MicroProfile Fault Tolerance ideas:
//! retry, timeout, fallback, circuit breaker, and bulkhead. It adapts them to
//! Rust as explicit runtime policies around async operations instead of Java
//! container interceptors.
//!
//! ```
//! use axum_fault_tolerance::{FaultTolerance, RetryPolicy};
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), axum_fault_tolerance::Error<&'static str>> {
//! let policy = FaultTolerance::builder()
//!     .timeout(Duration::from_secs(1))
//!     .retry(RetryPolicy::new().max_retries(2))
//!     .build();
//!
//! let value = policy
//!     .call(|| async { Ok::<_, &'static str>("ok") })
//!     .await?;
//!
//! assert_eq!(value, "ok");
//! # Ok(())
//! # }
//! ```

extern crate self as axum_fault_tolerance;

pub use axum_fault_tolerance_macros::fault_tolerant;

use std::collections::VecDeque;
use std::error::Error as StdError;
use std::fmt::{self, Display};
use std::future::Future;
#[cfg(feature = "tower")]
use std::pin::Pin;
use std::sync::{Arc, Mutex};
#[cfg(feature = "tower")]
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Boxed error type for applications that do not need a custom error enum.
pub type BoxError = Box<dyn StdError + Send + Sync>;

/// Result returned by fault-tolerant operations.
pub type Result<T, E = BoxError> = std::result::Result<T, Error<E>>;

/// Error returned by a fault-tolerant operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error<E> {
    /// The wrapped operation returned an application error.
    Operation(E),
    /// The operation did not finish before its timeout.
    Timeout,
    /// The circuit breaker rejected the call while open.
    CircuitOpen,
    /// The bulkhead rejected the call because all permits were in use.
    BulkheadRejected,
}

impl<E> Error<E> {
    /// Returns the wrapped operation error, if this is [`Self::Operation`].
    pub fn into_operation(self) -> Option<E> {
        match self {
            Self::Operation(error) => Some(error),
            Self::Timeout | Self::CircuitOpen | Self::BulkheadRejected => None,
        }
    }
}

impl<E> Display for Error<E>
where
    E: Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Operation(error) => Display::fmt(error, formatter),
            Self::Timeout => formatter.write_str("operation timed out"),
            Self::CircuitOpen => formatter.write_str("circuit breaker is open"),
            Self::BulkheadRejected => formatter.write_str("bulkhead rejected the operation"),
        }
    }
}

impl<E> StdError for Error<E>
where
    E: StdError + 'static,
{
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Operation(error) => Some(error),
            Self::Timeout | Self::CircuitOpen | Self::BulkheadRejected => None,
        }
    }
}

/// Retry configuration for an async operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    max_retries: usize,
    delay: Duration,
    max_duration: Option<Duration>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            delay: Duration::ZERO,
            max_duration: None,
        }
    }
}

impl RetryPolicy {
    /// Creates a retry policy with MicroProfile-like defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum number of retries after the initial attempt.
    pub fn max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Sets the delay between attempts.
    pub fn delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    /// Sets the maximum elapsed time for all attempts.
    pub fn max_duration(mut self, max_duration: Duration) -> Self {
        self.max_duration = Some(max_duration);
        self
    }
}

/// Shared circuit breaker state.
#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: Arc<Mutex<CircuitState>>,
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new(CircuitBreakerConfig::default())
    }
}

impl CircuitBreaker {
    /// Creates a circuit breaker with the supplied configuration.
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(CircuitState::Closed {
                outcomes: VecDeque::new(),
            })),
        }
    }

    /// Returns the current circuit breaker state.
    pub fn state(&self) -> CircuitBreakerState {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match &*state {
            CircuitState::Closed { .. } => CircuitBreakerState::Closed,
            CircuitState::Open { .. } => CircuitBreakerState::Open,
            CircuitState::HalfOpen { .. } => CircuitBreakerState::HalfOpen,
        }
    }

    fn before_call(&self) -> std::result::Result<CircuitPermit, Error<()>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match &mut *state {
            CircuitState::Closed { .. } => Ok(CircuitPermit::Closed),
            CircuitState::Open { opened_at } if opened_at.elapsed() >= self.config.delay => {
                *state = CircuitState::HalfOpen {
                    probe_running: true,
                };
                Ok(CircuitPermit::HalfOpenProbe)
            }
            CircuitState::Open { .. } => Err(Error::CircuitOpen),
            CircuitState::HalfOpen { probe_running } if !*probe_running => {
                *probe_running = true;
                Ok(CircuitPermit::HalfOpenProbe)
            }
            CircuitState::HalfOpen { .. } => Err(Error::CircuitOpen),
        }
    }

    fn after_call(&self, success: bool, permit: CircuitPermit) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match permit {
            CircuitPermit::Closed => {
                let CircuitState::Closed { outcomes } = &mut *state else {
                    return;
                };
                outcomes.push_back(success);
                while outcomes.len() > self.config.request_volume_threshold {
                    outcomes.pop_front();
                }
                if outcomes.len() == self.config.request_volume_threshold {
                    let failures = outcomes.iter().filter(|success| !**success).count();
                    let ratio = failures as f64 / self.config.request_volume_threshold as f64;
                    if ratio >= self.config.failure_ratio {
                        *state = CircuitState::Open {
                            opened_at: Instant::now(),
                        };
                    }
                }
            }
            CircuitPermit::HalfOpenProbe => {
                if success {
                    *state = CircuitState::Closed {
                        outcomes: VecDeque::new(),
                    };
                } else {
                    *state = CircuitState::Open {
                        opened_at: Instant::now(),
                    };
                }
            }
        }
    }
}

/// Circuit breaker configuration.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CircuitBreakerConfig {
    request_volume_threshold: usize,
    failure_ratio: f64,
    delay: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            request_volume_threshold: 20,
            failure_ratio: 0.5,
            delay: Duration::from_secs(5),
        }
    }
}

impl CircuitBreakerConfig {
    /// Creates a circuit breaker configuration with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets how many recent calls are considered before the circuit can open.
    ///
    /// A value of zero is treated as one.
    pub fn request_volume_threshold(mut self, threshold: usize) -> Self {
        self.request_volume_threshold = threshold.max(1);
        self
    }

    /// Sets the failure ratio required to open the circuit.
    ///
    /// The value is clamped to `0.0..=1.0`.
    pub fn failure_ratio(mut self, ratio: f64) -> Self {
        self.failure_ratio = ratio.clamp(0.0, 1.0);
        self
    }

    /// Sets how long the circuit stays open before allowing a probe call.
    pub fn delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }
}

/// Public circuit breaker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitBreakerState {
    /// Calls are allowed and recent failures are being tracked.
    Closed,
    /// Calls are rejected until the configured delay has elapsed.
    Open,
    /// One probe call is allowed to decide whether the circuit should close.
    HalfOpen,
}

#[derive(Debug)]
enum CircuitState {
    Closed { outcomes: VecDeque<bool> },
    Open { opened_at: Instant },
    HalfOpen { probe_running: bool },
}

#[derive(Debug, Clone, Copy)]
enum CircuitPermit {
    Closed,
    HalfOpenProbe,
}

/// Semaphore-style bulkhead that limits concurrent operations.
#[derive(Debug, Clone)]
pub struct Bulkhead {
    semaphore: Arc<Semaphore>,
}

impl Bulkhead {
    /// Creates a bulkhead allowing at most `max_concurrent` in-flight calls.
    ///
    /// A value of zero is treated as one.
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent.max(1))),
        }
    }

    fn try_acquire(&self) -> std::result::Result<OwnedSemaphorePermit, Error<()>> {
        self.semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::BulkheadRejected)
    }
}

/// Builder for [`FaultTolerance`].
#[derive(Debug, Clone, Default)]
pub struct FaultToleranceBuilder {
    retry: Option<RetryPolicy>,
    timeout: Option<Duration>,
    circuit_breaker: Option<CircuitBreaker>,
    bulkhead: Option<Bulkhead>,
}

impl FaultToleranceBuilder {
    /// Enables retry handling.
    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = Some(retry);
        self
    }

    /// Enables timeout handling.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Uses a shared circuit breaker.
    pub fn circuit_breaker(mut self, circuit_breaker: CircuitBreaker) -> Self {
        self.circuit_breaker = Some(circuit_breaker);
        self
    }

    /// Uses a semaphore-style bulkhead.
    pub fn bulkhead(mut self, bulkhead: Bulkhead) -> Self {
        self.bulkhead = Some(bulkhead);
        self
    }

    /// Finishes the policy set.
    pub fn build(self) -> FaultTolerance {
        FaultTolerance {
            retry: self.retry,
            timeout: self.timeout,
            circuit_breaker: self.circuit_breaker,
            bulkhead: self.bulkhead,
        }
    }
}

/// Composable fault tolerance policy set.
#[derive(Debug, Clone, Default)]
pub struct FaultTolerance {
    retry: Option<RetryPolicy>,
    timeout: Option<Duration>,
    circuit_breaker: Option<CircuitBreaker>,
    bulkhead: Option<Bulkhead>,
}

impl FaultTolerance {
    /// Starts building a policy set.
    pub fn builder() -> FaultToleranceBuilder {
        FaultToleranceBuilder::default()
    }

    /// Runs an async operation with the configured policies.
    ///
    /// Retries wrap the complete attempt, so each retry enters the circuit
    /// breaker and bulkhead again. This matches the useful MicroProfile
    /// behaviour without relying on Java interceptor machinery.
    pub async fn call<F, Fut, T, E>(&self, mut operation: F) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = std::result::Result<T, E>>,
    {
        let Some(retry) = self.retry else {
            return self.call_once(&mut operation).await;
        };

        let started_at = Instant::now();
        let mut retries = 0;

        loop {
            let result = self.call_once(&mut operation).await;
            if result.is_ok() || retries >= retry.max_retries {
                return result;
            }

            retries += 1;

            if let Some(max_duration) = retry.max_duration
                && started_at.elapsed() >= max_duration
            {
                return result;
            }

            if !retry.delay.is_zero() {
                tokio::time::sleep(retry.delay).await;
            }
        }
    }

    /// Runs an async operation and invokes `fallback` if the policy set fails.
    pub async fn call_with_fallback<F, Fut, Fb, FbFut, T, E>(
        &self,
        operation: F,
        fallback: Fb,
    ) -> std::result::Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = std::result::Result<T, E>>,
        Fb: FnOnce(Error<E>) -> FbFut,
        FbFut: Future<Output = std::result::Result<T, E>>,
    {
        match self.call(operation).await {
            Ok(value) => Ok(value),
            Err(error) => fallback(error).await,
        }
    }

    async fn call_once<F, Fut, T, E>(&self, operation: &mut F) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = std::result::Result<T, E>>,
    {
        let circuit_permit = match &self.circuit_breaker {
            Some(circuit_breaker) => Some(
                circuit_breaker
                    .before_call()
                    .map_err(|error| error.map_operation_error())?,
            ),
            None => None,
        };

        let bulkhead_permit = match &self.bulkhead {
            Some(bulkhead) => Some(
                bulkhead
                    .try_acquire()
                    .map_err(|error| error.map_operation_error())?,
            ),
            None => None,
        };

        let result = match self.timeout {
            Some(timeout) => tokio::time::timeout(timeout, operation())
                .await
                .map_err(|_| Error::Timeout)
                .and_then(|result| result.map_err(Error::Operation)),
            None => operation().await.map_err(Error::Operation),
        };

        drop(bulkhead_permit);

        if let (Some(circuit_breaker), Some(circuit_permit)) =
            (&self.circuit_breaker, circuit_permit)
        {
            circuit_breaker.after_call(result.is_ok(), circuit_permit);
        }

        result
    }
}

#[cfg(feature = "tower")]
/// Tower integration for applying fault tolerance to Axum-compatible services.
pub mod tower {
    use super::*;
    use tower_layer::Layer;
    use tower_service::Service;

    /// Tower layer that wraps a service with a [`FaultTolerance`] policy set.
    #[derive(Debug, Clone)]
    pub struct FaultToleranceLayer {
        policy: FaultTolerance,
    }

    impl FaultToleranceLayer {
        /// Creates a new layer from a policy set.
        pub fn new(policy: FaultTolerance) -> Self {
            Self { policy }
        }
    }

    impl<S> Layer<S> for FaultToleranceLayer {
        type Service = FaultToleranceService<S>;

        fn layer(&self, inner: S) -> Self::Service {
            FaultToleranceService {
                inner,
                policy: self.policy.clone(),
            }
        }
    }

    /// Tower service wrapper produced by [`FaultToleranceLayer`].
    #[derive(Debug, Clone)]
    pub struct FaultToleranceService<S> {
        inner: S,
        policy: FaultTolerance,
    }

    impl<S> FaultToleranceService<S> {
        /// Wraps a service directly without constructing a layer.
        pub fn new(inner: S, policy: FaultTolerance) -> Self {
            Self { inner, policy }
        }
    }

    impl<S, Request> Service<Request> for FaultToleranceService<S>
    where
        S: Service<Request> + Clone + Send + 'static,
        S::Future: Send + 'static,
        S::Response: Send + 'static,
        S::Error: Send + 'static,
        Request: Clone + Send + 'static,
    {
        type Response = S::Response;
        type Error = Error<S::Error>;
        type Future =
            Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(
            &mut self,
            context: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            self.inner.poll_ready(context).map_err(Error::Operation)
        }

        fn call(&mut self, request: Request) -> Self::Future {
            let policy = self.policy.clone();
            let inner = self.inner.clone();
            Box::pin(async move {
                policy
                    .call(move || {
                        let mut inner = inner.clone();
                        let request = request.clone();
                        async move { inner.call(request).await }
                    })
                    .await
            })
        }
    }
}

trait MapOperationError {
    fn map_operation_error<E>(self) -> Error<E>;
}

impl MapOperationError for Error<()> {
    fn map_operation_error<E>(self) -> Error<E> {
        match self {
            Self::Operation(()) => {
                unreachable!("internal policy errors never wrap operation errors")
            }
            Self::Timeout => Error::Timeout,
            Self::CircuitOpen => Error::CircuitOpen,
            Self::BulkheadRejected => Error::BulkheadRejected,
        }
    }
}

/// Runs an operation with only timeout handling.
pub async fn timeout<Fut, T, E>(duration: Duration, future: Fut) -> Result<T, E>
where
    Fut: Future<Output = std::result::Result<T, E>>,
{
    tokio::time::timeout(duration, future)
        .await
        .map_err(|_| Error::Timeout)
        .and_then(|result| result.map_err(Error::Operation))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn retries_until_operation_succeeds() {
        let attempts = AtomicUsize::new(0);
        let policy = FaultTolerance::builder()
            .retry(RetryPolicy::new().max_retries(3))
            .build();

        let result = policy
            .call(|| async {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                if attempt < 2 {
                    Err("not yet")
                } else {
                    Ok("ok")
                }
            })
            .await;

        assert_eq!(result, Ok("ok"));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn timeout_fails_slow_operation() {
        let policy = FaultTolerance::builder()
            .timeout(Duration::from_millis(5))
            .build();

        let result = policy
            .call(|| async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok::<_, &'static str>("late")
            })
            .await;

        assert_eq!(result, Err(Error::Timeout));
    }

    #[tokio::test]
    async fn fallback_receives_policy_error() {
        let policy = FaultTolerance::builder()
            .timeout(Duration::from_millis(5))
            .build();

        let result = policy
            .call_with_fallback(
                || async {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    Ok::<_, &'static str>("late")
                },
                |error| async move {
                    assert_eq!(error, Error::Timeout);
                    Ok("fallback")
                },
            )
            .await;

        assert_eq!(result, Ok("fallback"));
    }

    #[tokio::test]
    async fn circuit_breaker_opens_after_failure_threshold() {
        let circuit = CircuitBreaker::new(
            CircuitBreakerConfig::new()
                .request_volume_threshold(2)
                .failure_ratio(0.5)
                .delay(Duration::from_secs(60)),
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

    #[tokio::test]
    async fn circuit_breaker_allows_probe_after_delay() {
        let circuit = CircuitBreaker::new(
            CircuitBreakerConfig::new()
                .request_volume_threshold(1)
                .failure_ratio(1.0)
                .delay(Duration::from_millis(1)),
        );
        let policy = FaultTolerance::builder()
            .circuit_breaker(circuit.clone())
            .build();

        let failed = policy.call(|| async { Err::<(), _>("failed") }).await;
        assert_eq!(failed, Err(Error::Operation("failed")));
        assert_eq!(circuit.state(), CircuitBreakerState::Open);

        tokio::time::sleep(Duration::from_millis(2)).await;

        let probe = policy.call(|| async { Ok::<_, &'static str>(()) }).await;

        assert_eq!(probe, Ok(()));
        assert_eq!(circuit.state(), CircuitBreakerState::Closed);
    }

    #[tokio::test]
    async fn bulkhead_rejects_when_full() {
        let policy = FaultTolerance::builder().bulkhead(Bulkhead::new(1)).build();
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
                        Ok::<_, &'static str>(())
                    }
                })
                .await
        });

        started.notified().await;
        let rejected = policy.call(|| async { Ok::<_, &'static str>(()) }).await;
        release.notify_one();

        assert_eq!(rejected, Err(Error::BulkheadRejected));
        assert_eq!(first.await.unwrap(), Ok(()));
    }

    #[cfg(feature = "tower")]
    #[tokio::test]
    async fn tower_service_applies_retry_policy() {
        use crate::tower::FaultToleranceLayer;
        use std::future::{Ready, ready};
        use tower_layer::Layer;
        use tower_service::Service;

        #[derive(Clone)]
        struct FlakyService {
            attempts: Arc<AtomicUsize>,
        }

        impl Service<()> for FlakyService {
            type Response = &'static str;
            type Error = &'static str;
            type Future = Ready<std::result::Result<Self::Response, Self::Error>>;

            fn poll_ready(
                &mut self,
                _context: &mut Context<'_>,
            ) -> Poll<std::result::Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, _request: ()) -> Self::Future {
                match self.attempts.fetch_add(1, Ordering::SeqCst) {
                    0 => ready(Err("try again")),
                    _ => ready(Ok("ok")),
                }
            }
        }

        let attempts = Arc::new(AtomicUsize::new(0));
        let policy = FaultTolerance::builder()
            .retry(RetryPolicy::new().max_retries(1))
            .build();
        let layer = FaultToleranceLayer::new(policy);
        let mut service = layer.layer(FlakyService {
            attempts: Arc::clone(&attempts),
        });

        let result = service.call(()).await;

        assert_eq!(result, Ok("ok"));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn macro_wraps_method_with_retry_and_fallback() {
        struct Client {
            attempts: AtomicUsize,
        }

        #[fault_tolerant]
        impl Client {
            #[retry(max_retries = 1)]
            #[fallback(method = "fallback_user")]
            async fn user(&self, id: u64) -> std::result::Result<String, &'static str> {
                let _ = id;
                self.attempts.fetch_add(1, Ordering::SeqCst);
                Err("offline")
            }

            async fn fallback_user(
                &self,
                id: u64,
                error: Error<&'static str>,
            ) -> std::result::Result<String, &'static str> {
                assert_eq!(id, 42);
                assert_eq!(error, Error::Operation("offline"));
                Ok("cached".to_owned())
            }
        }

        let client = Client {
            attempts: AtomicUsize::new(0),
        };

        let result = client.user(42).await;

        assert_eq!(result, Ok("cached".to_owned()));
        assert_eq!(client.attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn macro_circuit_breaker_state_is_shared_per_method() {
        struct Client;

        #[fault_tolerant]
        impl Client {
            #[circuit_breaker(request_volume_threshold = 1, failure_ratio = 1.0, delay_ms = 60000)]
            async fn fail(&self) -> std::result::Result<(), &'static str> {
                Err("failed")
            }
        }

        let client = Client;

        let first = client.fail().await;
        let second = client.fail().await;

        assert_eq!(first, Err(Error::Operation("failed")));
        assert_eq!(second, Err(Error::CircuitOpen));
    }
}
