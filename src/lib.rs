//! MicroProfile-style fault tolerance for Axum and async Rust.
//!
//! `axum-fault-tolerance` brings the useful parts of Eclipse MicroProfile Fault
//! Tolerance to Rust request handlers, service clients and Tower services. The
//! vocabulary stays familiar: retry, timeout, fallback, circuit breaker and
//! bulkhead. The execution model is Rust-native: explicit policy values wrap
//! async operations instead of Java container interceptors wrapping annotated
//! CDI beans.
//!
//! Use [`FaultTolerance`] directly around fallible async work in handlers,
//! extractors, repositories or clients. Enable the default `tower` feature to
//! wrap Axum-compatible Tower services with [`tower::FaultToleranceLayer`].
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
//!
//! The [`fault_tolerant`] macro is available when method-level attributes are a
//! better fit. It keeps the MicroProfile annotation style while still expanding
//! to the same runtime policies used by [`FaultTolerance`].
//!
//! See `docs/facets/` for focused guides covering each fault-tolerance facet.

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
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Boxed operation error for applications that do not need a custom error enum.
pub type BoxError = Box<dyn StdError + Send + Sync>;

/// Result returned by fault-tolerant operations and method wrappers.
///
/// The outer [`Error`] identifies whether the failure came from the protected
/// operation or from a policy such as timeout, circuit breaker or bulkhead.
pub type Result<T, E = BoxError> = std::result::Result<T, Error<E>>;

/// Error returned by a protected operation or by a policy decision.
///
/// MicroProfile models these cases as exceptions from interceptors. This crate
/// keeps them explicit so Axum handlers and Tower services can decide whether
/// to map them to HTTP responses, logs, metrics or fallbacks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error<E> {
    /// The wrapped operation returned an application or service error.
    Operation(E),
    /// The operation did not finish before the configured timeout.
    Timeout,
    /// The circuit breaker rejected the call while the circuit was open.
    CircuitOpen,
    /// The bulkhead rejected the call because all concurrency permits were in use.
    BulkheadRejected,
}

impl<E> Error<E> {
    fn as_ref(&self) -> Error<&E> {
        match self {
            Self::Operation(error) => Error::Operation(error),
            Self::Timeout => Error::Timeout,
            Self::CircuitOpen => Error::CircuitOpen,
            Self::BulkheadRejected => Error::BulkheadRejected,
        }
    }
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

type FailurePredicate<E> = Arc<dyn Fn(Error<&E>) -> bool + Send + Sync>;

/// Classification rules for retry, fallback and circuit breaker decisions.
///
/// MicroProfile classifies Java exceptions with attributes such as `retryOn`,
/// `abortOn`, `applyOn`, `skipOn`, and `failOn`. Rust applications usually know
/// richer error values, so this type uses predicates over [`Error`].
///
/// Use a classifier when an Axum route should retry upstream transport errors,
/// skip fallback for validation errors, or prevent expected domain errors from
/// counting against a circuit breaker.
pub struct FailureClassifier<E> {
    retry_on: Option<FailurePredicate<E>>,
    abort_on: Option<FailurePredicate<E>>,
    fallback_on: Option<FailurePredicate<E>>,
    skip_fallback_on: Option<FailurePredicate<E>>,
    circuit_failure_on: Option<FailurePredicate<E>>,
    circuit_skip_on: Option<FailurePredicate<E>>,
}

impl<E> Clone for FailureClassifier<E> {
    fn clone(&self) -> Self {
        Self {
            retry_on: self.retry_on.clone(),
            abort_on: self.abort_on.clone(),
            fallback_on: self.fallback_on.clone(),
            skip_fallback_on: self.skip_fallback_on.clone(),
            circuit_failure_on: self.circuit_failure_on.clone(),
            circuit_skip_on: self.circuit_skip_on.clone(),
        }
    }
}

impl<E> Default for FailureClassifier<E> {
    fn default() -> Self {
        Self {
            retry_on: None,
            abort_on: None,
            fallback_on: None,
            skip_fallback_on: None,
            circuit_failure_on: None,
            circuit_skip_on: None,
        }
    }
}

impl<E> std::fmt::Debug for FailureClassifier<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FailureClassifier")
            .finish_non_exhaustive()
    }
}

impl<E> FailureClassifier<E> {
    /// Creates a classifier with permissive defaults.
    ///
    /// By default, all failures are retryable, trigger fallback, and count as
    /// circuit breaker failures. Add predicates to narrow that behaviour.
    pub fn new() -> Self {
        Self::default()
    }

    /// Retries only when `predicate` returns true.
    ///
    /// This is the Rust predicate form of MicroProfile's `retryOn`.
    pub fn retry_on_error(
        mut self,
        predicate: impl Fn(Error<&E>) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.retry_on = Some(Arc::new(predicate));
        self
    }

    /// Prevents retries when `predicate` returns true.
    ///
    /// This is the Rust predicate form of MicroProfile's `abortOn`.
    pub fn abort_on_error(
        mut self,
        predicate: impl Fn(Error<&E>) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.abort_on = Some(Arc::new(predicate));
        self
    }

    /// Applies fallback only when `predicate` returns true.
    ///
    /// This is the Rust predicate form of MicroProfile's `applyOn`.
    pub fn fallback_on_error(
        mut self,
        predicate: impl Fn(Error<&E>) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.fallback_on = Some(Arc::new(predicate));
        self
    }

    /// Skips fallback when `predicate` returns true.
    ///
    /// This is the Rust predicate form of MicroProfile's `skipOn`.
    pub fn skip_fallback_on_error(
        mut self,
        predicate: impl Fn(Error<&E>) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.skip_fallback_on = Some(Arc::new(predicate));
        self
    }

    /// Counts only failures where `predicate` returns true against the circuit.
    ///
    /// This is the Rust predicate form of MicroProfile's `failOn`.
    pub fn circuit_failure_on_error(
        mut self,
        predicate: impl Fn(Error<&E>) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.circuit_failure_on = Some(Arc::new(predicate));
        self
    }

    /// Prevents failures where `predicate` returns true from counting against
    /// the circuit.
    pub fn circuit_skip_on_error(
        mut self,
        predicate: impl Fn(Error<&E>) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.circuit_skip_on = Some(Arc::new(predicate));
        self
    }

    /// Retries only operation errors where `predicate` returns true.
    pub fn retry_on_operation(
        self,
        predicate: impl Fn(&E) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.retry_on_error(move |error| match error {
            Error::Operation(error) => predicate(error),
            Error::Timeout | Error::CircuitOpen | Error::BulkheadRejected => false,
        })
    }

    /// Prevents retries for operation errors where `predicate` returns true.
    pub fn abort_on_operation(
        self,
        predicate: impl Fn(&E) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.abort_on_error(move |error| match error {
            Error::Operation(error) => predicate(error),
            Error::Timeout | Error::CircuitOpen | Error::BulkheadRejected => false,
        })
    }

    /// Applies fallback only for operation errors where `predicate` returns true.
    pub fn fallback_on_operation(
        self,
        predicate: impl Fn(&E) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.fallback_on_error(move |error| match error {
            Error::Operation(error) => predicate(error),
            Error::Timeout | Error::CircuitOpen | Error::BulkheadRejected => false,
        })
    }

    /// Skips fallback for operation errors where `predicate` returns true.
    pub fn skip_fallback_on_operation(
        self,
        predicate: impl Fn(&E) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.skip_fallback_on_error(move |error| match error {
            Error::Operation(error) => predicate(error),
            Error::Timeout | Error::CircuitOpen | Error::BulkheadRejected => false,
        })
    }

    /// Counts only operation errors where `predicate` returns true against the circuit.
    pub fn circuit_failure_on_operation(
        self,
        predicate: impl Fn(&E) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.circuit_failure_on_error(move |error| match error {
            Error::Operation(error) => predicate(error),
            Error::Timeout | Error::CircuitOpen | Error::BulkheadRejected => false,
        })
    }

    /// Prevents operation errors where `predicate` returns true from counting
    /// against the circuit.
    pub fn circuit_skip_on_operation(
        self,
        predicate: impl Fn(&E) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.circuit_skip_on_error(move |error| match error {
            Error::Operation(error) => predicate(error),
            Error::Timeout | Error::CircuitOpen | Error::BulkheadRejected => false,
        })
    }

    fn should_retry(&self, error: Error<&E>) -> bool {
        if self
            .abort_on
            .as_ref()
            .is_some_and(|predicate| predicate(error.clone()))
        {
            return false;
        }

        self.retry_on
            .as_ref()
            .is_none_or(|predicate| predicate(error))
    }

    fn should_fallback(&self, error: Error<&E>) -> bool {
        if self
            .skip_fallback_on
            .as_ref()
            .is_some_and(|predicate| predicate(error.clone()))
        {
            return false;
        }

        self.fallback_on
            .as_ref()
            .is_none_or(|predicate| predicate(error))
    }

    fn is_circuit_failure(&self, error: Error<&E>) -> bool {
        if self
            .circuit_skip_on
            .as_ref()
            .is_some_and(|predicate| predicate(error.clone()))
        {
            return false;
        }

        self.circuit_failure_on
            .as_ref()
            .is_none_or(|predicate| predicate(error))
    }
}

/// Retry configuration for an async operation.
///
/// This is the runtime equivalent of MicroProfile's `@Retry`. In Axum code,
/// place it on a [`FaultTolerance`] policy used around a handler dependency,
/// service client call, queue operation or other fallible async work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    max_retries: usize,
    delay: Duration,
    jitter: Duration,
    max_duration: Option<Duration>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            delay: Duration::ZERO,
            jitter: Duration::ZERO,
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
    ///
    /// `max_retries(2)` permits up to three total attempts.
    pub fn max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Sets the base delay between attempts.
    pub fn delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    /// Sets the random variation applied to retry delays.
    ///
    /// Effective delays are chosen from `delay - jitter` through
    /// `delay + jitter`, saturating at zero.
    pub fn jitter(mut self, jitter: Duration) -> Self {
        self.jitter = jitter;
        self
    }

    /// Sets the maximum elapsed time for all attempts.
    ///
    /// This bounds the retry loop itself. Pair it with
    /// [`FaultToleranceBuilder::timeout`] when each individual attempt also
    /// needs a latency budget.
    pub fn max_duration(mut self, max_duration: Duration) -> Self {
        self.max_duration = Some(max_duration);
        self
    }
}

/// Shared circuit breaker state for failing fast while a dependency recovers.
///
/// This is the runtime equivalent of MicroProfile's `@CircuitBreaker`. Clone
/// one breaker into every policy that should share the same request-volume and
/// failure-ratio window, such as all calls from an Axum application to one
/// upstream service.
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
    ///
    /// This is useful for health endpoints, metrics and tests. Normal request
    /// flow does not need to inspect the state before calling a policy.
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
///
/// The configuration mirrors the MicroProfile model: a recent request volume,
/// a failure ratio and a delay before the half-open probe. Durations use
/// [`Duration`] so Axum applications can configure them from normal Rust
/// settings rather than annotation literals.
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
///
/// This is the async Rust equivalent of MicroProfile's `@Bulkhead`. It protects
/// the rest of an Axum application from one busy dependency by rejecting calls
/// once the configured in-flight limit is reached.
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

/// Builder for a [`FaultTolerance`] policy set.
///
/// The builder composes MicroProfile-style facets into the policy object that
/// Axum handlers, Tower services or application clients can reuse.
#[derive(Debug, Clone, Default)]
pub struct FaultToleranceBuilder {
    retry: Option<RetryPolicy>,
    timeout: Option<Duration>,
    circuit_breaker: Option<CircuitBreaker>,
    bulkhead: Option<Bulkhead>,
}

impl FaultToleranceBuilder {
    /// Enables retry handling for protected operations.
    pub fn retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = Some(retry);
        self
    }

    /// Enables timeout handling for each protected attempt.
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

/// Composable fault-tolerance policy set.
///
/// `FaultTolerance` is the main Axum-facing API. Build one policy for a
/// dependency or operation class, clone it into handlers or service layers, and
/// call async work through [`Self::call`] or [`Self::call_with_fallback`].
///
/// The policy applies MicroProfile-style facets in a Rust-friendly way:
/// bulkhead and circuit breaker checks happen before an attempt, timeout wraps
/// the attempt future, retry repeats the whole attempt, and fallback can turn a
/// policy failure into a degraded value.
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
    ///
    /// In Axum code, the closure usually calls a database, HTTP client, queue
    /// producer or other fallible dependency.
    pub async fn call<F, Fut, T, E>(&self, mut operation: F) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = std::result::Result<T, E>>,
    {
        self.call_classified(FailureClassifier::new(), &mut operation)
            .await
    }

    /// Runs an async operation with explicit failure classification.
    ///
    /// Use this when `retryOn`, `abortOn`, `applyOn`, `skipOn` or `failOn`
    /// semantics need to depend on the actual Rust error value.
    pub async fn call_classified<F, Fut, T, E>(
        &self,
        classifier: FailureClassifier<E>,
        mut operation: F,
    ) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = std::result::Result<T, E>>,
    {
        let Some(retry) = self.retry else {
            return self.call_once(&classifier, &mut operation).await;
        };

        let started_at = Instant::now();
        let mut retries = 0;

        loop {
            let result = self.call_once(&classifier, &mut operation).await;
            let Err(error) = &result else {
                return result;
            };

            if retries >= retry.max_retries || !classifier.should_retry(error.as_ref()) {
                return result;
            }

            retries += 1;

            if let Some(max_duration) = retry.max_duration
                && started_at.elapsed() >= max_duration
            {
                return result;
            }

            let delay = retry.delay_for_attempt(retries);
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
        }
    }

    /// Runs an async operation and invokes `fallback` if the policy set fails.
    ///
    /// This is the runtime equivalent of MicroProfile's `@Fallback`. The
    /// fallback receives the policy error so it can distinguish an upstream
    /// error from timeout, circuit-open and bulkhead rejection cases.
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

    /// Runs an async operation with explicit failure classification and fallback.
    ///
    /// Use this when only some failures should trigger a degraded response.
    pub async fn call_with_classified_fallback<F, Fut, Fb, FbFut, T, E>(
        &self,
        classifier: FailureClassifier<E>,
        operation: F,
        fallback: Fb,
    ) -> std::result::Result<T, Error<E>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = std::result::Result<T, E>>,
        Fb: FnOnce(Error<E>) -> FbFut,
        FbFut: Future<Output = std::result::Result<T, E>>,
    {
        match self.call_classified(classifier.clone(), operation).await {
            Ok(value) => Ok(value),
            Err(error) if classifier.should_fallback(error.as_ref()) => {
                fallback(error).await.map_err(Error::Operation)
            }
            Err(error) => Err(error),
        }
    }

    async fn call_once<F, Fut, T, E>(
        &self,
        classifier: &FailureClassifier<E>,
        operation: &mut F,
    ) -> Result<T, E>
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
            let circuit_success = match &result {
                Ok(_) => true,
                Err(error) => !classifier.is_circuit_failure(error.as_ref()),
            };
            circuit_breaker.after_call(circuit_success, circuit_permit);
        }

        result
    }
}

impl RetryPolicy {
    fn delay_for_attempt(self, attempt: usize) -> Duration {
        jitter_delay(self.delay, self.jitter, attempt)
    }
}

fn jitter_delay(delay: Duration, jitter: Duration, attempt: usize) -> Duration {
    if jitter.is_zero() {
        return delay;
    }

    let delay_nanos = duration_nanos(delay);
    let jitter_nanos = duration_nanos(jitter);
    let spread = jitter_nanos.saturating_mul(2).saturating_add(1);
    let offset = pseudo_random_nanos(attempt) % spread;

    if offset <= jitter_nanos {
        nanos_duration(delay_nanos.saturating_sub(jitter_nanos - offset))
    } else {
        nanos_duration(delay_nanos.saturating_add(offset - jitter_nanos))
    }
}

fn duration_nanos(duration: Duration) -> u128 {
    duration.as_nanos()
}

fn nanos_duration(nanos: u128) -> Duration {
    let capped = nanos.min(u64::MAX as u128);
    Duration::from_nanos(capped as u64)
}

fn pseudo_random_nanos(attempt: usize) -> u128 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut value = now ^ ((attempt as u128).wrapping_mul(0x9e37_79b9_7f4a_7c15));
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(feature = "tower")]
/// Tower integration for applying fault tolerance to Axum-compatible services.
///
/// This module adapts the same MicroProfile-style facets used by
/// [`FaultTolerance`] to Tower's `Layer` and `Service` traits. Use it when the
/// fault-tolerance boundary is a whole service rather than a single client call
/// inside a handler.
pub mod tower {
    use super::*;
    use tower_layer::Layer;
    use tower_service::Service;

    /// Tower layer that wraps a service with a [`FaultTolerance`] policy set.
    ///
    /// In an Axum stack, this is the layer form of applying retry, timeout,
    /// circuit breaker and bulkhead policies around a service.
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
    ///
    /// The wrapped service must be cloneable because retries need a fresh
    /// service value for each attempt. Requests must also be cloneable so the
    /// same request can be replayed when retry is enabled.
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
///
/// This is the lightweight equivalent of using `@Timeout` or
/// [`FaultToleranceBuilder::timeout`] when no other facet is needed.
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
    async fn classifier_can_abort_retries_for_operation_errors() {
        #[derive(Debug, PartialEq, Eq)]
        enum ClientError {
            Permanent,
        }

        let attempts = AtomicUsize::new(0);
        let policy = FaultTolerance::builder()
            .retry(RetryPolicy::new().max_retries(3))
            .build();
        let classifier =
            FailureClassifier::new().abort_on_operation(|error| *error == ClientError::Permanent);

        let result = policy
            .call_classified(classifier, || async {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(ClientError::Permanent)
            })
            .await;

        assert_eq!(result, Err(Error::Operation(ClientError::Permanent)));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn classifier_can_skip_fallback() {
        let policy = FaultTolerance::default();
        let classifier = FailureClassifier::<&'static str>::new().skip_fallback_on_error(
            |error| matches!(error, Error::Operation(error) if *error == "do-not-fallback"),
        );

        let result = policy
            .call_with_classified_fallback(
                classifier,
                || async { Err::<(), &'static str>("do-not-fallback") },
                |_error| async { Ok(()) },
            )
            .await;

        assert_eq!(result, Err(Error::Operation("do-not-fallback")));
    }

    #[tokio::test]
    async fn classifier_can_skip_circuit_failures() {
        let circuit = CircuitBreaker::new(
            CircuitBreakerConfig::new()
                .request_volume_threshold(1)
                .failure_ratio(1.0)
                .delay(Duration::from_secs(60)),
        );
        let policy = FaultTolerance::builder()
            .circuit_breaker(circuit.clone())
            .build();
        let classifier = FailureClassifier::<&'static str>::new().circuit_skip_on_error(
            |error| matches!(error, Error::Operation(error) if *error == "ignored"),
        );

        let result = policy
            .call_classified(classifier, || async { Err::<(), &'static str>("ignored") })
            .await;

        assert_eq!(result, Err(Error::Operation("ignored")));
        assert_eq!(circuit.state(), CircuitBreakerState::Closed);
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

    #[test]
    fn retry_jitter_stays_within_configured_bounds() {
        let delay = Duration::from_millis(50);
        let jitter = Duration::from_millis(10);

        for attempt in 1..100 {
            let effective = jitter_delay(delay, jitter, attempt);
            assert!(effective >= Duration::from_millis(40));
            assert!(effective <= Duration::from_millis(60));
        }
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
