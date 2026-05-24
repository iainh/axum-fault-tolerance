use axum_fault_tolerance::{
    Bulkhead, CircuitBreaker, CircuitBreakerConfig, Error, FaultTolerance, RetryPolicy,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let circuit = CircuitBreaker::new(
        CircuitBreakerConfig::new()
            .request_volume_threshold(4)
            .failure_ratio(0.5)
            .delay(Duration::from_secs(30)),
    );

    let policy = FaultTolerance::builder()
        .timeout(Duration::from_millis(100))
        .retry(
            RetryPolicy::new()
                .max_retries(2)
                .delay(Duration::from_millis(10))
                .jitter(Duration::from_millis(2))
                .max_duration(Duration::from_secs(1)),
        )
        .circuit_breaker(circuit.clone())
        .bulkhead(Bulkhead::new(8))
        .build();

    let value = policy
        .call(|| {
            let attempts = Arc::clone(&attempts);
            async move {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                if attempt < 2 {
                    Err("temporary upstream failure")
                } else {
                    Ok("fresh value")
                }
            }
        })
        .await;

    assert_eq!(value, Ok("fresh value"));
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert_eq!(
        circuit.state(),
        axum_fault_tolerance::CircuitBreakerState::Closed
    );

    let timeout_only = FaultTolerance::builder()
        .timeout(Duration::from_millis(100))
        .build();
    let timed_out = timeout_only
        .call(|| async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok::<_, &'static str>("too slow")
        })
        .await;

    assert_eq!(timed_out, Err(Error::Timeout));
}
