use axum_fault_tolerance::tower::FaultToleranceLayer;
use axum_fault_tolerance::{FaultTolerance, RetryPolicy};
use std::future::{Ready, ready};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use tower_layer::Layer;
use tower_service::Service;

#[derive(Clone)]
struct FlakyService {
    attempts: Arc<AtomicUsize>,
}

impl Service<()> for FlakyService {
    type Response = &'static str;
    type Error = &'static str;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _request: ()) -> Self::Future {
        match self.attempts.fetch_add(1, Ordering::SeqCst) {
            0 => ready(Err("try again")),
            _ => ready(Ok("ok")),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let policy = FaultTolerance::builder()
        .retry(RetryPolicy::new().max_retries(1))
        .build();
    let layer = FaultToleranceLayer::new(policy);
    let mut service = layer.layer(FlakyService {
        attempts: Arc::clone(&attempts),
    });

    let response = service.call(()).await;

    assert_eq!(response, Ok("ok"));
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}
