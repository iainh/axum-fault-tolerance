# Fault tolerance facets

This directory documents the fault-tolerance facets provided by
`axum-fault-tolerance`. Each document explains the concept, how the library
applies it to async Rust operations, and shows focused builder and macro
example code.

- [Retry](retry.md)
- [Timeout](timeout.md)
- [Fallback](fallback.md)
- [Circuit breaker](circuit-breaker.md)
- [Bulkhead](bulkhead.md)

These facets can be composed with `FaultTolerance::builder()` or applied to
inherent async methods with `#[fault_tolerant]`.
