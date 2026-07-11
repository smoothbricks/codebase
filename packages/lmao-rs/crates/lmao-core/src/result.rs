//! Result/retry model, per `specs/lmao/01l_op_context_pattern.md`.
//!
//! The load-bearing pattern: the RETRY POLICY LIVES ON THE ERROR VALUE, the retry
//! loop lives in the span executor ([`crate::context::SpanContext::run_with_retry`]).
//! Ops that can fail transiently return `Err(Transient { .. })`; callers never
//! write retry loops.
//!
//! Rust already gives tagged-error discrimination via enums + `Result`, so the TS
//! `Ok/Err` fluent classes reduce to [`SpanOutcome`] — a thin completion recorder.

/// Retry policy carried by a transient error (`01l`/`01r`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryPolicy {
    /// Exponential backoff: `base_ms * 2^attempt`, up to `max_attempts`.
    ExponentialBackoff { max_attempts: u32, base_ms: u64 },
    /// Fixed delay between a bounded number of attempts.
    FixedDelay { attempts: u32, delay_ms: u64 },
}

impl RetryPolicy {
    pub fn max_attempts(&self) -> u32 {
        match *self {
            Self::ExponentialBackoff { max_attempts, .. } => max_attempts,
            Self::FixedDelay { attempts, .. } => attempts,
        }
    }

    /// Delay before retry number `attempt` (0-based), in milliseconds.
    pub fn delay_ms(&self, attempt: u32) -> u64 {
        match *self {
            Self::ExponentialBackoff { base_ms, .. } => {
                base_ms.saturating_mul(1u64 << attempt.min(32))
            }
            Self::FixedDelay { delay_ms, .. } => delay_ms,
        }
    }
}

/// A transient error: the wrapped error plus how the span executor should retry.
/// Non-transient errors just don't wear this wrapper.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transient<E> {
    pub error: E,
    pub policy: RetryPolicy,
}

impl<E> Transient<E> {
    pub fn exponential(error: E, max_attempts: u32, base_ms: u64) -> Self {
        Self {
            error,
            policy: RetryPolicy::ExponentialBackoff {
                max_attempts,
                base_ms,
            },
        }
    }

    pub fn fixed(error: E, attempts: u32, delay_ms: u64) -> Self {
        Self {
            error,
            policy: RetryPolicy::FixedDelay { attempts, delay_ms },
        }
    }
}

/// How a span completed — what gets written to row 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanOutcome {
    Ok,
    Err,
    /// Row 1's pre-armed state; recorded when the body panicked/was abandoned.
    Exception,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_delays_double_and_saturate() {
        let p = RetryPolicy::ExponentialBackoff {
            max_attempts: 5,
            base_ms: 100,
        };
        assert_eq!(p.delay_ms(0), 100);
        assert_eq!(p.delay_ms(1), 200);
        assert_eq!(p.delay_ms(3), 800);
        // Never overflows even at absurd attempt numbers.
        assert!(p.delay_ms(200) > 0);
    }

    #[test]
    fn fixed_delay_is_flat() {
        let p = RetryPolicy::FixedDelay {
            attempts: 3,
            delay_ms: 5000,
        };
        assert_eq!(p.delay_ms(0), 5000);
        assert_eq!(p.delay_ms(2), 5000);
        assert_eq!(p.max_attempts(), 3);
    }
}
