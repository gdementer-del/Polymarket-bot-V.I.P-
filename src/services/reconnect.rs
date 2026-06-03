//! Small reconnect backoff used only outside the trading hot path.

use std::time::Duration;

use tokio::time::sleep;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ReconnectBackoff {
    min_delay_ms: u64,
    max_delay_ms: u64,
    next_delay_ms: u64,
}

impl ReconnectBackoff {
    pub(crate) const fn new(min_delay_ms: u64, max_delay_ms: u64) -> Self {
        Self {
            min_delay_ms,
            max_delay_ms,
            next_delay_ms: min_delay_ms,
        }
    }

    #[cfg(test)]
    pub(crate) const fn current_delay_ms(&self) -> u64 {
        self.next_delay_ms
    }

    pub(crate) fn reset(&mut self) {
        self.next_delay_ms = self.min_delay_ms;
    }

    pub(crate) fn advance(&mut self) {
        self.next_delay_ms = self
            .next_delay_ms
            .saturating_mul(2)
            .clamp(self.min_delay_ms, self.max_delay_ms);
    }

    pub(crate) async fn sleep_next(&mut self) {
        let delay_ms = self.next_delay_ms;
        self.advance();
        sleep(Duration::from_millis(delay_ms)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::ReconnectBackoff;

    #[test]
    fn reconnect_backoff_doubles_until_cap_and_resets() {
        let mut backoff = ReconnectBackoff::new(300, 2_400);

        assert_eq!(backoff.current_delay_ms(), 300);
        backoff.advance();
        assert_eq!(backoff.current_delay_ms(), 600);
        backoff.advance();
        assert_eq!(backoff.current_delay_ms(), 1_200);
        backoff.advance();
        assert_eq!(backoff.current_delay_ms(), 2_400);
        backoff.advance();
        assert_eq!(backoff.current_delay_ms(), 2_400);

        backoff.reset();
        assert_eq!(backoff.current_delay_ms(), 300);
    }
}
