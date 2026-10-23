// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use nautilus_core::DurationNanos;
use serde::{Deserialize, Serialize};

/// Configuration for `OrderMatchingEngine` instances.
///
/// With `defer_option_settlement`, automatic checks at the exact option expiry
/// timestamp cancel orders but defer settlement to `process_instrument_expiration`.
/// To settle at expiry, the caller schedules that call after processing the
/// timestamp's market data. Automatic checks after expiry can also settle.
/// Explicit contract-close events bypass this deferral.
#[derive(Debug, Clone, Deserialize, Serialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
pub struct OrderMatchingEngineConfig {
    #[builder(default = true)]
    pub bar_execution: bool,
    #[builder(default)]
    pub bar_adaptive_high_low_ordering: bool,
    #[builder(default = true)]
    pub trade_execution: bool,
    #[builder(default)]
    pub liquidity_consumption: bool,
    #[builder(default = true)]
    pub reject_stop_orders: bool,
    #[builder(default = true)]
    pub support_gtd_orders: bool,
    #[builder(default = true)]
    pub support_contingent_orders: bool,
    #[builder(default = true)]
    pub use_position_ids: bool,
    #[builder(default)]
    pub use_random_ids: bool,
    #[builder(default = true)]
    pub use_reduce_only: bool,
    #[builder(default)]
    pub use_market_order_acks: bool,
    #[builder(default)]
    pub queue_position: bool,
    /// If a book update that crosses a resting limit order fills it. When `false`, a resting
    /// limit fills only while a trade tick is processed, and only when that trade could fill it
    /// (a seller, or no aggressor, at or below a bid's limit; a buyer, or none, at or above an
    /// ask's): a book that moves through its price with no trade leaves it resting, its queue
    /// position updated as the book changes. Fills on arrival (a marketable new or modified
    /// order) are unaffected.
    #[builder(default = true)]
    pub book_cross_fills: bool,
    /// How far back, on the venue's clock, a trade reaches for the book deltas that reflect it.
    ///
    /// A venue can publish a trade and the delta that removes its size from the book on channels
    /// not ordered with each other, so the delta often arrives first. With `queue_position` on an
    /// `L2_MBP` book, that delta caps the queue ahead of an order at the level and the trade then
    /// eats the queue again: each print counts twice. With a window set, the engine keeps the
    /// queue each level reduction took from each order, and processes a trade as if it arrived
    /// ahead of the deltas that reflect it: the reductions at its price on the resting side
    /// stamped at or before it, at most the window earlier, newest first, until its size is
    /// covered, and every reduction stamped after it, at most the window later. Their queue is
    /// given back, the trade decrements the queue and fills as usual, then the queue is capped
    /// again at the level's current size. A reduction arriving after a trade that covers it gives
    /// nothing back.
    /// `None` (the default) processes data in arrival order.
    pub trade_reflection_window: Option<DurationNanos>,
    #[builder(default)]
    pub oto_full_trigger: bool,
    #[builder(default)]
    pub defer_option_settlement: bool,
    pub price_protection_points: Option<u32>,
}

impl Default for OrderMatchingEngineConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_default() {
        let config = OrderMatchingEngineConfig::default();
        assert!(config.bar_execution);
        assert!(!config.bar_adaptive_high_low_ordering);
        assert!(config.trade_execution);
        assert!(config.book_cross_fills);
        assert!(!config.liquidity_consumption);
        assert!(config.reject_stop_orders);
        assert!(config.support_gtd_orders);
        assert!(config.support_contingent_orders);
        assert!(config.use_position_ids);
        assert!(!config.use_random_ids);
        assert!(config.use_reduce_only);
        assert!(!config.use_market_order_acks);
        assert!(!config.queue_position);
        assert!(!config.oto_full_trigger);
        assert!(!config.defer_option_settlement);
        assert_eq!(config.trade_reflection_window, None);
        assert_eq!(config.price_protection_points, None);
    }
}
