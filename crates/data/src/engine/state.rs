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

//! A [`DataEngine`]'s state as plain data, for saving a backtest and resuming it.
//!
//! The engine's subscriptions are its components' subscriptions: a resume issues each actor's
//! again ([`nautilus_common::actor::resubscribe`]) and the engine rebuilds its own bookkeeping as
//! it processes them. What is left to save is its counts; everything it holds between two data
//! points of a replay (requests, buffered deltas, deferred commands) must be empty at a batch
//! boundary, and what a saved backtest cannot carry (internal bar aggregation, book snapshots at
//! an interval, spread quotes, option chains, synthetics, continuous futures) is refused.

use serde::{Deserialize, Serialize};

use super::DataEngine;

/// A [`DataEngine`]'s counts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataEngineState {
    /// Commands processed.
    pub command_count: u64,
    /// Data processed.
    pub data_count: u64,
    /// Requests processed.
    pub request_count: u64,
    /// Responses processed.
    pub response_count: u64,
}

impl DataEngine {
    /// Returns the engine's counts.
    ///
    /// # Errors
    ///
    /// Returns an error if the engine holds work between two data points (a request, buffered
    /// deltas, a deferred command), or state a saved backtest cannot carry.
    pub fn export_state(&self) -> anyhow::Result<DataEngineState> {
        let Self {
            clock: _,            // Rebuilt by setup
            cache: _,            // Rebuilt by setup
            config: _,           // Rebuilt by setup
            msgbus_priority: _,  // Rebuilt by setup
            external_clients: _, // Rebuilt by setup
            subscriptions_external,
            clients: _,           // Rebuilt by setup, subscriptions issued again
            default_client_id: _, // Rebuilt by setup
            routing_map: _,       // Rebuilt by setup
            book_intervals,
            book_snapshot_counts,
            book_snapshot_sources,
            book_deltas_counts: _, // Rebuilt by the subscriptions issued again
            book_depth_counts: _,  // Rebuilt by the subscriptions issued again
            book_updaters: _,      // Rebuilt by the subscriptions issued again
            book_subscriptions: _, // Rebuilt by the subscriptions issued again
            book_subscription_owners: _, // Rebuilt by the subscriptions issued again
            book_snapshotters,
            bar_aggregators,
            bar_aggregator_handlers,
            subscriptions_bar_aggregation,
            request_bar_aggregations,
            request_pipeline_parent_request,
            request_pipeline_n_components,
            request_pipeline_parent_request_id,
            request_pipeline_responses,
            time_range_pipeline_requests,
            time_range_pipeline_parent_request_id,
            parent_join_request_id,
            pending_join_requests,
            continuous_future_requests,
            continuous_future_subscriptions,
            continuous_future_roller: _, // Rebuilt by setup
            spread_quote_states,
            option_chain_managers,
            option_chain_instrument_index,
            deferred_cmd_queue,
            option_chain_bootstrapper: _, // Rebuilt by setup
            pending_option_chain_requests,
            option_chain_greeks_bootstraps,
            synthetic_quote_feeds,
            synthetic_trade_feeds,
            subscribed_synthetic_quotes,
            subscribed_synthetic_trades,
            buffered_deltas_map,
            deltas_frame: _, // Scratch: cleared before each use
            command_count,
            data_count,
            request_count,
            response_count,
            #[cfg(feature = "streaming")]
                catalogs: _, // Rebuilt by setup
            #[cfg(feature = "defi")]
            pool_updaters,
            #[cfg(feature = "defi")]
            pool_snapshot_pending,
            #[cfg(feature = "defi")]
            pool_event_buffers,
        } = self;
        for (what, empty) in [
            ("pending requests", {
                request_bar_aggregations.is_empty()
                    && request_pipeline_parent_request.is_empty()
                    && request_pipeline_n_components.is_empty()
                    && request_pipeline_parent_request_id.is_empty()
                    && request_pipeline_responses.is_empty()
                    && time_range_pipeline_requests.is_empty()
                    && time_range_pipeline_parent_request_id.is_empty()
                    && parent_join_request_id.is_empty()
                    && pending_join_requests.is_empty()
                    && continuous_future_requests.is_empty()
                    && pending_option_chain_requests.is_empty()
            }),
            ("buffered book deltas", buffered_deltas_map.is_empty()),
            ("deferred commands", deferred_cmd_queue.borrow().is_empty()),
        ] {
            anyhow::ensure!(empty, "the data engine holds {what} at the boundary");
        }
        for (what, empty) in [
            (
                "external subscriptions",
                subscriptions_external.iter().next().is_none(),
            ),
            (
                "book snapshots at an interval",
                book_intervals.is_empty()
                    && book_snapshot_counts.is_empty()
                    && book_snapshot_sources.is_empty()
                    && book_snapshotters.is_empty(),
            ),
            (
                "internally aggregated bars",
                bar_aggregators.is_empty()
                    && bar_aggregator_handlers.is_empty()
                    && subscriptions_bar_aggregation.is_empty(),
            ),
            (
                "continuous futures",
                continuous_future_subscriptions.is_empty(),
            ),
            ("spread quotes", spread_quote_states.is_empty()),
            (
                "option chains",
                option_chain_managers.is_empty()
                    && option_chain_instrument_index.is_empty()
                    && option_chain_greeks_bootstraps.is_empty(),
            ),
            (
                "synthetic instrument feeds",
                synthetic_quote_feeds.is_empty()
                    && synthetic_trade_feeds.is_empty()
                    && subscribed_synthetic_quotes.is_empty()
                    && subscribed_synthetic_trades.is_empty(),
            ),
        ] {
            anyhow::ensure!(
                empty,
                "the data engine holds {what}, which a saved backtest cannot carry"
            );
        }
        #[cfg(feature = "defi")]
        anyhow::ensure!(
            pool_updaters.is_empty()
                && pool_snapshot_pending.is_empty()
                && pool_event_buffers.is_empty(),
            "the data engine holds DeFi pools, which a saved backtest cannot carry"
        );
        Ok(DataEngineState {
            command_count: *command_count,
            data_count: *data_count,
            request_count: *request_count,
            response_count: *response_count,
        })
    }

    /// Sets the engine's counts to `state`'s, after the resumed components' subscriptions were
    /// issued again (which the engine counted).
    pub fn import_state(&mut self, state: DataEngineState) {
        let DataEngineState {
            command_count,
            data_count,
            request_count,
            response_count,
        } = state;
        self.command_count = command_count;
        self.data_count = data_count;
        self.request_count = request_count;
        self.response_count = response_count;
    }
}
