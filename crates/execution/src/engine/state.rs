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

//! An [`ExecutionEngine`]'s state as plain data, for saving a backtest and resuming it.

use std::{cell::RefCell, rc::Rc};

use nautilus_model::identifiers::{ClientOrderId, StrategyId, Venue};
use serde::{Deserialize, Serialize};

use super::ExecutionEngine;

/// An [`ExecutionEngine`]'s replay state: its counts, the position IDs it generated per strategy,
/// the orders it dispatched that are still unrouted, and the venues it listens to.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionEngineState {
    /// The position IDs generated per strategy, sorted by strategy.
    pub position_id_counts: Vec<(StrategyId, usize)>,
    /// The orders dispatched to a client, sorted.
    pub orders_dispatched: Vec<ClientOrderId>,
    /// Commands received.
    pub command_count: u64,
    /// Order events received.
    pub event_count: u64,
    /// Execution reports received.
    pub report_count: u64,
    /// Unclaimed external venue orders filtered.
    pub filtered_unclaimed_external_order_count: u64,
    /// The venues whose instruments the engine listens to, sorted.
    pub instrument_venues: Vec<Venue>,
    /// The venues whose instrument closes the engine listens to, sorted.
    pub instrument_close_venues: Vec<Venue>,
}

impl ExecutionEngine {
    /// Returns the engine's replay state.
    #[must_use]
    pub fn export_state(&self) -> ExecutionEngineState {
        let Self {
            clock: _,             // Rebuilt by setup: the kernel's clock
            cache: _,             // Rebuilt by setup: the kernel's cache
            clients: _,           // Rebuilt by setup: the venues' clients
            default_client_id: _, // Rebuilt by setup
            routing_map: _,       // Rebuilt by setup: the venues' routing
            instrument_venues,
            instrument_close_venues,
            oms_overrides: _,    // Rebuilt by setup: each strategy's OMS type
            external_clients: _, // Rebuilt by setup: the config
            pos_id_generator,
            config: _, // Rebuilt by setup
            orders_dispatched,
            command_count,
            event_count,
            report_count,
            filtered_unclaimed_external_order_count,
            snapshot_anchorer: _, // Rebuilt by setup: the event store's, none in a backtest
        } = self;
        let mut position_id_counts = pos_id_generator.counts();
        position_id_counts.sort_unstable();
        let mut orders_dispatched: Vec<ClientOrderId> =
            orders_dispatched.borrow().iter().copied().collect();
        orders_dispatched.sort_unstable();
        let mut instrument_venues: Vec<Venue> = instrument_venues.iter().copied().collect();
        instrument_venues.sort_unstable();
        let mut instrument_close_venues: Vec<Venue> =
            instrument_close_venues.iter().copied().collect();
        instrument_close_venues.sort_unstable();
        ExecutionEngineState {
            position_id_counts,
            orders_dispatched,
            command_count: command_count.get(),
            event_count: *event_count,
            report_count: *report_count,
            filtered_unclaimed_external_order_count: *filtered_unclaimed_external_order_count,
            instrument_venues,
            instrument_close_venues,
        }
    }

    /// Overwrites the engine's replay state with `state`, subscribing to the instruments and
    /// closes of any venue the saved engine listened to (a no-op for those setup subscribed).
    pub fn import_state(engine: &Rc<RefCell<Self>>, state: ExecutionEngineState) {
        let ExecutionEngineState {
            position_id_counts,
            orders_dispatched,
            command_count,
            event_count,
            report_count,
            filtered_unclaimed_external_order_count,
            instrument_venues,
            instrument_close_venues,
        } = state;
        for venue in instrument_venues {
            Self::subscribe_venue_instruments(engine, venue);
        }
        for venue in instrument_close_venues {
            Self::subscribe_venue_instrument_closes(engine, venue);
        }
        let mut engine = engine.borrow_mut();
        engine.pos_id_generator.reset();
        for (strategy_id, count) in position_id_counts {
            engine.pos_id_generator.set_count(count, strategy_id);
        }
        *engine.orders_dispatched.get_mut() = orders_dispatched.into_iter().collect();
        engine.command_count.set(command_count);
        engine.event_count = event_count;
        engine.report_count = report_count;
        engine.filtered_unclaimed_external_order_count = filtered_unclaimed_external_order_count;
    }
}
