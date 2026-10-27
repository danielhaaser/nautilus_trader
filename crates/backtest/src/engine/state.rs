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

//! Saving a streamed backtest at a batch boundary and resuming it in another process.
//!
//! "Rebuild the wiring, overwrite the data": the resuming host builds a [`BacktestEngine`] by the
//! same setup as the saving one (the same venues, the same instruments added in the same order,
//! the same actors and strategies), then [`BacktestEngine::import_state`] overwrites everything
//! the replay changed and starts the engine without its first-run setup, and the host's next
//! `run` continues where the saved engine stopped. The continued run is the uninterrupted one:
//! every order event, fill, position and account state at the same instant, the same IDs.
//!
//! [`BacktestEngine::export_state`] is called only between streaming batches: after a
//! `run(.., streaming = true)` returned and `clear_data`, never after `end`. What legitimately
//! spans a boundary is saved (commands the latency model holds, events its event leg holds, the
//! risk engine's reservations, every timer); what must be empty there is checked and refused
//! otherwise (the command queues, pending callbacks, the time-event accumulator, data requests).
//!
//! Callbacks are closures, so they are named rather than saved. The engine re-attaches its own
//! (instrument expiration, funding settlement and event release alerts); every other timer that
//! fires through a named callback gets it from the host's resolver.

use std::rc::Rc;

use ahash::AHashSet;
use nautilus_common::{
    actor,
    cache::state::CacheState,
    clock::{Clock, RebuiltTimers, VirtualClock, VirtualClockState},
    logging::{logging_clock_set_static_mode, logging_clock_set_static_time},
    runner::{data_cmd_queue_is_empty, drain_data_cmd_queue, trading_cmd_queue_is_empty},
    timer::{TimeEvent, TimeEventCallback},
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_data::engine::state::DataEngineState;
use nautilus_execution::engine::{ExecutionEngine, state::ExecutionEngineState};
use nautilus_model::identifiers::{InstrumentId, Venue};
use nautilus_portfolio::portfolio::{is_snapshot_timer, state::PortfolioResumeState};
use nautilus_risk::engine::state::RiskEngineState;
use nautilus_system::trader::TraderResumeState;
use serde::{Deserialize, Serialize};
use ustr::Ustr;

use super::{BacktestEngine, CALLBACK_DRAIN_BUDGET};
use crate::exchange::state::SimulatedExchangeState;

/// The [`EngineState`] format this engine writes and reads.
pub const ENGINE_STATE_FORMAT: u32 = 1;

/// The kernel clock's name in [`EngineState::clocks`].
const KERNEL_CLOCK: &str = "kernel";

/// The name prefix of the engine's instrument expiration alerts.
const EXPIRATION_PREFIX: &str = "INSTRUMENT-EXPIRATION:";
/// The name prefix of the engine's funding settlement alerts.
const FUNDING_PREFIX: &str = "FUNDING-SETTLEMENT:";
/// The name prefix of the venues' event release alerts.
const EVENT_RELEASE_PREFIX: &str = "SimulatedExchange.event_release.";

/// A [`BacktestEngine`]'s complete state at a batch boundary, as plain data.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EngineState {
    /// The format version ([`ENGINE_STATE_FORMAT`]).
    pub format: u32,
    /// The data items processed.
    pub iteration: usize,
    /// The last instant processed.
    pub last_ns: UnixNanos,
    /// The last instant the venue modules ran.
    pub last_module_ns: Option<UnixNanos>,
    /// The last instant liquidations were checked.
    pub last_liquidation_ns: Option<UnixNanos>,
    /// The last batch's end.
    pub end_ns: UnixNanos,
    /// The run's config ID.
    pub run_config_id: Option<String>,
    /// The run's ID.
    pub run_id: Option<UUID4>,
    /// When the run started (wall clock).
    pub run_started: Option<UnixNanos>,
    /// The run's start.
    pub backtest_start: Option<UnixNanos>,
    /// When the kernel started.
    pub kernel_started: Option<UnixNanos>,
    /// The instruments whose book data reached their venue, sorted.
    pub has_book_processed: Vec<InstrumentId>,
    /// The time event accumulator's next sequence.
    pub accumulator_next_sequence: u64,
    /// The instrument expiration alerts armed since setup, sorted.
    pub expiration_alerts_armed: Vec<Ustr>,
    /// The instrument expiration alerts cancelled since setup, sorted.
    pub expiration_alerts_cancelled: Vec<Ustr>,
    /// Every clock: the kernel's, then each component's in registration order.
    pub clocks: Vec<(String, VirtualClockState)>,
    /// The cache.
    pub cache: CacheState,
    /// The portfolio.
    pub portfolio: PortfolioResumeState,
    /// The data engine's counts.
    pub data_engine: DataEngineState,
    /// The risk engine.
    pub risk_engine: RiskEngineState,
    /// The execution engine.
    pub exec_engine: ExecutionEngineState,
    /// The actors and strategies.
    pub trader: TraderResumeState,
    /// Each venue, in the order added.
    pub venues: Vec<(Venue, SimulatedExchangeState)>,
}

fn virtual_clock(clock: &mut dyn Clock) -> anyhow::Result<&mut VirtualClock> {
    clock
        .as_any_mut()
        .downcast_mut::<VirtualClock>()
        .ok_or_else(|| anyhow::anyhow!("a backtest clock is not a VirtualClock"))
}

/// Whether a callback registered under `name` for no timer must be saved: the engine's own are
/// always set again with their callback, so a stale one is never reached.
fn keep_stale(name: &str) -> bool {
    !(name.starts_with(EXPIRATION_PREFIX)
        || name.starts_with(FUNDING_PREFIX)
        || name.starts_with(EVENT_RELEASE_PREFIX)
        || is_snapshot_timer(name))
}

impl BacktestEngine {
    /// Returns the engine's complete state, between two streaming batches.
    ///
    /// # Errors
    ///
    /// Returns an error if the engine is not at a batch boundary of a running replay (not
    /// started, ended, stopped, or data still loaded), if anything that must be empty at a
    /// boundary is not (the trading or data command queue, pending callbacks, the time event
    /// accumulator, an execution client's buffered events, a data request), or if the engine
    /// holds something a saved backtest cannot carry (named in the error).
    pub fn export_state(&self) -> anyhow::Result<EngineState> {
        let Self {
            kernel,
            instance_id: _, // Rebuilt by setup
            config: _,      // Rebuilt by setup
            accumulator,
            run_config_id,
            run_id,
            venues,
            exec_clients,
            has_data,
            has_book_data,
            has_book_processed,
            data_iterator: _, // Checked empty through `data_len`
            data_len,
            data_stream_counter,
            ts_first,
            ts_last_data,
            sorted: _, // Constant at a boundary: `clear_data` sets it
            iteration,
            force_stop,
            last_ns,
            last_module_ns,
            last_liquidation_ns,
            end_ns,
            run_started,
            run_finished,
            backtest_start,
            backtest_end,
            funding_error,
            reference_settlement: _, // Rebuilt by setup
            expiration_alerts_armed,
            expiration_alerts_cancelled,
        } = self;

        anyhow::ensure!(
            run_started.is_some() && *iteration > 0,
            "the replay has not started: nothing to save"
        );
        anyhow::ensure!(
            run_finished.is_none() && backtest_end.is_none(),
            "the replay has ended: a saved state continues a replay, not an ended one"
        );
        anyhow::ensure!(
            !*force_stop && !kernel.is_shutdown_requested(),
            "the replay is stopping"
        );
        anyhow::ensure!(funding_error.is_none(), "funding failed: {funding_error:?}");
        if let Some(error) = actor::callback_failure() {
            anyhow::bail!("a callback failed: {error}");
        }
        if let Some(error) = kernel.portfolio.borrow().balance_error() {
            anyhow::bail!("a balance update failed: {error}");
        }
        anyhow::ensure!(
            *data_len == 0
                && *data_stream_counter == 0
                && ts_first.is_none()
                && ts_last_data.is_none()
                && has_data.is_empty()
                && has_book_data.is_empty(),
            "data is still loaded: save between batches, after `clear_data`"
        );
        for (queue, empty) in [
            ("the trading command queue", trading_cmd_queue_is_empty()),
            ("the data command queue", data_cmd_queue_is_empty()),
            ("the callback queue", !actor::callbacks_pending()),
            (
                "an execution client's event buffer",
                exec_clients
                    .iter()
                    .all(|client| !client.has_queued_events()),
            ),
        ] {
            anyhow::ensure!(empty, "{queue} is not empty: not at a batch boundary");
        }
        anyhow::ensure!(
            !kernel.is_event_store_replay() && !kernel.is_event_store_replay_configured(),
            "an event-store replay cannot be saved"
        );
        kernel.order_emulator.check_idle()?;

        // The expiration alerts setup armed are armed again by the resume's setup: only those
        // armed since are saved
        let rebuilt = RebuiltTimers {
            prefix: EXPIRATION_PREFIX,
            armed_since: expiration_alerts_armed,
            cancelled_since: expiration_alerts_cancelled,
        };
        let mut clocks = vec![(KERNEL_CLOCK.to_string(), {
            let mut clock = kernel.clock.borrow_mut();
            virtual_clock(&mut *clock)?
                .export_state(Some(rebuilt), &keep_stale)
                .map_err(|e| anyhow::anyhow!("the kernel clock: {e:#}"))?
        })];
        for (component_id, clock) in kernel.trader.borrow().component_clocks() {
            let mut clock = clock.borrow_mut();
            let state = virtual_clock(&mut *clock)?
                .export_state(None, &|_| true)
                .map_err(|e| anyhow::anyhow!("component {component_id}'s clock: {e:#}"))?;
            clocks.push((component_id.to_string(), state));
        }
        let mut alerts_armed: Vec<Ustr> = expiration_alerts_armed.iter().copied().collect();
        alerts_armed.sort_unstable();
        let mut alerts_cancelled: Vec<Ustr> = expiration_alerts_cancelled.iter().copied().collect();
        alerts_cancelled.sort_unstable();

        let mut has_book_processed: Vec<InstrumentId> =
            has_book_processed.iter().copied().collect();
        has_book_processed.sort_unstable();

        let venues = venues
            .iter()
            .map(|(venue, exchange)| Ok((*venue, exchange.borrow().export_state()?)))
            .collect::<anyhow::Result<Vec<_>>>()?;

        Ok(EngineState {
            format: ENGINE_STATE_FORMAT,
            iteration: *iteration,
            last_ns: *last_ns,
            last_module_ns: *last_module_ns,
            last_liquidation_ns: *last_liquidation_ns,
            end_ns: *end_ns,
            run_config_id: run_config_id.clone(),
            run_id: *run_id,
            run_started: *run_started,
            backtest_start: *backtest_start,
            kernel_started: kernel.ts_started,
            has_book_processed,
            accumulator_next_sequence: accumulator.export_state()?,
            expiration_alerts_armed: alerts_armed,
            expiration_alerts_cancelled: alerts_cancelled,
            clocks,
            cache: kernel.cache.borrow().export_state()?,
            portfolio: kernel.portfolio.borrow().export_state(),
            data_engine: kernel.data_engine.borrow().export_state()?,
            risk_engine: kernel.risk_engine.borrow().export_state()?,
            exec_engine: kernel.exec_engine.borrow().export_state(),
            trader: kernel.trader.borrow().export_resume_state()?,
            venues,
        })
    }

    /// Overwrites the engine's state with `state` and starts it without its first-run setup, so
    /// the host's next `run(start = the boundary, ..)` continues the saved replay.
    ///
    /// The engine must be built by the same setup as the saving one, and must never have run:
    /// the same venues with the same configs and models, the same instruments added in the same
    /// order, the same actors and strategies added. No account is initialized, no expiration
    /// alert armed for every instrument and no `on_start` called: the saved run did all three.
    ///
    /// `callbacks` gives the callback of each timer that fires through a named callback and is
    /// not the engine's own (an instrument expiration, funding settlement or event release
    /// alert, which the engine re-attaches). A name it does not resolve gets the callback the
    /// setup registered under it, if any.
    ///
    /// # Errors
    ///
    /// Returns an error if `state` has another format, the engine is not a fresh one built by the
    /// same setup (the venues, instruments, actors, strategies or clocks differ), a named
    /// callback cannot be resolved, or a component's state cannot be restored. An engine an
    /// import failed on is part restored: dispose of it and replay from the start instead.
    pub fn import_state(
        &mut self,
        state: EngineState,
        callbacks: &dyn Fn(&str) -> Option<TimeEventCallback>,
    ) -> anyhow::Result<()> {
        let EngineState {
            format,
            iteration,
            last_ns,
            last_module_ns,
            last_liquidation_ns,
            end_ns,
            run_config_id,
            run_id,
            run_started,
            backtest_start,
            kernel_started,
            has_book_processed,
            accumulator_next_sequence,
            expiration_alerts_armed,
            expiration_alerts_cancelled,
            clocks,
            cache,
            portfolio,
            data_engine,
            risk_engine,
            exec_engine,
            trader,
            venues,
        } = state;
        anyhow::ensure!(
            format == ENGINE_STATE_FORMAT,
            "saved engine state format {format}, this engine reads {ENGINE_STATE_FORMAT}"
        );
        anyhow::ensure!(
            self.iteration == 0 && self.run_started.is_none(),
            "the engine has run: a saved state is imported into a freshly built engine"
        );
        let saved_venues: Vec<Venue> = venues.iter().map(|(venue, _)| *venue).collect();
        let built_venues: Vec<Venue> = self.venues.keys().copied().collect();
        anyhow::ensure!(
            saved_venues == built_venues,
            "the saved venues {saved_venues:?} are not the engine's {built_venues:?}"
        );

        // Start as the first run would, without what the saved run already did
        Self::init_command_senders();
        logging_clock_set_static_mode();
        logging_clock_set_static_time(last_ns.as_u64());
        self.force_stop = false;
        self.kernel.reset_shutdown_flag();
        self.kernel.start_restored()?;

        // Clocks and timers, each named callback re-attached
        let component_clocks = self.kernel.trader.borrow().component_clocks();
        let mut built_clocks = vec![(KERNEL_CLOCK.to_string(), Rc::clone(&self.kernel.clock))];
        built_clocks.extend(
            component_clocks
                .into_iter()
                .map(|(component_id, clock)| (component_id.to_string(), clock)),
        );
        let saved_names: Vec<&str> = clocks.iter().map(|(name, _)| name.as_str()).collect();
        let built_names: Vec<&str> = built_clocks.iter().map(|(name, _)| name.as_str()).collect();
        anyhow::ensure!(
            saved_names == built_names,
            "the saved clocks {saved_names:?} are not the engine's {built_names:?}"
        );
        let armed: AHashSet<Ustr> = expiration_alerts_armed.into_iter().collect();
        let cancelled: AHashSet<Ustr> = expiration_alerts_cancelled.into_iter().collect();
        for ((name, clock_state), (_, clock)) in clocks.into_iter().zip(built_clocks) {
            let kernel = name == KERNEL_CLOCK;
            // Setup armed every expiration alert again: those the saved run still held are kept
            let rebuilt = kernel.then_some(RebuiltTimers {
                prefix: EXPIRATION_PREFIX,
                armed_since: &armed,
                cancelled_since: &cancelled,
            });
            let mut resolve = |timer: Ustr, before: Option<TimeEventCallback>| {
                self.resolve_callback(kernel, timer, before, callbacks)
            };
            let mut clock = clock.borrow_mut();
            virtual_clock(&mut *clock)?
                .import_state(clock_state, rebuilt, &mut resolve)
                .map_err(|e| anyhow::anyhow!("clock {name}: {e:#}"))?;
        }
        self.expiration_alerts_armed = armed;
        self.expiration_alerts_cancelled = cancelled;

        // The components' subscriptions issued again, as the first run's start would have
        self.kernel
            .trader
            .borrow_mut()
            .import_resume_state(trader)?;
        loop {
            drain_data_cmd_queue();
            let callbacks_pending = actor::drain_callbacks(CALLBACK_DRAIN_BUDGET)?;
            if data_cmd_queue_is_empty() && !callbacks_pending {
                break;
            }
        }

        // Then the data, over whatever the subscriptions built
        self.kernel.cache.borrow_mut().import_state(cache)?;
        for exchange in self.venues.values() {
            exchange.borrow().restore_account_models()?;
        }
        self.kernel.portfolio.borrow().import_state(portfolio);
        self.kernel
            .data_engine
            .borrow_mut()
            .import_state(data_engine);
        self.kernel
            .risk_engine
            .borrow_mut()
            .import_state(risk_engine)?;
        ExecutionEngine::import_state(&self.kernel.exec_engine, exec_engine);
        for (venue, exchange_state) in venues {
            self.venues[&venue]
                .borrow_mut()
                .import_state(exchange_state)
                .map_err(|e| anyhow::anyhow!("venue {venue}: {e:#}"))?;
        }

        self.iteration = iteration;
        self.last_ns = last_ns;
        self.last_module_ns = last_module_ns;
        self.last_liquidation_ns = last_liquidation_ns;
        self.end_ns = end_ns;
        self.run_config_id = run_config_id;
        self.run_id = run_id;
        self.run_started = run_started;
        self.backtest_start = backtest_start;
        self.kernel.ts_started = kernel_started;
        self.has_book_processed = has_book_processed.into_iter().collect();
        self.accumulator
            .restore_next_sequence(accumulator_next_sequence)?;
        Ok(())
    }

    /// The callback for the timer `name` of a restored clock: the engine's own for its alerts
    /// on the kernel clock, else the host's, else the one setup registered (`before`).
    fn resolve_callback(
        &self,
        kernel: bool,
        name: Ustr,
        before: Option<TimeEventCallback>,
        callbacks: &dyn Fn(&str) -> Option<TimeEventCallback>,
    ) -> anyhow::Result<TimeEventCallback> {
        if kernel {
            if let Some(rest) = name.as_str().strip_prefix(EXPIRATION_PREFIX) {
                let venue = rest
                    .rsplit_once(':')
                    .map(|(venue, _)| Venue::from(venue))
                    .ok_or_else(|| anyhow::anyhow!("malformed expiration alert {name}"))?;
                let exchange = self
                    .venues
                    .get(&venue)
                    .ok_or_else(|| anyhow::anyhow!("expiration alert {name}: no venue {venue}"))?;
                return Ok(Self::instrument_expiration_callback(exchange));
            }
            if name.as_str().starts_with(FUNDING_PREFIX)
                || name.as_str().starts_with(EVENT_RELEASE_PREFIX)
            {
                let noop: Rc<dyn Fn(TimeEvent)> = Rc::new(|_event: TimeEvent| {});
                return Ok(TimeEventCallback::from(noop));
            }
            if let Some(callback) = self.kernel.portfolio.borrow().timer_callback(name.as_str()) {
                return Ok(callback);
            }
        }
        callbacks(name.as_str())
            .or(before)
            .ok_or_else(|| anyhow::anyhow!("no callback for timer {name}"))
    }
}
