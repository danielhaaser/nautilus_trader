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

//! A [`SimulatedExchange`]'s state as plain data, for saving a backtest at a batch boundary and
//! resuming it in another process.
//!
//! The venue's setup (its config, models and every instrument registered before the replay)
//! is rebuilt by the resuming host; the state names what the replay changed since. It scales
//! with that, not with the instruments registered: a matching engine still as setup built it is
//! not saved, and the expirations still pending from setup are described by the instant
//! expirations were processed through.

use std::collections::{BTreeMap, BTreeSet};

use nautilus_common::messages::execution::{
    BatchCancelOrders, BatchModifyOrders, CancelAllOrders, CancelOrder, ModifyOrder, QueryAccount,
    QueryOrder, SubmitOrder, SubmitOrderList, TradingCommand,
};
use nautilus_core::UnixNanos;
use nautilus_execution::matching_engine::{
    outbound::OutboundEventsState, state::MatchingEngineState,
};
use nautilus_model::{
    accounts::AccountAny,
    data::FundingRateUpdate,
    enums::MarketStatus,
    identifiers::{ClientOrderId, InstrumentId},
    instruments::{Instrument, InstrumentAny},
};
use serde::{Deserialize, Serialize};

use super::{ChangedSlots, InflightCommand, InflightQueue, MatchingEngines, SimulatedExchange};

/// What a venue's setup registered, recorded when the replay starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetupMark {
    /// The last raw ID setup assigned: an engine with a later one was registered in the replay.
    pub raw_id: u32,
    /// The engine slots setup made.
    pub slots: usize,
    /// The next registration sequence after setup.
    pub engine_seq: u64,
}

/// A trading command, tagged by kind (the command's own serde form is untagged).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[expect(
    missing_docs,
    reason = "each variant mirrors the trading command of the same name"
)]
#[expect(
    clippy::large_enum_variant,
    reason = "mirrors TradingCommand, which is not boxed either"
)]
pub enum TradingCommandState {
    SubmitOrder(SubmitOrder),
    SubmitOrderList(SubmitOrderList),
    ModifyOrder(ModifyOrder),
    ModifyOrders(BatchModifyOrders),
    CancelOrder(CancelOrder),
    CancelOrders(BatchCancelOrders),
    CancelAllOrders(CancelAllOrders),
    QueryOrder(QueryOrder),
    QueryAccount(QueryAccount),
}

impl From<&TradingCommand> for TradingCommandState {
    fn from(command: &TradingCommand) -> Self {
        match command.clone() {
            TradingCommand::SubmitOrder(c) => Self::SubmitOrder(c),
            TradingCommand::SubmitOrderList(c) => Self::SubmitOrderList(c),
            TradingCommand::ModifyOrder(c) => Self::ModifyOrder(c),
            TradingCommand::ModifyOrders(c) => Self::ModifyOrders(c),
            TradingCommand::CancelOrder(c) => Self::CancelOrder(c),
            TradingCommand::CancelOrders(c) => Self::CancelOrders(c),
            TradingCommand::CancelAllOrders(c) => Self::CancelAllOrders(c),
            TradingCommand::QueryOrder(c) => Self::QueryOrder(c),
            TradingCommand::QueryAccount(c) => Self::QueryAccount(c),
        }
    }
}

impl From<TradingCommandState> for TradingCommand {
    fn from(command: TradingCommandState) -> Self {
        match command {
            TradingCommandState::SubmitOrder(c) => Self::SubmitOrder(c),
            TradingCommandState::SubmitOrderList(c) => Self::SubmitOrderList(c),
            TradingCommandState::ModifyOrder(c) => Self::ModifyOrder(c),
            TradingCommandState::ModifyOrders(c) => Self::ModifyOrders(c),
            TradingCommandState::CancelOrder(c) => Self::CancelOrder(c),
            TradingCommandState::CancelOrders(c) => Self::CancelOrders(c),
            TradingCommandState::CancelAllOrders(c) => Self::CancelAllOrders(c),
            TradingCommandState::QueryOrder(c) => Self::QueryOrder(c),
            TradingCommandState::QueryAccount(c) => Self::QueryAccount(c),
        }
    }
}

/// A command the latency model holds on its way to the venue.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InflightCommandState {
    /// The arrival instant.
    pub timestamp: UnixNanos,
    /// The per-instant counter, the heap's tie-break.
    pub counter: u32,
    /// The command.
    pub command: TradingCommandState,
}

/// The matching engines beyond setup's.
///
/// The wake bookkeeping (which engines the next iterate visits) is not saved: it is an index
/// over engines whose iterate would be a no-op, so a resumed venue wakes every engine as setup
/// left them, and the first iterate puts the idle ones back to sleep exactly as the pin's walk
/// over every engine would have left them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MatchingEnginesState {
    /// The instruments of the slots registered in the replay, in slot order.
    pub added_slots: Vec<InstrumentId>,
    /// Each engine that differs from the one setup built (or was registered in the replay), by
    /// slot index, in slot order.
    pub engines: Vec<(usize, MatchingEngineState)>,
}

/// A [`SimulatedExchange`]'s replay state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SimulatedExchangeState {
    /// What setup registered.
    pub setup: SetupMark,
    /// The instruments registered in the replay (whose engine has a raw ID after setup's), in
    /// slot order.
    pub added_instruments: Vec<InstrumentAny>,
    /// The matching engines.
    pub matching_engines: MatchingEnginesState,
    /// The last raw ID assigned.
    pub last_raw_id: u32,
    /// The released engines' raw ID and market status, sorted by instrument.
    pub removed_engines: Vec<(InstrumentId, u32, MarketStatus)>,
    /// The engines settled and awaiting a removal sweep, in order.
    pub settled_engines: Vec<InstrumentId>,
    /// The registration sequence of each instrument registered in the replay, sorted.
    pub added_engine_seq: Vec<(InstrumentId, u64)>,
    /// The next registration sequence.
    pub next_engine_seq: u64,
    /// The latest instant expirations were processed through.
    pub expirations_processed_ns: Option<UnixNanos>,
    /// The pending expirations setup's registrations do not account for, sorted.
    pub added_expirations_pending: Vec<(UnixNanos, InstrumentId)>,
    /// The engines whose expiration is reached and not settled, by registration sequence.
    pub expirations_due: Vec<(u64, InstrumentId)>,
    /// The funding rates awaiting settlement, in order.
    pub pending_funding_rates: Vec<((UnixNanos, InstrumentId), FundingRateUpdate)>,
    /// The funding settlements due, in order.
    pub funding_settlements: Vec<(UnixNanos, InstrumentId)>,
    /// The commands queued for the next settlement, in order.
    pub message_queue: Vec<TradingCommandState>,
    /// The commands the latency model holds, in arrival order.
    pub inflight_queue: Vec<InflightCommandState>,
    /// The submitted orders awaiting venue receipt, sorted.
    pub inflight_orders: Vec<ClientOrderId>,
    /// The per-instant inflight counters, sorted by instant.
    pub inflight_counter: Vec<(UnixNanos, u32)>,
    /// The order events the latency model holds on their way back.
    pub outbound: OutboundEventsState,
}

impl MatchingEngines {
    /// Starts recording which slots change from here on.
    pub(crate) fn track_changes(&mut self) {
        self.changed.get_or_insert_with(ChangedSlots::default);
    }

    fn export_state(&self, setup: SetupMark) -> anyhow::Result<MatchingEnginesState> {
        let Self {
            slots,
            live: _,        // Derived: the slots holding an engine
            awake: _,       // Derived: see `MatchingEnginesState`
            awake_slots: _, // Derived: see `MatchingEnginesState`
            alarms: _,      // Derived: see `MatchingEnginesState`
            changed,
        } = self;
        let Some(changed) = changed else {
            anyhow::bail!("the matching engines are not recording changes since setup");
        };
        let added_slots = slots.keys().skip(setup.slots).copied().collect();
        let mut indices = changed.slots.clone();
        indices.sort_unstable();
        let engines = indices
            .into_iter()
            .filter_map(|index| {
                let (_, slot) = slots.get_index(index)?;
                let engine = slot.as_deref()?;
                (engine.raw_id > setup.raw_id || !engine.is_pristine())
                    .then(|| (index, engine.export_state()))
            })
            .collect();
        Ok(MatchingEnginesState {
            added_slots,
            engines,
        })
    }
}

impl SimulatedExchange {
    /// Records what the venue's setup registered. The backtest engine calls it when the replay
    /// starts; a later call keeps the first mark.
    pub fn mark_setup_complete(&mut self) {
        if self.setup.is_none() {
            self.setup = Some(SetupMark {
                raw_id: self.last_raw_id,
                slots: self.matching_engines.slots.len(),
                engine_seq: self.next_engine_seq,
            });
            self.matching_engines.track_changes();
        }
    }

    /// Returns the venue's replay state.
    ///
    /// # Errors
    ///
    /// Returns an error if the replay has not started, the venue runs simulation modules or has a
    /// module error, or an order event is being deferred.
    pub fn export_state(&self) -> anyhow::Result<SimulatedExchangeState> {
        let Self {
            id,
            oms_type: _,          // Rebuilt by setup: the venue's config
            account_type: _,      // Rebuilt by setup: the venue's config
            base_currency: _,     // Rebuilt by setup: the venue's config
            starting_balances: _, // Rebuilt by setup: the venue's config
            book_type: _,         // Rebuilt by setup: the venue's config
            default_leverage: _,  // Rebuilt by setup: the venue's config
            exec_client: _,       // Rebuilt by setup
            event_handler: _,     // Rebuilt by setup
            deferring_events,
            outbound,
            outbound_handler: _, // Rebuilt by setup: the latency model's wiring
            fee_model: _,        // Rebuilt by setup: the venue's config
            fill_model: _,       // Rebuilt by setup: the venue's config
            latency_model: _,    // Rebuilt by setup: the venue's config
            instruments,
            matching_engines,
            last_raw_id,
            removed_engines,
            settled_engines,
            engine_seq,
            next_engine_seq,
            expirations_pending,
            expirations_due,
            pending_funding_rates,
            funding_settlements,
            leverages: _,    // Rebuilt by setup: the venue's config
            margin_model: _, // Rebuilt by setup: the venue's config
            modules,
            module_error,
            clock: _, // Rebuilt by setup: the kernel's clock
            cache: _, // Rebuilt by setup: the kernel's cache
            message_queue,
            inflight_queue,
            inflight_orders,
            inflight_counter,
            bar_execution: _, // Rebuilt by setup: the venue's config
            bar_adaptive_high_low_ordering: _, // Rebuilt by setup: the venue's config
            trade_execution: _, // Rebuilt by setup: the venue's config
            liquidity_consumption: _, // Rebuilt by setup: the venue's config
            reject_stop_orders: _, // Rebuilt by setup: the venue's config
            support_gtd_orders: _, // Rebuilt by setup: the venue's config
            support_contingent_orders: _, // Rebuilt by setup: the venue's config
            use_position_ids: _, // Rebuilt by setup: the venue's config
            use_random_ids: _, // Rebuilt by setup: the venue's config
            use_reduce_only: _, // Rebuilt by setup: the venue's config
            use_message_queue: _, // Rebuilt by setup: the venue's config
            use_market_order_acks: _, // Rebuilt by setup: the venue's config
            allow_cash_borrowing: _, // Rebuilt by setup: the venue's config
            frozen_account: _, // Rebuilt by setup: the venue's config
            queue_position: _, // Rebuilt by setup: the venue's config
            book_cross_fills: _, // Rebuilt by setup: the venue's config
            oto_full_trigger: _, // Rebuilt by setup: the venue's config
            defer_option_settlement: _, // Rebuilt by setup: the venue's config
            price_protection_points: _, // Rebuilt by setup: the venue's config
            liquidation_enabled: _, // Rebuilt by setup: the venue's config
            liquidation_trigger_ratio: _, // Rebuilt by setup: the venue's config
            liquidation_cancel_open_orders: _, // Rebuilt by setup: the venue's config
            setup,
            expirations_processed_ns,
            expirations_added,
        } = self;
        let Some(setup) = *setup else {
            anyhow::bail!("venue {id}: the replay has not started, so there is nothing to save");
        };
        anyhow::ensure!(
            modules.is_empty(),
            "venue {id} runs simulation modules, which a saved backtest cannot carry"
        );
        anyhow::ensure!(
            module_error.is_none(),
            "venue {id} has a module error: {module_error:?}"
        );
        anyhow::ensure!(
            !deferring_events.get(),
            "venue {id} is deferring order events: not at a boundary"
        );

        // The instruments registered in the replay: their engine (live or released) has a raw ID
        // after setup's; a re-registration marks its slot changed
        let matching = matching_engines.export_state(setup)?;
        let mut added_ids: Vec<InstrumentId> = matching
            .engines
            .iter()
            .filter(|(_, engine)| engine.raw_id > setup.raw_id)
            .map(|(_, engine)| engine.instrument.id())
            .chain(
                removed_engines
                    .iter()
                    .filter(|(_, (raw_id, _))| *raw_id > setup.raw_id)
                    .map(|(instrument_id, _)| *instrument_id),
            )
            .collect();
        added_ids.sort_unstable_by_key(|instrument_id| {
            matching_engines.slots.get_index_of(instrument_id)
        });
        let added_instruments = added_ids
            .iter()
            .filter_map(|instrument_id| instruments.get(instrument_id).cloned())
            .collect();

        let mut removed: Vec<(InstrumentId, u32, MarketStatus)> = removed_engines
            .iter()
            .map(|(instrument_id, (raw_id, status))| (*instrument_id, *raw_id, *status))
            .collect();
        removed.sort_unstable_by_key(|(instrument_id, _, _)| *instrument_id);

        // New instruments take the sequence after setup's, in their slots after setup's
        let mut added_engine_seq: Vec<(InstrumentId, u64)> = matching
            .added_slots
            .iter()
            .filter_map(|instrument_id| {
                engine_seq
                    .get(instrument_id)
                    .map(|seq| (*instrument_id, *seq))
            })
            .collect();
        added_engine_seq.sort_unstable();

        // The pending expirations setup left after the processed instant are rebuilt by the
        // resume's setup; those added since and still pending are saved
        let added_expirations_pending: BTreeSet<(UnixNanos, InstrumentId)> = expirations_added
            .iter()
            .filter(|(expiration, instrument_id)| {
                expirations_pending
                    .get(expiration)
                    .is_some_and(|ids| ids.contains(instrument_id))
            })
            .copied()
            .collect();

        let InflightQueue {
            heap,
            by_instrument: _, // Derived: rebuilt as each command is pushed back
            zero_latency: _,  // Derived: rebuilt as each command is pushed back
        } = inflight_queue;
        let mut inflight: Vec<&InflightCommand> = heap.iter().collect();
        inflight.sort_unstable_by(|a, b| b.cmp(a));
        let mut inflight_counter: Vec<(UnixNanos, u32)> = inflight_counter
            .iter()
            .map(|(ts, counter)| (*ts, *counter))
            .collect();
        inflight_counter.sort_unstable();

        Ok(SimulatedExchangeState {
            setup,
            added_instruments,
            matching_engines: matching,
            last_raw_id: *last_raw_id,
            removed_engines: removed,
            settled_engines: settled_engines.clone(),
            added_engine_seq,
            next_engine_seq: *next_engine_seq,
            expirations_processed_ns: *expirations_processed_ns,
            added_expirations_pending: added_expirations_pending.into_iter().collect(),
            expirations_due: expirations_due
                .iter()
                .map(|(seq, instrument_id)| (*seq, *instrument_id))
                .collect(),
            pending_funding_rates: pending_funding_rates
                .iter()
                .map(|(key, rate)| (*key, *rate))
                .collect(),
            funding_settlements: funding_settlements.iter().copied().collect(),
            message_queue: message_queue
                .iter()
                .map(TradingCommandState::from)
                .collect(),
            inflight_queue: inflight
                .into_iter()
                .map(|inflight| InflightCommandState {
                    timestamp: inflight.timestamp,
                    counter: inflight.counter,
                    command: TradingCommandState::from(&inflight.command),
                })
                .collect(),
            inflight_orders: inflight_orders.export_state(),
            inflight_counter,
            outbound: outbound.export_state(),
        })
    }

    /// Overwrites the venue's replay state with `state`. The venue must be one the saving
    /// host's setup built, in the same way: the same config and models, and the same instruments
    /// registered in the same order, before any replay.
    ///
    /// # Errors
    ///
    /// Returns an error if the venue's setup differs from the saved one's, or an engine's state
    /// cannot be restored.
    pub fn import_state(&mut self, state: SimulatedExchangeState) -> anyhow::Result<()> {
        let SimulatedExchangeState {
            setup,
            added_instruments,
            matching_engines,
            last_raw_id,
            removed_engines,
            settled_engines,
            added_engine_seq,
            next_engine_seq,
            expirations_processed_ns,
            added_expirations_pending,
            expirations_due,
            pending_funding_rates,
            funding_settlements,
            message_queue,
            inflight_queue,
            inflight_orders,
            inflight_counter,
            outbound,
        } = state;
        let id = self.id;
        anyhow::ensure!(
            self.setup.is_none(),
            "venue {id} has already started a replay"
        );
        let built = SetupMark {
            raw_id: self.last_raw_id,
            slots: self.matching_engines.slots.len(),
            engine_seq: self.next_engine_seq,
        };
        anyhow::ensure!(
            built == setup,
            "venue {id}: setup registered {built:?}, the saved venue's setup {setup:?}"
        );
        anyhow::ensure!(
            self.modules.is_empty(),
            "venue {id} runs simulation modules, which a saved backtest cannot carry"
        );

        let MatchingEnginesState {
            added_slots,
            engines,
        } = matching_engines;
        self.matching_engines.track_changes();

        // The instruments registered in the replay, and their slots, in slot order
        for instrument in added_instruments {
            self.instruments.insert(instrument.id(), instrument);
        }
        for instrument_id in &added_slots {
            let instrument = self
                .instruments
                .get(instrument_id)
                .cloned()
                .ok_or_else(|| {
                    anyhow::anyhow!("venue {id}: slot {instrument_id} has no registered instrument")
                })?;
            let engine = self.build_matching_engine(instrument, 0);
            self.matching_engines.insert(*instrument_id, engine);
        }
        anyhow::ensure!(
            self.matching_engines.slots.len() == setup.slots + added_slots.len(),
            "venue {id}: an added slot repeats a registered instrument"
        );

        // The released engines, then the saved ones
        self.removed_engines.clear();
        for (instrument_id, raw_id, status) in removed_engines {
            if self.matching_engines.take(&instrument_id).is_none() {
                anyhow::ensure!(
                    self.matching_engines.slots.contains_key(&instrument_id),
                    "venue {id}: released engine {instrument_id} has no slot"
                );
            }
            self.removed_engines.insert(instrument_id, (raw_id, status));
        }
        for (index, engine_state) in engines {
            let Some((instrument_id, Some(engine))) =
                self.matching_engines.slots.get_index_mut(index)
            else {
                anyhow::bail!("venue {id}: saved engine slot {index} holds no engine");
            };
            engine
                .import_state(engine_state)
                .map_err(|e| anyhow::anyhow!("venue {id}: engine {instrument_id}: {e:#}"))?;
            if let Some(changed) = &mut self.matching_engines.changed {
                changed.mark(index);
            }
        }

        self.last_raw_id = last_raw_id;
        self.settled_engines = settled_engines;
        for (instrument_id, seq) in added_engine_seq {
            self.engine_seq.insert(instrument_id, seq);
        }
        self.next_engine_seq = next_engine_seq;

        // Setup's pending expirations through the processed instant are gone; the saved ones are
        // added back
        if let Some(processed) = expirations_processed_ns {
            let later = match processed.as_u64().checked_add(1) {
                Some(next) => self.expirations_pending.split_off(&UnixNanos::from(next)),
                None => BTreeMap::default(),
            };
            self.expirations_pending = later;
        }
        for (expiration, instrument_id) in added_expirations_pending {
            let pending = self.expirations_pending.entry(expiration).or_default();
            if !pending.contains(&instrument_id) {
                pending.push(instrument_id);
            }
            self.expirations_added.push((expiration, instrument_id));
        }
        self.expirations_processed_ns = expirations_processed_ns;
        self.expirations_due = expirations_due.into_iter().collect();
        self.pending_funding_rates = pending_funding_rates.into_iter().collect();
        self.funding_settlements = funding_settlements.into_iter().collect();

        self.message_queue = message_queue
            .into_iter()
            .map(TradingCommand::from)
            .collect();
        self.inflight_queue.clear();
        for InflightCommandState {
            timestamp,
            counter,
            command,
        } in inflight_queue
        {
            self.inflight_queue.push(InflightCommand::new(
                timestamp,
                counter,
                TradingCommand::from(command),
            ));
        }
        self.inflight_orders.import_state(inflight_orders);
        self.inflight_counter = inflight_counter.into_iter().collect();
        self.outbound.import_state(outbound);
        self.setup = Some(setup);
        Ok(())
    }
}

impl SimulatedExchange {
    /// Gives the venue's margin model back to its account, after the cache restored the account
    /// without it (an account's serde skips its margin model).
    ///
    /// # Errors
    ///
    /// Returns an error if the cache refuses the account.
    pub fn restore_account_models(&self) -> anyhow::Result<()> {
        let Some(model) = &self.margin_model else {
            return Ok(());
        };
        if let Some(AccountAny::Margin(mut account)) = self.get_account() {
            account.set_margin_model(model.clone());
            self.cache
                .borrow_mut()
                .update_account(&AccountAny::Margin(account))?;
        }
        Ok(())
    }
}
