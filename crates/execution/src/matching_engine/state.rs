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

//! An [`OrderMatchingEngine`]'s state as plain data, for saving a backtest at a batch boundary and
//! resuming it in another process.
//!
//! The engine's wiring (venue, configuration, clock, cache, fill, fee and event handling) is
//! rebuilt by the venue's setup; everything the replay changed is saved here. An engine still as
//! its setup built it is [`OrderMatchingEngine::is_pristine`] and needs no state at all.

use std::{collections::VecDeque, rc::Rc};

use indexmap::{IndexMap, IndexSet};
use jiff::SignedDuration;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType, InstrumentClose, order::OrderId},
    enums::{MarketStatus, OrderSide},
    events::OrderUpdated,
    identifiers::{AccountId, ClientOrderId, InstrumentId, PositionId, TradeId, TraderId},
    instruments::{Instrument, InstrumentAny},
    orderbook::{OrderBook, OrderBookState},
    types::{Price, Quantity, price::PriceRaw, quantity::QuantityRaw},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::{
    OrderMatchingEngine, PendingFill, QueueCut,
    ids_generator::{IdsGenerator, IdsGeneratorState},
};
use crate::matching_core::OrderMatchingCoreState;

/// A fill applied to the cache whose position effect is still pending.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingFillState {
    /// The fill's trade ID.
    pub trade_id: TradeId,
    /// The filled order.
    pub client_order_id: ClientOrderId,
    /// The position the fill applies to.
    pub position_id: Option<PositionId>,
    /// The trade that opened the position, when the fill opens one.
    pub opening_trade_id: Option<TradeId>,
    /// The position quantity change.
    pub quantity_change: Decimal,
}

/// A level reduction recorded for the trade reflection window.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueCutState {
    /// The delta's venue stamp.
    pub ts_event: UnixNanos,
    /// The size no trade has claimed yet.
    pub remaining: QuantityRaw,
    /// The queue ahead each order lost.
    pub cuts: Vec<(ClientOrderId, QuantityRaw)>,
}

/// An [`OrderMatchingEngine`]'s complete replay state.
///
/// Every ordered collection keeps its order, so a resumed engine iterates exactly as the saved
/// one would have.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MatchingEngineState {
    /// The engine's instrument.
    pub instrument: InstrumentAny,
    /// The instrument's raw ID for the venue.
    pub raw_id: u32,
    /// The market status.
    pub market_status: MarketStatus,
    /// The matching core: prices and resting orders.
    pub core: OrderMatchingCoreState,
    /// The venue's book.
    pub book: OrderBookState,
    /// The pending bid override.
    pub target_bid: Option<Price>,
    /// The pending ask override.
    pub target_ask: Option<Price>,
    /// The pending last override.
    pub target_last: Option<Price>,
    /// The last bid bar.
    pub last_bar_bid: Option<Bar>,
    /// The last ask bar.
    pub last_bar_ask: Option<Bar>,
    /// Whether fills happen at the market.
    pub fill_at_market: bool,
    /// The bar type executed against, per instrument.
    pub execution_bar_types: IndexMap<InstrumentId, BarType>,
    /// Each execution bar type's interval, as (seconds, nanoseconds).
    pub execution_bar_deltas: IndexMap<BarType, (i64, i32)>,
    /// The account each trader's orders are booked to.
    pub account_ids: IndexMap<TraderId, AccountId>,
    /// The filled quantity cached per order.
    pub cached_filled_qty: IndexMap<ClientOrderId, Quantity>,
    /// The updates pending per order.
    pub pending_updates: IndexMap<ClientOrderId, Vec<OrderUpdated>>,
    /// The fills whose position effect is pending, in order.
    pub pending_fills: Vec<PendingFillState>,
    /// The orders to revisit after matching.
    pub post_match_order_ids: IndexSet<ClientOrderId>,
    /// The rejected OTO parents.
    pub rejected_oto_parent_ids: IndexSet<ClientOrderId>,
    /// The OTO children pending their parent.
    pub pending_oto_order_ids: IndexSet<ClientOrderId>,
    /// The OTO children on another instrument.
    pub other_instrument_oto_order_ids: IndexSet<ClientOrderId>,
    /// The canceled OTO children.
    pub canceled_oto_order_ids: IndexSet<ClientOrderId>,
    /// The venue identifier counts.
    pub ids_generator: IdsGeneratorState,
    /// The last trade's size.
    pub last_trade_size: Option<Quantity>,
    /// The L1 trade size.
    pub l1_trade_size: Option<QuantityRaw>,
    /// The liquidity consumed from the last trade.
    pub trade_consumption: QuantityRaw,
    /// The consumed liquidity per bid level, `(original size, consumed)`.
    pub bid_consumption: IndexMap<PriceRaw, (QuantityRaw, QuantityRaw)>,
    /// The consumed liquidity per ask level, `(original size, consumed)`.
    pub ask_consumption: IndexMap<PriceRaw, (QuantityRaw, QuantityRaw)>,
    /// The orders waiting for their queue position, by price.
    pub queue_pending: IndexMap<ClientOrderId, PriceRaw>,
    /// The book orders ahead of each tracked order.
    pub queue_ahead_orders: IndexMap<ClientOrderId, IndexMap<OrderId, QuantityRaw>>,
    /// The total queue ahead of each tracked order, by price.
    pub queue_ahead_total: IndexMap<ClientOrderId, (PriceRaw, QuantityRaw)>,
    /// Whether a queue snapshot is being taken.
    pub queue_snapshot_in_progress: bool,
    /// The tracked orders at each price.
    pub queue_ids_by_price: IndexMap<PriceRaw, IndexSet<ClientOrderId>>,
    /// The excess queue per order.
    pub queue_excess: IndexMap<ClientOrderId, QuantityRaw>,
    /// The level reductions recorded per side and price.
    pub queue_cuts: IndexMap<(OrderSide, PriceRaw), Vec<QueueCutState>>,
    /// The trades not yet reflected in the book per side and price.
    pub unreflected_trades: IndexMap<(OrderSide, PriceRaw), Vec<(UnixNanos, QuantityRaw)>>,
    /// The previous best bid's raw price.
    pub prev_bid_price_raw: PriceRaw,
    /// The previous best ask's raw price.
    pub prev_ask_price_raw: PriceRaw,
    /// Whether the top of book is initialized.
    pub tob_initialized: bool,
    /// The last quote's bid.
    pub last_quote_bid: Option<Price>,
    /// The last quote's ask.
    pub last_quote_ask: Option<Price>,
    /// The run of precision mismatches.
    pub precision_mismatch_streak: u32,
    /// The instrument close held.
    pub instrument_close: Option<InstrumentClose>,
    /// Whether a binary resolution is pending.
    pub pending_resolution: bool,
    /// Whether the expiration is processed.
    pub expiration_processed: bool,
    /// Whether option settlement failed.
    pub option_settlement_failed: bool,
    /// The option settlement warning last logged.
    pub option_settlement_warning: Option<String>,
    /// Whether the option expiration canceled the orders.
    pub option_expiration_orders_canceled: bool,
}

impl OrderMatchingEngine {
    /// Returns whether the engine is exactly as [`OrderMatchingEngine::new`] built it (with the
    /// same raw ID): nothing the replay can change has changed, so a resume that rebuilds it the
    /// same way needs no [`MatchingEngineState`] for it.
    ///
    /// Constant time apart from the empty-collection checks; an engine whose instrument was
    /// replaced through [`OrderMatchingEngine::update_instrument`] is not told apart, so the
    /// instrument is saved whenever anything else is.
    #[must_use]
    pub fn is_pristine(&self) -> bool {
        let Self {
            venue: _,        // Rebuilt by setup
            instrument: _,   // Rebuilt by setup (see above)
            raw_id: _,       // Compared by the caller against the setup's
            book_type: _,    // Rebuilt by setup
            oms_type: _,     // Rebuilt by setup
            account_type: _, // Rebuilt by setup
            market_status,
            config: _, // Rebuilt by setup
            core,
            clock: _, // Rebuilt by setup
            cache: _, // Rebuilt by setup
            book,
            fill_model: _,      // Rebuilt by setup
            fee_model: _,       // Rebuilt by setup
            event_handler: _,   // Rebuilt by setup
            inflight_orders: _, // Shared with the venue, saved there
            target_bid,
            target_ask,
            target_last,
            last_bar_bid,
            last_bar_ask,
            fill_at_market,
            execution_bar_types,
            execution_bar_deltas,
            account_ids,
            cached_filled_qty,
            pending_updates,
            pending_fills,
            post_match_order_ids,
            rejected_oto_parent_ids,
            pending_oto_order_ids,
            other_instrument_oto_order_ids,
            canceled_oto_order_ids,
            ids_generator,
            last_trade_size,
            l1_trade_size,
            trade_consumption,
            bid_consumption,
            ask_consumption,
            queue_pending,
            queue_ahead_orders,
            queue_ahead_total,
            queue_snapshot_in_progress,
            queue_ids_by_price,
            queue_excess,
            queue_id_scratch: _,      // Scratch: cleared before each use
            queue_pending_scratch: _, // Scratch: cleared before each use
            queue_stale_scratch: _,   // Scratch: cleared before each use
            queue_entry_scratch: _,   // Scratch: cleared before each use
            queue_cuts,
            unreflected_trades,
            prev_bid_price_raw,
            prev_ask_price_raw,
            tob_initialized,
            last_quote_bid,
            last_quote_ask,
            precision_mismatch_streak,
            instrument_close,
            pending_resolution,
            expiration_processed,
            option_settlement_failed,
            option_settlement_warning,
            option_expiration_orders_canceled,
        } = self;
        *market_status == MarketStatus::Open
            && core.bid.is_none()
            && core.ask.is_none()
            && core.last.is_none()
            && core.iter_orders().next().is_none()
            && book.sequence == 0
            && book.ts_last == UnixNanos::default()
            && book.update_count == 0
            && book.bids(None).next().is_none()
            && book.asks(None).next().is_none()
            && target_bid.is_none()
            && target_ask.is_none()
            && target_last.is_none()
            && last_bar_bid.is_none()
            && last_bar_ask.is_none()
            && *fill_at_market
            && execution_bar_types.is_empty()
            && execution_bar_deltas.is_empty()
            && account_ids.is_empty()
            && cached_filled_qty.is_empty()
            && pending_updates.borrow().is_empty()
            && pending_fills.is_empty()
            && post_match_order_ids.is_empty()
            && rejected_oto_parent_ids.borrow().is_empty()
            && pending_oto_order_ids.is_empty()
            && other_instrument_oto_order_ids.is_empty()
            && canceled_oto_order_ids.is_empty()
            && ids_generator.export_state() == IdsGeneratorState::default()
            && last_trade_size.is_none()
            && l1_trade_size.is_none()
            && *trade_consumption == 0
            && bid_consumption.is_empty()
            && ask_consumption.is_empty()
            && queue_pending.is_empty()
            && queue_ahead_orders.is_empty()
            && queue_ahead_total.is_empty()
            && !*queue_snapshot_in_progress
            && queue_ids_by_price.is_empty()
            && queue_excess.is_empty()
            && queue_cuts.is_empty()
            && unreflected_trades.is_empty()
            && *prev_bid_price_raw == 0
            && *prev_ask_price_raw == 0
            && !*tob_initialized
            && last_quote_bid.is_none()
            && last_quote_ask.is_none()
            && *precision_mismatch_streak == 0
            && instrument_close.is_none()
            && !*pending_resolution
            && !*expiration_processed
            && !*option_settlement_failed
            && option_settlement_warning.is_none()
            && !*option_expiration_orders_canceled
    }

    /// Returns the engine's complete replay state.
    #[must_use]
    pub fn export_state(&self) -> MatchingEngineState {
        let Self {
            venue: _, // Rebuilt by setup: the venue
            instrument,
            raw_id,
            book_type: _,    // Rebuilt by setup: the venue's config
            oms_type: _,     // Rebuilt by setup: the venue's config
            account_type: _, // Rebuilt by setup: the venue's config
            market_status,
            config: _, // Rebuilt by setup: the venue's config
            core,
            clock: _, // Rebuilt by setup: the kernel's clock
            cache: _, // Rebuilt by setup: the kernel's cache
            book,
            fill_model: _,      // Rebuilt by setup: the venue's fill model
            fee_model: _,       // Rebuilt by setup: the venue's fee model
            event_handler: _,   // Rebuilt by setup: the venue's event wiring
            inflight_orders: _, // Shared with the venue, saved there
            target_bid,
            target_ask,
            target_last,
            last_bar_bid,
            last_bar_ask,
            fill_at_market,
            execution_bar_types,
            execution_bar_deltas,
            account_ids,
            cached_filled_qty,
            pending_updates,
            pending_fills,
            post_match_order_ids,
            rejected_oto_parent_ids,
            pending_oto_order_ids,
            other_instrument_oto_order_ids,
            canceled_oto_order_ids,
            ids_generator,
            last_trade_size,
            l1_trade_size,
            trade_consumption,
            bid_consumption,
            ask_consumption,
            queue_pending,
            queue_ahead_orders,
            queue_ahead_total,
            queue_snapshot_in_progress,
            queue_ids_by_price,
            queue_excess,
            queue_id_scratch: _,      // Scratch: cleared before each use
            queue_pending_scratch: _, // Scratch: cleared before each use
            queue_stale_scratch: _,   // Scratch: cleared before each use
            queue_entry_scratch: _,   // Scratch: cleared before each use
            queue_cuts,
            unreflected_trades,
            prev_bid_price_raw,
            prev_ask_price_raw,
            tob_initialized,
            last_quote_bid,
            last_quote_ask,
            precision_mismatch_streak,
            instrument_close,
            pending_resolution,
            expiration_processed,
            option_settlement_failed,
            option_settlement_warning,
            option_expiration_orders_canceled,
        } = self;
        MatchingEngineState {
            instrument: instrument.clone(),
            raw_id: *raw_id,
            market_status: *market_status,
            core: core.export_state(),
            book: book.export_state(),
            target_bid: *target_bid,
            target_ask: *target_ask,
            target_last: *target_last,
            last_bar_bid: *last_bar_bid,
            last_bar_ask: *last_bar_ask,
            fill_at_market: *fill_at_market,
            execution_bar_types: execution_bar_types.clone(),
            execution_bar_deltas: execution_bar_deltas
                .iter()
                .map(|(bar_type, delta)| (*bar_type, (delta.as_secs(), delta.subsec_nanos())))
                .collect(),
            account_ids: account_ids.clone(),
            cached_filled_qty: cached_filled_qty.clone(),
            pending_updates: pending_updates.borrow().clone(),
            pending_fills: pending_fills
                .iter()
                .map(|(trade_id, fill)| {
                    let PendingFill {
                        client_order_id,
                        position_id,
                        opening_trade_id,
                        quantity_change,
                    } = fill;
                    PendingFillState {
                        trade_id: *trade_id,
                        client_order_id: *client_order_id,
                        position_id: *position_id,
                        opening_trade_id: *opening_trade_id,
                        quantity_change: *quantity_change,
                    }
                })
                .collect(),
            post_match_order_ids: post_match_order_ids.clone(),
            rejected_oto_parent_ids: rejected_oto_parent_ids.borrow().clone(),
            pending_oto_order_ids: pending_oto_order_ids.clone(),
            other_instrument_oto_order_ids: other_instrument_oto_order_ids.clone(),
            canceled_oto_order_ids: canceled_oto_order_ids.clone(),
            ids_generator: ids_generator.export_state(),
            last_trade_size: *last_trade_size,
            l1_trade_size: *l1_trade_size,
            trade_consumption: *trade_consumption,
            bid_consumption: bid_consumption.clone(),
            ask_consumption: ask_consumption.clone(),
            queue_pending: queue_pending.clone(),
            queue_ahead_orders: queue_ahead_orders.clone(),
            queue_ahead_total: queue_ahead_total.clone(),
            queue_snapshot_in_progress: *queue_snapshot_in_progress,
            queue_ids_by_price: queue_ids_by_price.clone(),
            queue_excess: queue_excess.clone(),
            queue_cuts: queue_cuts
                .iter()
                .map(|(key, cuts)| {
                    let cuts = cuts
                        .iter()
                        .map(|cut| {
                            let QueueCut {
                                ts_event,
                                remaining,
                                cuts,
                            } = cut;
                            QueueCutState {
                                ts_event: *ts_event,
                                remaining: *remaining,
                                cuts: cuts.clone(),
                            }
                        })
                        .collect();
                    (*key, cuts)
                })
                .collect(),
            unreflected_trades: unreflected_trades
                .iter()
                .map(|(key, trades)| (*key, trades.iter().copied().collect()))
                .collect(),
            prev_bid_price_raw: *prev_bid_price_raw,
            prev_ask_price_raw: *prev_ask_price_raw,
            tob_initialized: *tob_initialized,
            last_quote_bid: *last_quote_bid,
            last_quote_ask: *last_quote_ask,
            precision_mismatch_streak: *precision_mismatch_streak,
            instrument_close: *instrument_close,
            pending_resolution: *pending_resolution,
            expiration_processed: *expiration_processed,
            option_settlement_failed: *option_settlement_failed,
            option_settlement_warning: option_settlement_warning.map(str::to_owned),
            option_expiration_orders_canceled: *option_expiration_orders_canceled,
        }
    }

    /// Overwrites the engine's replay state with `state`. The engine must be one the venue's
    /// setup built for the same instrument (its wiring is kept).
    ///
    /// # Errors
    ///
    /// Returns an error if `state` is for another instrument, or its book or resting orders are
    /// inconsistent.
    pub fn import_state(&mut self, state: MatchingEngineState) -> anyhow::Result<()> {
        let MatchingEngineState {
            instrument,
            raw_id,
            market_status,
            core,
            book,
            target_bid,
            target_ask,
            target_last,
            last_bar_bid,
            last_bar_ask,
            fill_at_market,
            execution_bar_types,
            execution_bar_deltas,
            account_ids,
            cached_filled_qty,
            pending_updates,
            pending_fills,
            post_match_order_ids,
            rejected_oto_parent_ids,
            pending_oto_order_ids,
            other_instrument_oto_order_ids,
            canceled_oto_order_ids,
            ids_generator,
            last_trade_size,
            l1_trade_size,
            trade_consumption,
            bid_consumption,
            ask_consumption,
            queue_pending,
            queue_ahead_orders,
            queue_ahead_total,
            queue_snapshot_in_progress,
            queue_ids_by_price,
            queue_excess,
            queue_cuts,
            unreflected_trades,
            prev_bid_price_raw,
            prev_ask_price_raw,
            tob_initialized,
            last_quote_bid,
            last_quote_ask,
            precision_mismatch_streak,
            instrument_close,
            pending_resolution,
            expiration_processed,
            option_settlement_failed,
            option_settlement_warning,
            option_expiration_orders_canceled,
        } = state;
        anyhow::ensure!(
            instrument.id() == self.instrument.id(),
            "saved matching engine for {} restored into the engine for {}",
            instrument.id(),
            self.instrument.id()
        );
        anyhow::ensure!(
            book.instrument_id == self.book.instrument_id && book.book_type == self.book.book_type,
            "saved book {} {:?} restored into the engine for {} {:?}",
            book.instrument_id,
            book.book_type,
            self.book.instrument_id,
            self.book.book_type
        );
        self.instrument = instrument;
        if raw_id != self.raw_id {
            // Registered again during the replay, under a later raw ID
            self.raw_id = raw_id;
            self.ids_generator = IdsGenerator::new(
                self.venue,
                self.oms_type,
                raw_id,
                self.config.use_random_ids,
                self.config.use_position_ids,
                Rc::clone(&self.cache),
            );
        }
        self.market_status = market_status;
        self.core.import_state(core)?;
        self.book = OrderBook::from_state(book)?;
        self.target_bid = target_bid;
        self.target_ask = target_ask;
        self.target_last = target_last;
        self.last_bar_bid = last_bar_bid;
        self.last_bar_ask = last_bar_ask;
        self.fill_at_market = fill_at_market;
        self.execution_bar_types = execution_bar_types;
        self.execution_bar_deltas = execution_bar_deltas
            .into_iter()
            .map(|(bar_type, (secs, nanos))| (bar_type, SignedDuration::new(secs, nanos)))
            .collect();
        self.account_ids = account_ids;
        self.cached_filled_qty = cached_filled_qty;
        *self.pending_updates.get_mut() = pending_updates;
        self.pending_fills = pending_fills
            .into_iter()
            .map(|fill| {
                let PendingFillState {
                    trade_id,
                    client_order_id,
                    position_id,
                    opening_trade_id,
                    quantity_change,
                } = fill;
                (
                    trade_id,
                    PendingFill {
                        client_order_id,
                        position_id,
                        opening_trade_id,
                        quantity_change,
                    },
                )
            })
            .collect();
        self.post_match_order_ids = post_match_order_ids;
        *self.rejected_oto_parent_ids.get_mut() = rejected_oto_parent_ids;
        self.pending_oto_order_ids = pending_oto_order_ids;
        self.other_instrument_oto_order_ids = other_instrument_oto_order_ids;
        self.canceled_oto_order_ids = canceled_oto_order_ids;
        self.ids_generator.import_state(ids_generator);
        self.last_trade_size = last_trade_size;
        self.l1_trade_size = l1_trade_size;
        self.trade_consumption = trade_consumption;
        self.bid_consumption = bid_consumption;
        self.ask_consumption = ask_consumption;
        self.queue_pending = queue_pending;
        self.queue_ahead_orders = queue_ahead_orders;
        self.queue_ahead_total = queue_ahead_total;
        self.queue_snapshot_in_progress = queue_snapshot_in_progress;
        self.queue_ids_by_price = queue_ids_by_price;
        self.queue_excess = queue_excess;
        self.queue_cuts = queue_cuts
            .into_iter()
            .map(|(key, cuts)| {
                let cuts: VecDeque<QueueCut> = cuts
                    .into_iter()
                    .map(|cut| {
                        let QueueCutState {
                            ts_event,
                            remaining,
                            cuts,
                        } = cut;
                        QueueCut {
                            ts_event,
                            remaining,
                            cuts,
                        }
                    })
                    .collect();
                (key, cuts)
            })
            .collect();
        self.unreflected_trades = unreflected_trades
            .into_iter()
            .map(|(key, trades)| (key, trades.into_iter().collect()))
            .collect();
        self.prev_bid_price_raw = prev_bid_price_raw;
        self.prev_ask_price_raw = prev_ask_price_raw;
        self.tob_initialized = tob_initialized;
        self.last_quote_bid = last_quote_bid;
        self.last_quote_ask = last_quote_ask;
        self.precision_mismatch_streak = precision_mismatch_streak;
        self.instrument_close = instrument_close;
        self.pending_resolution = pending_resolution;
        self.expiration_processed = expiration_processed;
        self.option_settlement_failed = option_settlement_failed;
        // The warning only keeps a repeated retry from logging twice; its few reasons are
        // literals, so leaking the restored one is bounded.
        self.option_settlement_warning =
            option_settlement_warning.map(|reason| &*Box::leak(reason.into_boxed_str()));
        self.option_expiration_orders_canceled = option_expiration_orders_canceled;
        Ok(())
    }
}
