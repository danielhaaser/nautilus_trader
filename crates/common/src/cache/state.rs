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

//! A [`Cache`]'s state as plain data, for saving a backtest at a batch boundary and resuming it in
//! another process.
//!
//! The saving engine has called [`Cache::track_instrument_changes`] after its setup, so the
//! instruments are saved as the changes since then: a resume rebuilds the setup's instruments and
//! applies them. Everything else the replay can change is saved whole, the index included, so a
//! resumed cache answers every query as the saved one did.

use std::hash::Hash;

use ahash::{AHashMap, AHashSet};
use bytes::Bytes;
use nautilus_core::SharedCell;
use nautilus_model::{
    accounts::AccountAny,
    data::{
        Bar, BarType, FundingRateUpdate, IndexPriceUpdate, InstrumentClose, InstrumentStatus,
        MarkPriceUpdate, QuoteTick, TradeTick,
    },
    enums::OmsType,
    identifiers::{
        AccountId, ClientId, ClientOrderId, ExecAlgorithmId, InstrumentId, PositionId, StrategyId,
        Venue, VenueOrderId,
    },
    instruments::{Instrument, InstrumentAny},
    orderbook::{OrderBook, OrderBookState},
    orders::{Order, OrderAny, OrderList},
    position::Position,
    types::{Currency, Money},
};
use serde::{Deserialize, Serialize};
use ustr::Ustr;

use super::{Cache, bounded::BoundedVecDeque, index::CacheIndex, position::PositionSnapshotFrame};

/// A cached account and its per-instrument locked balances, which the account's serde skips.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachedAccountState {
    /// The account.
    pub account: AccountAny,
    /// The locked balance per instrument and currency, sorted.
    pub balances_locked: Vec<((InstrumentId, Currency), Money)>,
}

/// One bounded history of a [`CacheState`]: its key, capacity and items, newest first.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HistoryState<K, T> {
    /// The history's key.
    pub key: K,
    /// The history's capacity.
    pub capacity: usize,
    /// The items, newest first.
    pub items: Vec<T>,
}

/// A position snapshot frame: the archived position and its bytes, if they were encoded.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PositionSnapshotState {
    /// The archived position.
    pub position: Position,
    /// The position's encoded bytes, if they were encoded.
    pub encoded: Option<Bytes>,
}

/// The cache's index, each map and set sorted by key.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[expect(
    missing_docs,
    reason = "each field mirrors the cache index's field of the same name"
)]
pub struct CacheIndexState {
    pub venue_accounts: Vec<(Venue, Vec<AccountId>)>,
    pub venue_orders: Vec<(Venue, Vec<ClientOrderId>)>,
    pub venue_positions: Vec<(Venue, Vec<PositionId>)>,
    pub venue_order_ids: Vec<(VenueOrderId, ClientOrderId)>,
    pub client_order_ids: Vec<(ClientOrderId, VenueOrderId)>,
    pub order_position: Vec<(ClientOrderId, PositionId)>,
    pub order_strategy: Vec<(ClientOrderId, StrategyId)>,
    pub order_client: Vec<(ClientOrderId, ClientId)>,
    pub position_strategy: Vec<(PositionId, StrategyId)>,
    pub position_oms: Vec<(PositionId, OmsType)>,
    pub position_orders: Vec<(PositionId, Vec<ClientOrderId>)>,
    pub instrument_orders: Vec<(InstrumentId, Vec<ClientOrderId>)>,
    pub instrument_positions: Vec<(InstrumentId, Vec<PositionId>)>,
    pub strategy_orders: Vec<(StrategyId, Vec<ClientOrderId>)>,
    pub strategy_positions: Vec<(StrategyId, Vec<PositionId>)>,
    pub account_orders: Vec<(AccountId, Vec<ClientOrderId>)>,
    pub account_positions: Vec<(AccountId, Vec<PositionId>)>,
    pub exec_algorithm_orders: Vec<(ExecAlgorithmId, Vec<ClientOrderId>)>,
    pub exec_spawn_orders: Vec<(ClientOrderId, Vec<ClientOrderId>)>,
    pub orders: Vec<ClientOrderId>,
    pub orders_active_local: Vec<ClientOrderId>,
    pub orders_open: Vec<ClientOrderId>,
    pub orders_closed: Vec<ClientOrderId>,
    pub orders_emulated: Vec<ClientOrderId>,
    pub orders_inflight: Vec<ClientOrderId>,
    pub orders_pending_cancel: Vec<ClientOrderId>,
    pub positions: Vec<PositionId>,
    pub positions_open: Vec<PositionId>,
    pub positions_closed: Vec<PositionId>,
    pub strategies: Vec<StrategyId>,
    pub exec_algorithms: Vec<ExecAlgorithmId>,
}

/// A [`Cache`]'s complete replay state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CacheState {
    /// The general key-value store, sorted by key.
    pub general: Vec<(String, Bytes)>,
    /// The currencies, sorted by code.
    pub currencies: Vec<Currency>,
    /// The instruments added, replaced or purged since setup, sorted, each with its instrument
    /// now (`None` once purged).
    pub instrument_changes: Vec<(InstrumentId, Option<InstrumentAny>)>,
    /// The instrument closes, sorted by instrument.
    pub instrument_closes: Vec<InstrumentClose>,
    /// The order books, sorted by instrument.
    pub books: Vec<OrderBookState>,
    /// The quote histories, sorted by instrument.
    pub quotes: Vec<HistoryState<InstrumentId, QuoteTick>>,
    /// The trade histories, sorted by instrument.
    pub trades: Vec<HistoryState<InstrumentId, TradeTick>>,
    /// The mark exchange rates, sorted by currency pair code.
    pub mark_xrates: Vec<((Currency, Currency), f64)>,
    /// The mark price histories, sorted by instrument.
    pub mark_prices: Vec<HistoryState<InstrumentId, MarkPriceUpdate>>,
    /// The index price histories, sorted by instrument.
    pub index_prices: Vec<HistoryState<InstrumentId, IndexPriceUpdate>>,
    /// The funding rate histories, sorted by instrument.
    pub funding_rates: Vec<HistoryState<InstrumentId, FundingRateUpdate>>,
    /// The instrument status histories, sorted by instrument.
    pub instrument_statuses: Vec<HistoryState<InstrumentId, InstrumentStatus>>,
    /// The bar histories, sorted by bar type.
    pub bars: Vec<HistoryState<BarType, Bar>>,
    /// The external order claims, sorted by instrument.
    pub external_order_claims: Vec<(InstrumentId, StrategyId)>,
    /// The account of each client, sorted by client.
    pub client_accounts: Vec<(ClientId, AccountId)>,
    /// The client routed to each venue, sorted by venue.
    pub client_routes: Vec<(Venue, ClientId)>,
    /// The default client.
    pub default_client_id: Option<ClientId>,
    /// The external clients, sorted.
    pub external_clients: Vec<ClientId>,
    /// The accounts, sorted by ID.
    pub accounts: Vec<CachedAccountState>,
    /// The orders, sorted by client order ID.
    pub orders: Vec<OrderAny>,
    /// The order lists, sorted by ID.
    pub order_lists: Vec<OrderList>,
    /// The positions, sorted by ID.
    pub positions: Vec<Position>,
    /// The position snapshot frames per position, sorted by position.
    pub position_snapshots: Vec<(PositionId, Vec<PositionSnapshotState>)>,
    /// The position snapshot revisions, sorted by position.
    pub position_snapshot_revisions: Vec<(PositionId, u64)>,
    /// The index.
    pub index: CacheIndexState,
}

fn sorted<K: Ord + Copy, V: Clone>(map: &AHashMap<K, V>) -> Vec<(K, V)> {
    let mut entries: Vec<(K, V)> = map.iter().map(|(k, v)| (*k, v.clone())).collect();
    entries.sort_unstable_by_key(|(k, _)| *k);
    entries
}

fn sorted_set<K: Ord + Copy>(set: &AHashSet<K>) -> Vec<K> {
    let mut keys: Vec<K> = set.iter().copied().collect();
    keys.sort_unstable();
    keys
}

fn sorted_sets<K: Ord + Copy, V: Ord + Copy>(map: &AHashMap<K, AHashSet<V>>) -> Vec<(K, Vec<V>)> {
    let mut entries: Vec<(K, Vec<V>)> = map.iter().map(|(k, v)| (*k, sorted_set(v))).collect();
    entries.sort_unstable_by_key(|(k, _)| *k);
    entries
}

fn histories<K: Ord + Copy, T: Clone>(
    map: &AHashMap<K, BoundedVecDeque<T>>,
) -> Vec<HistoryState<K, T>> {
    let mut histories: Vec<HistoryState<K, T>> = map
        .iter()
        .map(|(key, deque)| HistoryState {
            key: *key,
            capacity: deque.capacity(),
            items: deque.iter().cloned().collect(),
        })
        .collect();
    histories.sort_unstable_by_key(|history| history.key);
    histories
}

fn restore_histories<K: Eq + Hash, T>(
    histories: Vec<HistoryState<K, T>>,
) -> anyhow::Result<AHashMap<K, BoundedVecDeque<T>>> {
    histories
        .into_iter()
        .map(|history| {
            let HistoryState {
                key,
                capacity,
                items,
            } = history;
            Ok((key, BoundedVecDeque::from_items(items, capacity)?))
        })
        .collect()
}

fn map_of<K: Eq + Hash, V>(entries: Vec<(K, V)>) -> AHashMap<K, V> {
    entries.into_iter().collect()
}

fn map_of_sets<K: Eq + Hash, V: Eq + Hash>(entries: Vec<(K, Vec<V>)>) -> AHashMap<K, AHashSet<V>> {
    entries
        .into_iter()
        .map(|(k, v)| (k, v.into_iter().collect()))
        .collect()
}

fn set_of<K: Eq + Hash>(keys: Vec<K>) -> AHashSet<K> {
    keys.into_iter().collect()
}

fn balances_locked(account: &AccountAny) -> Vec<((InstrumentId, Currency), Money)> {
    let locked = match account {
        AccountAny::Cash(account) => &account.balances_locked,
        AccountAny::Betting(account) => &account.balances_locked,
        AccountAny::Wallet(account) => &account.balances_locked,
        AccountAny::Margin(_) => return Vec::new(),
    };
    let mut entries: Vec<((InstrumentId, Currency), Money)> =
        locked.iter().map(|(key, money)| (*key, *money)).collect();
    entries.sort_unstable_by(|(a, _), (b, _)| a.0.cmp(&b.0).then_with(|| a.1.code.cmp(&b.1.code)));
    entries
}

fn set_balances_locked(
    account: &mut AccountAny,
    entries: Vec<((InstrumentId, Currency), Money)>,
) -> anyhow::Result<()> {
    let locked = match account {
        AccountAny::Cash(account) => &mut account.balances_locked,
        AccountAny::Betting(account) => &mut account.balances_locked,
        AccountAny::Wallet(account) => &mut account.balances_locked,
        AccountAny::Margin(_) => {
            anyhow::ensure!(
                entries.is_empty(),
                "a margin account carries no per-instrument locked balances"
            );
            return Ok(());
        }
    };
    *locked = entries.into_iter().collect();
    Ok(())
}

impl CacheIndex {
    fn export_state(&self) -> CacheIndexState {
        let Self {
            venue_accounts,
            venue_orders,
            venue_positions,
            venue_order_ids,
            client_order_ids,
            order_position,
            order_strategy,
            order_client,
            position_strategy,
            position_oms,
            position_orders,
            instrument_orders,
            instrument_positions,
            strategy_orders,
            strategy_positions,
            account_orders,
            account_positions,
            exec_algorithm_orders,
            exec_spawn_orders,
            orders,
            orders_active_local,
            orders_open,
            orders_closed,
            orders_emulated,
            orders_inflight,
            orders_pending_cancel,
            positions,
            positions_open,
            positions_closed,
            strategies,
            exec_algorithms,
        } = self;
        CacheIndexState {
            venue_accounts: sorted_sets(venue_accounts),
            venue_orders: sorted_sets(venue_orders),
            venue_positions: sorted_sets(venue_positions),
            venue_order_ids: sorted(venue_order_ids),
            client_order_ids: sorted(client_order_ids),
            order_position: sorted(order_position),
            order_strategy: sorted(order_strategy),
            order_client: sorted(order_client),
            position_strategy: sorted(position_strategy),
            position_oms: sorted(position_oms),
            position_orders: sorted_sets(position_orders),
            instrument_orders: sorted_sets(instrument_orders),
            instrument_positions: sorted_sets(instrument_positions),
            strategy_orders: sorted_sets(strategy_orders),
            strategy_positions: sorted_sets(strategy_positions),
            account_orders: sorted_sets(account_orders),
            account_positions: sorted_sets(account_positions),
            exec_algorithm_orders: sorted_sets(exec_algorithm_orders),
            exec_spawn_orders: sorted_sets(exec_spawn_orders),
            orders: sorted_set(orders),
            orders_active_local: sorted_set(orders_active_local),
            orders_open: sorted_set(orders_open),
            orders_closed: sorted_set(orders_closed),
            orders_emulated: sorted_set(orders_emulated),
            orders_inflight: sorted_set(orders_inflight),
            orders_pending_cancel: sorted_set(orders_pending_cancel),
            positions: sorted_set(positions),
            positions_open: sorted_set(positions_open),
            positions_closed: sorted_set(positions_closed),
            strategies: sorted_set(strategies),
            exec_algorithms: sorted_set(exec_algorithms),
        }
    }

    fn from_state(state: CacheIndexState) -> Self {
        let CacheIndexState {
            venue_accounts,
            venue_orders,
            venue_positions,
            venue_order_ids,
            client_order_ids,
            order_position,
            order_strategy,
            order_client,
            position_strategy,
            position_oms,
            position_orders,
            instrument_orders,
            instrument_positions,
            strategy_orders,
            strategy_positions,
            account_orders,
            account_positions,
            exec_algorithm_orders,
            exec_spawn_orders,
            orders,
            orders_active_local,
            orders_open,
            orders_closed,
            orders_emulated,
            orders_inflight,
            orders_pending_cancel,
            positions,
            positions_open,
            positions_closed,
            strategies,
            exec_algorithms,
        } = state;
        Self {
            venue_accounts: map_of_sets(venue_accounts),
            venue_orders: map_of_sets(venue_orders),
            venue_positions: map_of_sets(venue_positions),
            venue_order_ids: map_of(venue_order_ids),
            client_order_ids: map_of(client_order_ids),
            order_position: map_of(order_position),
            order_strategy: map_of(order_strategy),
            order_client: map_of(order_client),
            position_strategy: map_of(position_strategy),
            position_oms: map_of(position_oms),
            position_orders: map_of_sets(position_orders),
            instrument_orders: map_of_sets(instrument_orders),
            instrument_positions: map_of_sets(instrument_positions),
            strategy_orders: map_of_sets(strategy_orders),
            strategy_positions: map_of_sets(strategy_positions),
            account_orders: map_of_sets(account_orders),
            account_positions: map_of_sets(account_positions),
            exec_algorithm_orders: map_of_sets(exec_algorithm_orders),
            exec_spawn_orders: map_of_sets(exec_spawn_orders),
            orders: set_of(orders),
            orders_active_local: set_of(orders_active_local),
            orders_open: set_of(orders_open),
            orders_closed: set_of(orders_closed),
            orders_emulated: set_of(orders_emulated),
            orders_inflight: set_of(orders_inflight),
            orders_pending_cancel: set_of(orders_pending_cancel),
            positions: set_of(positions),
            positions_open: set_of(positions_open),
            positions_closed: set_of(positions_closed),
            strategies: set_of(strategies),
            exec_algorithms: set_of(exec_algorithms),
        }
    }
}

impl Cache {
    /// Starts recording which instruments are added, replaced or purged, for
    /// [`Self::export_state`]. A backtest engine calls it once its setup is complete; a second
    /// call keeps what is recorded.
    pub fn track_instrument_changes(&mut self) {
        self.instrument_changes.get_or_insert_with(AHashSet::new);
    }

    /// Returns the cache's complete replay state.
    ///
    /// # Errors
    ///
    /// Returns an error if instrument changes are not being recorded, or the cache holds what a
    /// saved backtest cannot carry: synthetic instruments, own order books, greeks, option greeks
    /// or yield curves.
    pub fn export_state(&self) -> anyhow::Result<CacheState> {
        let Self {
            config: _, // Rebuilt by setup
            index,
            database: _, // Rebuilt by setup: the backing database, if any
            general,
            currencies,
            instruments,
            instrument_closes,
            synthetics,
            books,
            own_books,
            quotes,
            trades,
            mark_xrates,
            mark_prices,
            index_prices,
            funding_rates,
            instrument_statuses,
            bars,
            greeks,
            option_greeks,
            yield_curves,
            external_order_claims,
            client_accounts,
            client_routes,
            default_client_id,
            external_clients,
            accounts,
            orders,
            order_lists,
            positions,
            position_snapshots,
            position_snapshot_revisions,
            instrument_changes,
            #[cfg(feature = "defi")]
                defi: _, // Not carried: a backtest saved with DeFi data is not resumable
        } = self;
        let Some(instrument_changes) = instrument_changes else {
            anyhow::bail!("the cache is not recording instrument changes since setup");
        };
        for (name, empty) in [
            ("synthetic instruments", synthetics.is_empty()),
            ("own order books", own_books.is_empty()),
            ("greeks", greeks.is_empty()),
            ("option greeks", option_greeks.is_empty()),
            ("yield curves", yield_curves.is_empty()),
        ] {
            anyhow::ensure!(
                empty,
                "the cache holds {name}, which a saved backtest cannot carry"
            );
        }

        let mut general: Vec<(String, Bytes)> = general
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        general.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
        let mut currencies: Vec<Currency> = currencies.values().copied().collect();
        currencies.sort_unstable_by_key(|currency| currency.code);
        let mut changes: Vec<(InstrumentId, Option<InstrumentAny>)> = instrument_changes
            .iter()
            .map(|id| (*id, instruments.get(id).cloned()))
            .collect();
        changes.sort_unstable_by_key(|(id, _)| *id);
        let mut instrument_closes: Vec<InstrumentClose> =
            instrument_closes.values().copied().collect();
        instrument_closes.sort_unstable_by_key(|close| close.instrument_id);
        let mut books: Vec<OrderBookState> = books.values().map(OrderBook::export_state).collect();
        books.sort_unstable_by_key(|book| book.instrument_id);
        let mut mark_xrates: Vec<((Currency, Currency), f64)> =
            mark_xrates.iter().map(|(k, v)| (*k, *v)).collect();
        mark_xrates.sort_unstable_by(|((a1, a2), _), ((b1, b2), _)| {
            a1.code.cmp(&b1.code).then_with(|| a2.code.cmp(&b2.code))
        });
        let mut account_states: Vec<CachedAccountState> = accounts
            .values()
            .map(|cell| {
                let account = cell.borrow().clone();
                let balances_locked = balances_locked(&account);
                CachedAccountState {
                    account,
                    balances_locked,
                }
            })
            .collect();
        account_states.sort_unstable_by_key(|state| state.account.id());
        let mut order_states: Vec<OrderAny> =
            orders.values().map(|cell| cell.borrow().clone()).collect();
        order_states.sort_unstable_by_key(Order::client_order_id);
        let mut order_list_states: Vec<OrderList> = order_lists.values().cloned().collect();
        order_list_states.sort_unstable_by_key(|list| list.id);
        let mut position_states: Vec<Position> = positions
            .values()
            .map(|cell| cell.borrow().clone())
            .collect();
        position_states.sort_unstable_by_key(|position| position.id);
        let mut snapshots: Vec<(PositionId, Vec<PositionSnapshotState>)> = position_snapshots
            .iter()
            .map(|(position_id, frames)| {
                let frames = frames
                    .iter()
                    .map(|frame| {
                        let (position, encoded) = frame.parts();
                        PositionSnapshotState {
                            position: position.clone(),
                            encoded: encoded.cloned(),
                        }
                    })
                    .collect();
                (*position_id, frames)
            })
            .collect();
        snapshots.sort_unstable_by_key(|(position_id, _)| *position_id);

        Ok(CacheState {
            general,
            currencies,
            instrument_changes: changes,
            instrument_closes,
            books,
            quotes: histories(quotes),
            trades: histories(trades),
            mark_xrates,
            mark_prices: histories(mark_prices),
            index_prices: histories(index_prices),
            funding_rates: histories(funding_rates),
            instrument_statuses: histories(instrument_statuses),
            bars: histories(bars),
            external_order_claims: sorted(external_order_claims),
            client_accounts: sorted(client_accounts),
            client_routes: sorted(client_routes),
            default_client_id: *default_client_id,
            external_clients: sorted_set(external_clients),
            accounts: account_states,
            orders: order_states,
            order_lists: order_list_states,
            positions: position_states,
            position_snapshots: snapshots,
            position_snapshot_revisions: sorted(position_snapshot_revisions),
            index: index.export_state(),
        })
    }

    /// Overwrites the cache's replay state with `state`. The cache must be one the saving
    /// engine's setup built: its instruments are the setup's, and `state`'s instrument changes are
    /// applied to them. Records instrument changes from here on, including those applied.
    ///
    /// # Errors
    ///
    /// Returns an error if a history's capacity or length is invalid, or a locked balance is
    /// listed for a margin account.
    pub fn import_state(&mut self, state: CacheState) -> anyhow::Result<()> {
        let CacheState {
            general,
            currencies,
            instrument_changes,
            instrument_closes,
            books,
            quotes,
            trades,
            mark_xrates,
            mark_prices,
            index_prices,
            funding_rates,
            instrument_statuses,
            bars,
            external_order_claims,
            client_accounts,
            client_routes,
            default_client_id,
            external_clients,
            accounts,
            orders,
            order_lists,
            positions,
            position_snapshots,
            position_snapshot_revisions,
            index,
        } = state;

        self.general = general.into_iter().collect();
        self.currencies = currencies
            .into_iter()
            .map(|currency| (currency.code, currency))
            .collect::<AHashMap<Ustr, Currency>>();
        let changes = self.instrument_changes.get_or_insert_with(AHashSet::new);
        for (instrument_id, instrument) in instrument_changes {
            changes.insert(instrument_id);
            match instrument {
                Some(instrument) => {
                    anyhow::ensure!(
                        instrument.id() == instrument_id,
                        "instrument change {instrument_id} carries {}",
                        instrument.id()
                    );
                    self.instruments.insert(instrument_id, instrument);
                }
                None => {
                    self.instruments.remove(&instrument_id);
                }
            }
        }
        self.instrument_closes = instrument_closes
            .into_iter()
            .map(|close| (close.instrument_id, close))
            .collect();
        self.books = books
            .into_iter()
            .map(|book| Ok((book.instrument_id, OrderBook::from_state(book)?)))
            .collect::<anyhow::Result<_>>()?;
        self.quotes = restore_histories(quotes)?;
        self.trades = restore_histories(trades)?;
        self.mark_xrates = map_of(mark_xrates);
        self.mark_prices = restore_histories(mark_prices)?;
        self.index_prices = restore_histories(index_prices)?;
        self.funding_rates = restore_histories(funding_rates)?;
        self.instrument_statuses = restore_histories(instrument_statuses)?;
        self.bars = restore_histories(bars)?;
        self.external_order_claims = map_of(external_order_claims);
        self.client_accounts = map_of(client_accounts);
        self.client_routes = map_of(client_routes);
        self.default_client_id = default_client_id;
        self.external_clients = set_of(external_clients);
        self.accounts = accounts
            .into_iter()
            .map(|state| {
                let CachedAccountState {
                    mut account,
                    balances_locked,
                } = state;
                set_balances_locked(&mut account, balances_locked)?;
                Ok((account.id(), SharedCell::new(account)))
            })
            .collect::<anyhow::Result<_>>()?;
        self.orders = orders
            .into_iter()
            .map(|order| (order.client_order_id(), SharedCell::new(order)))
            .collect();
        self.order_lists = order_lists
            .into_iter()
            .map(|list| (list.id, list))
            .collect();
        self.positions = positions
            .into_iter()
            .map(|position| (position.id, SharedCell::new(position)))
            .collect();
        self.position_snapshots = position_snapshots
            .into_iter()
            .map(|(position_id, frames)| {
                let frames = frames
                    .into_iter()
                    .map(|frame| {
                        let PositionSnapshotState { position, encoded } = frame;
                        PositionSnapshotFrame::from_parts(position, encoded)
                    })
                    .collect();
                (position_id, frames)
            })
            .collect();
        self.position_snapshot_revisions = map_of(position_snapshot_revisions);
        self.index = CacheIndex::from_state(index);
        Ok(())
    }
}
