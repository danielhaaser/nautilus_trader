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

//! A [`Portfolio`]'s state as plain data, for saving a backtest and resuming it.

use std::{collections::BTreeMap, fmt::Debug, hash::Hash, rc::Rc};

use ahash::{AHashMap, AHashSet};
use indexmap::IndexMap;
use nautilus_analysis::analyzer::PortfolioAnalyzer;
use nautilus_common::timer::{TimeEvent, TimeEventCallback};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    enums::PositionSide,
    events::PortfolioSnapshot,
    identifiers::{AccountId, InstrumentId, PositionId, Venue},
    position::Position,
    types::{Currency, Money, Price},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::{
    EQUITY_CURVE_TIMER_PREFIX, Portfolio, PortfolioState, SNAPSHOT_TIMER_PREFIX, emit_snapshot,
};

/// Realized PnLs per currency: each position's realized PnL and its instant, in order.
pub type RealizedPnlsState = Vec<(Currency, Vec<(PositionId, UnixNanos, f64)>)>;

/// The instruments missing a price per venue and account.
pub type MissingPricesState = Vec<(Venue, Vec<(Option<AccountId>, Vec<InstrumentId>)>)>;

/// A [`PortfolioAnalyzer`]'s accumulators (its statistics are registered by setup).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PortfolioAnalyzerState {
    /// The starting balances, in order.
    pub account_balances_starting: IndexMap<Currency, Money>,
    /// The balances, in order.
    pub account_balances: IndexMap<Currency, Money>,
    /// The positions analyzed, in order.
    pub positions: Vec<Position>,
    /// The realized PnLs per currency, sorted by currency.
    pub realized_pnls: RealizedPnlsState,
    /// The recorded realized PnLs per currency, sorted by currency.
    pub recorded_realized_pnls: RealizedPnlsState,
    /// The position returns.
    pub position_returns: BTreeMap<UnixNanos, f64>,
    /// The portfolio returns.
    pub portfolio_returns: BTreeMap<UnixNanos, f64>,
    /// The primary returns.
    pub returns: BTreeMap<UnixNanos, f64>,
}

/// A [`Portfolio`]'s replay state. Each hashed collection is sorted by its key's debug form, so
/// equal states serialize equally.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[expect(
    missing_docs,
    reason = "each field mirrors the portfolio state's field of the same name"
)]
pub struct PortfolioResumeState {
    pub analyzer: PortfolioAnalyzerState,
    pub unrealized_pnls: IndexMap<InstrumentId, Money>,
    pub realized_pnls: IndexMap<InstrumentId, Money>,
    pub recorded_closed_position_cycles: Vec<(PositionId, UnixNanos)>,
    pub snapshot_sum_per_position: Vec<(PositionId, Money)>,
    pub snapshot_last_per_position: Vec<(PositionId, Money)>,
    pub snapshot_currency_mismatches: Vec<PositionId>,
    pub snapshot_aggregation_overflows: Vec<PositionId>,
    pub snapshot_processed_counts: Vec<(PositionId, usize)>,
    pub snapshot_processed_revisions: Vec<(PositionId, u64)>,
    pub snapshot_account_ids: Vec<(PositionId, AccountId)>,
    pub net_positions: IndexMap<InstrumentId, Decimal>,
    pub pending_calcs: Vec<InstrumentId>,
    pub bar_close_prices: Vec<(InstrumentId, Price)>,
    pub last_prices: Vec<((InstrumentId, PositionSide), Price)>,
    pub last_xrates: Vec<((Venue, Currency, Currency), Decimal)>,
    pub stale_prices: Vec<(InstrumentId, PositionSide)>,
    pub stale_xrates: Vec<(Venue, Currency, Currency)>,
    pub initialized: bool,
    pub last_account_state_log_ts: Vec<(AccountId, UnixNanos)>,
    pub last_published_account_states: Vec<(AccountId, UUID4)>,
    pub venues_missing_price: MissingPricesState,
    pub account_open_positions: Vec<(AccountId, usize)>,
    pub equity_curve_accounts: Vec<AccountId>,
    pub equity_curve_finalized: bool,
    pub portfolio_snapshots: Vec<(AccountId, Vec<PortfolioSnapshot>)>,
    pub pre_position_fill_events: Vec<UUID4>,
    pub balance_error: Option<String>,
}

fn sort_key<K: Debug>(key: &K) -> String {
    format!("{key:?}")
}

fn sorted_map<K: Debug + Clone, V: Clone>(map: &AHashMap<K, V>) -> Vec<(K, V)> {
    let mut entries: Vec<(String, K, V)> = map
        .iter()
        .map(|(k, v)| (sort_key(k), k.clone(), v.clone()))
        .collect();
    entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    entries.into_iter().map(|(_, k, v)| (k, v)).collect()
}

fn sorted_set<K: Debug + Clone>(set: &AHashSet<K>) -> Vec<K> {
    let mut keys: Vec<(String, K)> = set.iter().map(|k| (sort_key(k), k.clone())).collect();
    keys.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    keys.into_iter().map(|(_, k)| k).collect()
}

fn map_of<K: Eq + Hash, V>(entries: Vec<(K, V)>) -> AHashMap<K, V> {
    entries.into_iter().collect()
}

fn set_of<K: Eq + Hash>(keys: Vec<K>) -> AHashSet<K> {
    keys.into_iter().collect()
}

fn export_analyzer(analyzer: &PortfolioAnalyzer) -> PortfolioAnalyzerState {
    let PortfolioAnalyzer {
        statistics: _, // Rebuilt by setup: the registered statistics
        account_balances_starting,
        account_balances,
        positions,
        realized_pnls,
        recorded_realized_pnls,
        position_returns,
        portfolio_returns,
        returns,
    } = analyzer;
    PortfolioAnalyzerState {
        account_balances_starting: account_balances_starting.clone(),
        account_balances: account_balances.clone(),
        positions: positions.clone(),
        realized_pnls: sorted_map(realized_pnls),
        recorded_realized_pnls: sorted_map(recorded_realized_pnls),
        position_returns: position_returns.clone(),
        portfolio_returns: portfolio_returns.clone(),
        returns: returns.clone(),
    }
}

fn import_analyzer(analyzer: &mut PortfolioAnalyzer, state: PortfolioAnalyzerState) {
    let PortfolioAnalyzerState {
        account_balances_starting,
        account_balances,
        positions,
        realized_pnls,
        recorded_realized_pnls,
        position_returns,
        portfolio_returns,
        returns,
    } = state;
    analyzer.account_balances_starting = account_balances_starting;
    analyzer.account_balances = account_balances;
    analyzer.positions = positions;
    analyzer.realized_pnls = map_of(realized_pnls);
    analyzer.recorded_realized_pnls = map_of(recorded_realized_pnls);
    analyzer.position_returns = position_returns;
    analyzer.portfolio_returns = portfolio_returns;
    analyzer.returns = returns;
}

impl Portfolio {
    /// Returns the callback of the portfolio's own timer `name` (an account's equity curve or
    /// snapshot timer), as the portfolio built it when it armed the timer: for a resumed backtest,
    /// whose clocks are restored with each timer's callback re-attached. `None` for any other name.
    #[must_use]
    pub fn timer_callback(&self, name: &str) -> Option<TimeEventCallback> {
        let account = name
            .strip_prefix(EQUITY_CURVE_TIMER_PREFIX)
            .or_else(|| name.strip_prefix(SNAPSHOT_TIMER_PREFIX))?;
        let account_id = AccountId::from(account);
        let config = self.config;
        let cache_weak = Rc::downgrade(&self.cache);
        let clock_weak = Rc::downgrade(&self.clock);
        let inner_weak = Rc::downgrade(&self.inner);
        let callback: Rc<dyn Fn(TimeEvent)> = Rc::new(move |event| {
            let Some(cache) = cache_weak.upgrade() else {
                return;
            };
            let Some(clock) = clock_weak.upgrade() else {
                return;
            };
            let Some(inner) = inner_weak.upgrade() else {
                return;
            };
            emit_snapshot(&cache, &clock, &inner, config, account_id, event.ts_event);
        });
        Some(TimeEventCallback::from(callback))
    }

    /// Returns the portfolio's replay state.
    #[must_use]
    pub fn export_state(&self) -> PortfolioResumeState {
        let Self {
            clock: _, // Rebuilt by setup: the kernel's clock
            cache: _, // Rebuilt by setup: the kernel's cache
            inner,
            config: _, // Rebuilt by setup
        } = self;
        let inner = inner.borrow();
        let PortfolioState {
            accounts: _, // Rebuilt by setup: the accounts manager holds the clock and cache only
            analyzer,
            unrealized_pnls,
            realized_pnls,
            recorded_closed_position_cycles,
            snapshot_sum_per_position,
            snapshot_last_per_position,
            snapshot_currency_mismatches,
            snapshot_aggregation_overflows,
            snapshot_processed_counts,
            snapshot_processed_revisions,
            snapshot_account_ids,
            net_positions,
            pending_calcs,
            bar_close_prices,
            last_prices,
            last_xrates,
            stale_prices,
            stale_xrates,
            initialized,
            last_account_state_log_ts,
            last_published_account_states,
            min_account_state_logging_interval_ns: _, // Rebuilt by setup: the config
            venues_missing_price,
            account_open_positions,
            equity_curve_accounts,
            equity_curve_finalized,
            portfolio_snapshots,
            pre_position_fill_events,
            balance_error,
        } = &*inner;
        let mut venues_missing: MissingPricesState = venues_missing_price
            .iter()
            .map(|(venue, by_account)| {
                let by_account = sorted_map(by_account)
                    .into_iter()
                    .map(|(account_id, ids)| (account_id, sorted_set(&ids)))
                    .collect();
                (*venue, by_account)
            })
            .collect();
        venues_missing.sort_unstable_by_key(|(venue, _)| sort_key(venue));
        PortfolioResumeState {
            analyzer: export_analyzer(analyzer),
            unrealized_pnls: unrealized_pnls.clone(),
            realized_pnls: realized_pnls.clone(),
            recorded_closed_position_cycles: sorted_set(recorded_closed_position_cycles),
            snapshot_sum_per_position: sorted_map(snapshot_sum_per_position),
            snapshot_last_per_position: sorted_map(snapshot_last_per_position),
            snapshot_currency_mismatches: sorted_set(snapshot_currency_mismatches),
            snapshot_aggregation_overflows: sorted_set(snapshot_aggregation_overflows),
            snapshot_processed_counts: sorted_map(snapshot_processed_counts),
            snapshot_processed_revisions: sorted_map(snapshot_processed_revisions),
            snapshot_account_ids: sorted_map(snapshot_account_ids),
            net_positions: net_positions.clone(),
            pending_calcs: sorted_set(pending_calcs),
            bar_close_prices: sorted_map(bar_close_prices),
            last_prices: sorted_map(last_prices),
            last_xrates: sorted_map(last_xrates),
            stale_prices: sorted_set(stale_prices),
            stale_xrates: sorted_set(stale_xrates),
            initialized: *initialized,
            last_account_state_log_ts: sorted_map(last_account_state_log_ts),
            last_published_account_states: sorted_map(last_published_account_states),
            venues_missing_price: venues_missing,
            account_open_positions: sorted_map(account_open_positions),
            equity_curve_accounts: sorted_set(equity_curve_accounts),
            equity_curve_finalized: *equity_curve_finalized,
            portfolio_snapshots: sorted_map(portfolio_snapshots)
                .into_iter()
                .map(|(account_id, snapshots)| (account_id, snapshots.into_iter().collect()))
                .collect(),
            pre_position_fill_events: sorted_set(pre_position_fill_events),
            balance_error: balance_error.clone(),
        }
    }

    /// Overwrites the portfolio's replay state with `state`.
    pub fn import_state(&self, state: PortfolioResumeState) {
        let PortfolioResumeState {
            analyzer,
            unrealized_pnls,
            realized_pnls,
            recorded_closed_position_cycles,
            snapshot_sum_per_position,
            snapshot_last_per_position,
            snapshot_currency_mismatches,
            snapshot_aggregation_overflows,
            snapshot_processed_counts,
            snapshot_processed_revisions,
            snapshot_account_ids,
            net_positions,
            pending_calcs,
            bar_close_prices,
            last_prices,
            last_xrates,
            stale_prices,
            stale_xrates,
            initialized,
            last_account_state_log_ts,
            last_published_account_states,
            venues_missing_price,
            account_open_positions,
            equity_curve_accounts,
            equity_curve_finalized,
            portfolio_snapshots,
            pre_position_fill_events,
            balance_error,
        } = state;
        let mut inner = self.inner.borrow_mut();
        import_analyzer(&mut inner.analyzer, analyzer);
        inner.unrealized_pnls = unrealized_pnls;
        inner.realized_pnls = realized_pnls;
        inner.recorded_closed_position_cycles = set_of(recorded_closed_position_cycles);
        inner.snapshot_sum_per_position = map_of(snapshot_sum_per_position);
        inner.snapshot_last_per_position = map_of(snapshot_last_per_position);
        inner.snapshot_currency_mismatches = set_of(snapshot_currency_mismatches);
        inner.snapshot_aggregation_overflows = set_of(snapshot_aggregation_overflows);
        inner.snapshot_processed_counts = map_of(snapshot_processed_counts);
        inner.snapshot_processed_revisions = map_of(snapshot_processed_revisions);
        inner.snapshot_account_ids = map_of(snapshot_account_ids);
        inner.net_positions = net_positions;
        inner.pending_calcs = set_of(pending_calcs);
        inner.bar_close_prices = map_of(bar_close_prices);
        inner.last_prices = map_of(last_prices);
        inner.last_xrates = map_of(last_xrates);
        inner.stale_prices = set_of(stale_prices);
        inner.stale_xrates = set_of(stale_xrates);
        inner.initialized = initialized;
        inner.last_account_state_log_ts = map_of(last_account_state_log_ts);
        inner.last_published_account_states = map_of(last_published_account_states);
        inner.venues_missing_price = venues_missing_price
            .into_iter()
            .map(|(venue, by_account)| {
                let by_account = by_account
                    .into_iter()
                    .map(|(account_id, ids)| (account_id, set_of(ids)))
                    .collect();
                (venue, by_account)
            })
            .collect();
        inner.account_open_positions = map_of(account_open_positions);
        inner.equity_curve_accounts = set_of(equity_curve_accounts);
        inner.equity_curve_finalized = equity_curve_finalized;
        inner.portfolio_snapshots = portfolio_snapshots
            .into_iter()
            .map(|(account_id, snapshots)| (account_id, snapshots.into_iter().collect()))
            .collect();
        inner.pre_position_fill_events = set_of(pre_position_fill_events);
        inner.balance_error = balance_error;
    }
}
