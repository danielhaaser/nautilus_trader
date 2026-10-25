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

//! The engine's per-timestamp settlement skips the work nothing is due for: a scripted workload
//! played under the reference settlement (`BacktestEngine::set_reference_settlement`, the pin's)
//! and under the skipping one gives the same order events, at the same instants, and the same
//! canonical result.

use std::{cell::RefCell, fmt::Debug, rc::Rc};

use nautilus_backtest::{
    config::{BacktestEngineConfig, SimulatedVenueConfig},
    engine::BacktestEngine,
};
use nautilus_common::{actor::DataActor, throttler::RateLimit, timer::TimeEvent};
use nautilus_core::{DurationNanos, UnixNanos};
use nautilus_execution::models::{
    fee::{FeeModelAny, MakerTakerFeeModel},
    latency::{LatencyModelHandle, StaticLatencyModel},
};
use nautilus_model::{
    data::{Data, FundingRateUpdate, MarkPriceUpdate, QuoteTick, TradeTick},
    enums::{AccountType, AggressorSide, BookType, OmsType, OrderSide},
    events::{OrderEventAny, OrderEventType},
    identifiers::{ClientOrderId, InstrumentId, StrategyId, Symbol, TradeId, Venue},
    instruments::{CryptoPerpetual, InstrumentAny, stubs::crypto_perpetual_ethusdt},
    orders::Order,
    types::{Money, Price, Quantity},
};
use nautilus_risk::engine::config::RiskEngineConfig;
use nautilus_trading::{Strategy, StrategyConfig, StrategyCore, nautilus_strategy};
use rstest::rstest;
use serde_json::Value;

/// Sends, from its data and its timers, every kind of command a settlement acts on: limit orders
/// that rest and are later crossed, market orders, modifies, zero-latency cancels, and orders
/// sent from timers that fire between data instants and at them. Logs each order event with the
/// strategy clock's instant at its arrival.
struct SettlementWorkload {
    core: StrategyCore,
    instruments: [InstrumentId; 2],
    quotes_seen: u64,
    timers_set: u64,
    open: Vec<ClientOrderId>,
    log: Rc<RefCell<Vec<String>>>,
}

impl SettlementWorkload {
    fn new(instruments: [InstrumentId; 2], log: Rc<RefCell<Vec<String>>>) -> Self {
        let config = StrategyConfig {
            strategy_id: Some(StrategyId::from("SETTLEMENT-001")),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        };
        Self {
            core: StrategyCore::new(config),
            instruments,
            quotes_seen: 0,
            timers_set: 0,
            open: Vec::new(),
            log,
        }
    }

    fn limit(&mut self, instrument_id: InstrumentId, side: OrderSide, price: Price) {
        let order = self.order().limit(
            instrument_id,
            side,
            Quantity::from("0.020"),
            price,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        self.submit_order(order, None, None, None).unwrap();
    }

    fn market(&mut self, instrument_id: InstrumentId, side: OrderSide) {
        let order = self.order().market(
            instrument_id,
            side,
            Quantity::from("0.020"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        self.submit_order(order, None, None, None).unwrap();
    }

    fn side(n: u64) -> OrderSide {
        if n.is_multiple_of(2) {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        }
    }
}

nautilus_strategy!(SettlementWorkload, {
    fn on_order_event(&mut self, event: OrderEventAny) {
        let now = self.clock().timestamp_ns();
        let client_order_id = event.client_order_id();
        match event.event_type() {
            OrderEventType::Accepted => self.open.push(client_order_id),
            OrderEventType::Filled | OrderEventType::Canceled | OrderEventType::Rejected => {
                let closed = self
                    .cache()
                    .order(&client_order_id)
                    .is_some_and(|order| order.is_closed());
                if closed {
                    self.open.retain(|id| *id != client_order_id);
                }
            }
            _ => {}
        }
        let fill = match &event {
            OrderEventAny::Filled(fill) => format!(" {} @ {}", fill.last_qty, fill.last_px),
            _ => String::new(),
        };
        self.log.borrow_mut().push(format!(
            "{now} {:?} {client_order_id} ts_event={}{fill}",
            event.event_type(),
            event.ts_event()
        ));
    }
});

impl Debug for SettlementWorkload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(SettlementWorkload)).finish()
    }
}

impl DataActor for SettlementWorkload {
    fn on_start(&mut self) -> anyhow::Result<()> {
        for instrument_id in self.instruments {
            self.subscribe_quotes(instrument_id, None, None);
        }
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        self.quotes_seen += 1;
        let n = self.quotes_seen;
        let here = quote.instrument_id;
        let other = if here == self.instruments[0] {
            self.instruments[1]
        } else {
            self.instruments[0]
        };
        match n % 7 {
            // A bid below the touch here, an ask above it on the other market: both rest until
            // a later quote crosses them
            0 => self.limit(here, OrderSide::Buy, Price::from("999.90")),
            1 => self.limit(other, OrderSide::Sell, Price::from("1000.60")),
            // A cancel, with no latency: due at the instant it is sent, whatever the data's
            // market
            2 => {
                if let Some(oldest) = self.open.first().copied() {
                    self.cancel_order(oldest, None, None)?;
                }
            }
            // A modify of the newest resting order towards the touch
            3 => {
                if let Some(newest) = self.open.last().copied() {
                    let order = self.cache().order(&newest).map(|order| order.order_side());
                    let price = match order {
                        Some(OrderSide::Buy) => Price::from("1000.00"),
                        _ => Price::from("1000.40"),
                    };
                    self.modify_order(newest, None, Some(price), None, None, None)?;
                }
            }
            4 => self.market(here, Self::side(n / 7)),
            // Timers: one between data instants, one at the next data instant
            5 => {
                let now = self.clock().timestamp_ns().as_u64();
                for delay in [3, 10] {
                    self.timers_set += 1;
                    let name = format!("workload-{}", self.timers_set);
                    self.clock()
                        .set_time_alert_ns(&name, (now + delay).into(), None, None)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        let n = event.ts_event.as_u64();
        let instrument_id = self.instruments[(n % 2) as usize];
        self.market(instrument_id, Self::side(n / 10));
        if let Some(oldest) = self.open.first().copied() {
            self.cancel_order(oldest, None, None)?;
        }
        Ok(())
    }
}

fn second_perpetual() -> CryptoPerpetual {
    let mut instrument = crypto_perpetual_ethusdt();
    instrument.id = InstrumentId::from("BTCUSDT-PERP.BINANCE");
    instrument.raw_symbol = Symbol::from("BTCUSDT");
    instrument
}

fn quote(instrument_id: InstrumentId, bid: &str, ask: &str, ts: u64) -> Data {
    Data::Quote(QuoteTick::new(
        instrument_id,
        Price::from(bid),
        Price::from(ask),
        Quantity::from("1.000"),
        Quantity::from("1.000"),
        ts.into(),
        ts.into(),
    ))
}

/// Quotes on both markets every 10 ns, both at one instant every third step, moving the touch
/// through the resting orders' prices; trades every fifth step; and a mark price and a funding
/// rate on the first market.
fn workload_data(instruments: [InstrumentId; 2]) -> Vec<Data> {
    const TOUCHES: [(&str, &str); 6] = [
        ("1000.00", "1000.50"),
        ("999.80", "1000.30"),
        ("1000.20", "1000.70"),
        ("999.90", "1000.40"),
        ("1000.40", "1000.90"),
        ("999.70", "1000.20"),
    ];
    let mut data = Vec::new();
    for step in 0..360_u64 {
        let ts = 1_000 + step * 10;
        let (bid, ask) = TOUCHES[(step % 6) as usize];
        data.push(quote(instruments[(step % 2) as usize], bid, ask, ts));
        if step % 3 == 0 {
            let (bid, ask) = TOUCHES[((step + 2) % 6) as usize];
            data.push(quote(instruments[((step + 1) % 2) as usize], bid, ask, ts));
        }
        if step % 5 == 0 {
            data.push(Data::Trade(TradeTick::new(
                instruments[(step % 2) as usize],
                Price::from(bid),
                Quantity::from("0.500"),
                AggressorSide::NoAggressor,
                TradeId::new(format!("T-{step}")),
                ts.into(),
                ts.into(),
            )));
        }
    }
    data.push(Data::MarkPrice(MarkPriceUpdate::new(
        instruments[0],
        Price::from("1000.00"),
        UnixNanos::from(1_995),
        UnixNanos::from(1_995),
    )));
    data.push(Data::FundingRate(FundingRateUpdate::new(
        instruments[0],
        "0.001".parse().unwrap(),
        Some(480),
        Some(UnixNanos::from(2_505)),
        UnixNanos::from(1_997),
        UnixNanos::from(1_997),
    )));
    data.sort_by_key(nautilus_model::data::HasTsInit::ts_init);
    data
}

/// The latency legs of a case: (insert, update, delete) command latency and (fill, ack) event
/// latency, or no latency model at all.
type Legs = Option<((u64, u64, u64), Option<(u64, u64)>)>;

struct Played {
    log: Vec<String>,
    canonical: Value,
    iterations: usize,
}

/// The canonical document with each funding settlement's reason, which carries a fresh UUID per
/// run whichever settlement runs it, reduced to its kind.
fn without_settlement_ids(mut value: Value) -> Value {
    match &mut value {
        Value::String(text) if text.starts_with("funding_settlement:") => {
            *text = "funding_settlement".to_string();
        }
        Value::Array(items) => {
            for item in items {
                *item = without_settlement_ids(item.take());
            }
        }
        Value::Object(fields) => {
            for field in fields.values_mut() {
                *field = without_settlement_ids(field.take());
            }
        }
        _ => {}
    }
    value
}

fn play(legs: Legs, reference: bool) -> Played {
    // The workload sends hundreds of orders within a few microseconds: past the default 100/sec
    let unlimited = RateLimit::new(1_000_000, DurationNanos::from_secs(1));
    let config = BacktestEngineConfig {
        bypass_logging: true,
        run_analysis: false,
        risk_engine: Some(RiskEngineConfig {
            max_order_submit: unlimited,
            max_order_modify: unlimited,
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut engine = BacktestEngine::new(config).unwrap();
    engine.set_reference_settlement(reference);
    let mut venue = SimulatedVenueConfig::builder()
        .venue(Venue::from("BINANCE"))
        .oms_type(OmsType::Netting)
        .account_type(AccountType::Margin)
        .book_type(BookType::L1_MBP)
        .starting_balances(vec![Money::from("1_000_000 USDT")])
        .fee_model(FeeModelAny::MakerTaker(MakerTakerFeeModel::zero()).into())
        .build()
        .unwrap();
    if let Some(((insert, update, delete), events)) = legs {
        let mut latency = StaticLatencyModel::new(
            DurationNanos::default(),
            DurationNanos::new(insert),
            DurationNanos::new(update),
            DurationNanos::new(delete),
        );
        if let Some((fill, ack)) = events {
            latency = latency.with_event_latency(DurationNanos::new(fill), DurationNanos::new(ack));
        }
        venue.latency_model = Some(LatencyModelHandle::new(latency));
    }
    engine.add_venue(venue).unwrap();

    let first = crypto_perpetual_ethusdt();
    let second = second_perpetual();
    let instruments = [first.id, second.id];
    engine
        .add_instrument(&InstrumentAny::CryptoPerpetual(first))
        .unwrap();
    engine
        .add_instrument(&InstrumentAny::CryptoPerpetual(second))
        .unwrap();
    engine
        .add_data(workload_data(instruments), None, true, true)
        .unwrap();
    let log = Rc::new(RefCell::new(Vec::new()));
    engine
        .add_strategy(SettlementWorkload::new(instruments, Rc::clone(&log)))
        .unwrap();
    engine.run(None, None, None, false).unwrap();

    let canonical =
        without_settlement_ids(engine.get_canonical_result().unwrap().as_value().clone());
    let iterations = engine.get_result().iterations;
    let log = log.borrow().clone();
    Played {
        log,
        canonical,
        iterations,
    }
}

#[rstest]
// Command latency on submits and modifies, none on cancels, and event legs on the way back
#[case::latency_and_event_legs(Some(((5, 3, 0), Some((2, 1)))))]
// Command latency only: every event reaches the node at the venue instant
#[case::command_latency(Some(((5, 3, 0), None)))]
// A latency model with every leg zero: each command is due at the instant it is sent
#[case::zero_latency(Some(((0, 0, 0), None)))]
// No latency model: commands wait in the venue's message queue for the next settlement
#[case::no_latency_model(None)]
fn test_settlement_skips_give_the_reference_settlement_results(#[case] legs: Legs) {
    let reference = play(legs, true);
    let skipping = play(legs, false);

    // The workload exercises what a settlement acts on
    let count = |kind: &str| {
        reference
            .log
            .iter()
            .filter(|line| line.split(' ').nth(1) == Some(kind))
            .count()
    };
    assert!(count("Filled") > 20, "fills: {}", count("Filled"));
    assert!(count("Canceled") > 10, "cancels: {}", count("Canceled"));
    assert!(count("Updated") > 5, "modifies: {}", count("Updated"));
    assert_eq!(count("Denied"), 0);
    let funding = reference
        .canonical
        .to_string()
        .matches("funding_settlement")
        .count();
    assert!(funding > 0, "no funding settlement");

    assert_eq!(skipping.iterations, reference.iterations);
    assert_eq!(skipping.canonical, reference.canonical);
    assert_eq!(skipping.log.len(), reference.log.len());
    for (index, (expected, actual)) in reference.log.iter().zip(&skipping.log).enumerate() {
        assert_eq!(actual, expected, "order event {index}");
    }
}
