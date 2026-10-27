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

//! A streamed backtest saved at a batch boundary (`BacktestEngine::export_state`) and resumed in a
//! freshly built engine (`BacktestEngine::import_state`) gives the uninterrupted run: a workload
//! on an L2 venue with queue positions and liquidity consumption, a latency model holding commands
//! and events across the cut, timers (a repeating one, alerts, a host alert with a named
//! callback) across it, and an instrument expiring mid-run, saved at every boundary.

use std::{cell::RefCell, fmt::Debug, rc::Rc, time::Instant};

use nautilus_backtest::{
    config::{BacktestEngineConfig, SimulatedVenueConfig},
    engine::{BacktestEngine, state::EngineState},
};
use nautilus_common::{
    actor::DataActor,
    throttler::RateLimit,
    timer::{TimeEvent, TimeEventCallback},
};
use nautilus_core::{DurationNanos, UnixNanos};
use nautilus_execution::models::{
    fee::{FeeModelAny, MakerTakerFeeModel},
    latency::{LatencyModelHandle, StaticLatencyModel},
};
use nautilus_model::{
    data::{BookOrder, Data, OrderBookDelta, OrderBookDeltas, TradeTick},
    enums::{AccountType, AggressorSide, BookAction, BookType, OmsType, OrderSide, RecordFlag},
    events::{OrderEventAny, OrderEventType},
    identifiers::{ClientOrderId, InstrumentId, StrategyId, Symbol, TradeId, Venue},
    instruments::{FuturesContract, InstrumentAny, stubs::futures_contract_es},
    orders::Order,
    types::{Money, Price, Quantity},
};
use nautilus_risk::engine::config::RiskEngineConfig;
use nautilus_trading::{Strategy, StrategyConfig, StrategyCore, nautilus_strategy};
use serde_json::Value;

const BASE: u64 = 1_000_000;
const STEP: u64 = 10;
const STEPS: u64 = 360;
const BATCH_NS: u64 = 600;
const EXPIRY: u64 = BASE + 2_005;
/// The untraded third market's expiration: its engine settles with nothing open and is released,
/// then revived by its later book updates.
const IDLE_EXPIRY: u64 = BASE + 1_505;
const REPEAT_NS: u64 = 53;
const HOST_ALERT: &str = "host-alert";

/// What the workload remembers between callbacks, shared with the test so a resumed run starts
/// from the saved run's (the candidate's own checkpoint, outside the engine).
#[derive(Clone, Debug, Default)]
struct Memory {
    deltas_seen: u64,
    trades_seen: u64,
    alerts_set: u64,
    open: Vec<ClientOrderId>,
}

/// Rests limit orders at the touch (behind the queue there) and deeper, crosses with market
/// orders, modifies and cancels, from its book updates and from timers; logs every order event
/// with the strategy clock's instant at its arrival.
struct ResumeWorkload {
    core: StrategyCore,
    instruments: [InstrumentId; 2],
    memory: Rc<RefCell<Memory>>,
    log: Rc<RefCell<Vec<String>>>,
}

impl ResumeWorkload {
    fn new(
        instruments: [InstrumentId; 2],
        memory: Rc<RefCell<Memory>>,
        log: Rc<RefCell<Vec<String>>>,
    ) -> Self {
        let config = StrategyConfig {
            strategy_id: Some(StrategyId::from("RESUME-001")),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        };
        Self {
            core: StrategyCore::new(config),
            instruments,
            memory,
            log,
        }
    }

    fn live(&self, instrument_id: InstrumentId) -> bool {
        self.clock().timestamp_ns().as_u64() < EXPIRY - 100 || instrument_id == self.instruments[0]
    }

    fn limit(&mut self, instrument_id: InstrumentId, side: OrderSide, price: Price, qty: &str) {
        if !self.live(instrument_id) {
            return;
        }
        let order = self.order().limit(
            instrument_id,
            side,
            Quantity::from(qty),
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

    fn market(&mut self, instrument_id: InstrumentId, side: OrderSide, qty: &str) {
        if !self.live(instrument_id) {
            return;
        }
        let order = self.order().market(
            instrument_id,
            side,
            Quantity::from(qty),
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

    fn touch(&self, instrument_id: InstrumentId, side: OrderSide) -> Option<Price> {
        let cache = self.cache();
        let book = cache.order_book(&instrument_id)?;
        match side {
            OrderSide::Buy => book.best_bid_price(),
            _ => book.best_ask_price(),
        }
    }

    fn side(n: u64) -> OrderSide {
        if n.is_multiple_of(2) {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        }
    }

    fn oldest_open(&self) -> Option<ClientOrderId> {
        self.memory.borrow().open.first().copied()
    }
}

nautilus_strategy!(ResumeWorkload, {
    fn on_order_event(&mut self, event: OrderEventAny) {
        let now = self.clock().timestamp_ns();
        let client_order_id = event.client_order_id();
        match event.event_type() {
            OrderEventType::Accepted => self.memory.borrow_mut().open.push(client_order_id),
            OrderEventType::Filled
            | OrderEventType::Canceled
            | OrderEventType::Rejected
            | OrderEventType::Expired => {
                let closed = self
                    .cache()
                    .order(&client_order_id)
                    .is_some_and(|order| order.is_closed());
                if closed {
                    self.memory
                        .borrow_mut()
                        .open
                        .retain(|id| *id != client_order_id);
                }
            }
            _ => {}
        }
        let detail = match &event {
            OrderEventAny::Filled(fill) => format!(
                " {} @ {} trade={} venue_order={} position={:?}",
                fill.last_qty, fill.last_px, fill.trade_id, fill.venue_order_id, fill.position_id
            ),
            OrderEventAny::Accepted(accepted) => {
                format!(" venue_order={}", accepted.venue_order_id)
            }
            OrderEventAny::Denied(denied) => format!(" reason={}", denied.reason),
            OrderEventAny::Rejected(rejected) => format!(" reason={}", rejected.reason),
            OrderEventAny::Updated(updated) => {
                format!(" qty={} price={:?}", updated.quantity, updated.price)
            }
            _ => String::new(),
        };
        self.log.borrow_mut().push(format!(
            "{now} {:?} {client_order_id} ts_event={}{detail}",
            event.event_type(),
            event.ts_event()
        ));
    }
});

impl Debug for ResumeWorkload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(ResumeWorkload)).finish()
    }
}

impl DataActor for ResumeWorkload {
    fn on_start(&mut self) -> anyhow::Result<()> {
        for instrument_id in self.instruments {
            self.subscribe_book_deltas(instrument_id, BookType::L2_MBP, None, None, true, None);
            self.subscribe_trades(instrument_id, None, None);
        }
        let now = self.clock().timestamp_ns();
        self.clock().set_timer_ns(
            "workload-repeat",
            DurationNanos::new(REPEAT_NS),
            Some(now),
            None,
            None,
            None,
            None,
        )?;
        self.log.borrow_mut().push(format!("{now} started"));
        Ok(())
    }

    fn on_book_deltas(&mut self, deltas: &OrderBookDeltas) -> anyhow::Result<()> {
        let n = {
            let mut memory = self.memory.borrow_mut();
            memory.deltas_seen += 1;
            memory.deltas_seen
        };
        let here = deltas.instrument_id;
        let other = if here == self.instruments[0] {
            self.instruments[1]
        } else {
            self.instruments[0]
        };
        match n % 11 {
            // At the touch: behind the queue resting there, filled by trades in parts
            0 => {
                if let Some(bid) = self.touch(here, OrderSide::Buy) {
                    self.limit(here, OrderSide::Buy, bid, "3");
                }
            }
            1 => {
                if let Some(ask) = self.touch(other, OrderSide::Sell) {
                    self.limit(other, OrderSide::Sell, ask, "2");
                }
            }
            2 => {
                if let Some(oldest) = self.oldest_open() {
                    self.cancel_order(oldest, None, None)?;
                }
            }
            // A modify of the newest resting order: more size at the same price
            3 => {
                let newest = self.memory.borrow().open.last().copied();
                if let Some(newest) = newest {
                    let quantity = self.cache().order(&newest).map(|order| order.quantity());
                    if let Some(quantity) = quantity {
                        let more = Quantity::from(format!("{}", quantity.as_f64() as u64 + 1));
                        self.modify_order(newest, Some(more), None, None, None, None)?;
                    }
                }
            }
            4 => self.market(here, Self::side(n / 11), "1"),
            // Alerts: one before the next update, one later (often past the batch)
            5 => {
                let now = self.clock().timestamp_ns().as_u64();
                for delay in [3, 157] {
                    let set = {
                        let mut memory = self.memory.borrow_mut();
                        memory.alerts_set += 1;
                        memory.alerts_set
                    };
                    let name = format!("workload-alert-{set}");
                    self.clock()
                        .set_time_alert_ns(&name, (now + delay).into(), None, None)?;
                }
            }
            // Deeper in the book: rests until the touch moves through it
            6 => {
                if let Some(bid) = self.touch(here, OrderSide::Buy) {
                    let deeper = Price::from(format!("{:.2}", bid.as_f64() - 0.01));
                    self.limit(here, OrderSide::Buy, deeper, "1");
                }
            }
            8 => {
                if let Some(ask) = self.touch(here, OrderSide::Sell) {
                    self.limit(here, OrderSide::Sell, ask, "4");
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn on_trade(&mut self, _trade: &TradeTick) -> anyhow::Result<()> {
        self.memory.borrow_mut().trades_seen += 1;
        Ok(())
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        let n = event.ts_event.as_u64();
        self.log.borrow_mut().push(format!(
            "{} timer {} {}",
            self.clock().timestamp_ns(),
            event.name,
            n
        ));
        let instrument_id = self.instruments[(n % 2) as usize];
        self.market(instrument_id, Self::side(n / 10), "1");
        if let Some(oldest) = self.oldest_open() {
            self.cancel_order(oldest, None, None)?;
        }
        Ok(())
    }
}

fn future(symbol: &str, expiration: u64) -> FuturesContract {
    let mut instrument =
        futures_contract_es(Some(UnixNanos::from(1)), Some(UnixNanos::from(expiration)));
    instrument.id = InstrumentId::from(format!("{symbol}.GLBX").as_str());
    instrument.raw_symbol = Symbol::from(symbol);
    instrument
}

fn delta(
    id: InstrumentId,
    action: BookAction,
    side: OrderSide,
    price: &str,
    size: u64,
    ts: u64,
) -> Data {
    Data::BookDelta(OrderBookDelta::new(
        id,
        action,
        BookOrder::new(side, Price::from(price), Quantity::from(size), 0),
        RecordFlag::F_LAST as u8,
        0,
        ts.into(),
        ts.into(),
    ))
}

fn trade(
    id: InstrumentId,
    price: &str,
    size: u64,
    aggressor: AggressorSide,
    n: u64,
    ts: u64,
) -> Data {
    Data::Trade(TradeTick::new(
        id,
        Price::from(price),
        Quantity::from(size),
        aggressor,
        TradeId::new(format!("T-{n}")),
        ts.into(),
        ts.into(),
    ))
}

/// Book snapshots for both markets, then level size changes, a level appearing and going one
/// tick inside the touch, and trades at the touch on both sides; the second market stops before
/// its expiration.
fn workload_data(instruments: [InstrumentId; 2]) -> Vec<Data> {
    let mut data = Vec::new();
    for id in instruments {
        for (side, price) in [
            (OrderSide::Buy, "4500.00"),
            (OrderSide::Buy, "4499.99"),
            (OrderSide::Sell, "4500.02"),
            (OrderSide::Sell, "4500.03"),
        ] {
            data.push(delta(id, BookAction::Add, side, price, 10, BASE));
        }
    }
    for step in 1..STEPS {
        let ts = BASE + step * STEP;
        let id = instruments[(step % 2) as usize];
        if id == instruments[1] && ts >= EXPIRY - 50 {
            continue;
        }
        let size = 5 + (step * 7) % 11;
        match step % 6 {
            0 => data.push(delta(
                id,
                BookAction::Update,
                OrderSide::Buy,
                "4500.00",
                size,
                ts,
            )),
            1 => data.push(delta(
                id,
                BookAction::Update,
                OrderSide::Sell,
                "4500.02",
                size,
                ts,
            )),
            2 => data.push(delta(id, BookAction::Add, OrderSide::Buy, "4500.01", 3, ts)),
            3 => data.push(delta(
                id,
                BookAction::Delete,
                OrderSide::Buy,
                "4500.01",
                3,
                ts,
            )),
            4 => data.push(delta(
                id,
                BookAction::Update,
                OrderSide::Buy,
                "4499.99",
                size + 2,
                ts,
            )),
            _ => data.push(delta(
                id,
                BookAction::Update,
                OrderSide::Sell,
                "4500.03",
                size,
                ts,
            )),
        }
        if step % 4 == 0 {
            data.push(trade(
                id,
                "4500.00",
                1 + step % 4,
                AggressorSide::Sell,
                step,
                ts,
            ));
        }
        if step % 5 == 0 {
            data.push(trade(
                id,
                "4500.02",
                2 + step % 3,
                AggressorSide::Buy,
                step,
                ts,
            ));
        }
        // Both markets at one instant now and then
        if step % 9 == 0 && ts < EXPIRY - 50 {
            let other = instruments[((step + 1) % 2) as usize];
            data.push(delta(
                other,
                BookAction::Update,
                OrderSide::Sell,
                "4500.02",
                size + 1,
                ts,
            ));
        }
    }
    // The third market: updated every 30 ns before and after its expiration, never traded
    let third = InstrumentId::from("ESM22.GLBX");
    data.push(delta(
        third,
        BookAction::Add,
        OrderSide::Buy,
        "4400.00",
        10,
        BASE,
    ));
    data.push(delta(
        third,
        BookAction::Add,
        OrderSide::Sell,
        "4400.05",
        10,
        BASE,
    ));
    for step in 1..(STEPS * STEP / 30) {
        let ts = BASE + step * 30 + 1;
        data.push(delta(
            third,
            BookAction::Update,
            OrderSide::Buy,
            "4400.00",
            5 + step % 9,
            ts,
        ));
    }
    data.sort_by_key(nautilus_model::data::HasTsInit::ts_init);
    data
}

/// The data of each batch, and each batch's end.
fn batches(data: &[Data]) -> Vec<(Vec<Data>, u64)> {
    let last = BASE + STEPS * STEP;
    let mut out = Vec::new();
    let mut start = BASE;
    let mut end = BASE + BATCH_NS;
    while start < last {
        let batch: Vec<Data> = data
            .iter()
            .filter(|d| {
                let ts = nautilus_model::data::HasTsInit::ts_init(*d).as_u64();
                (if out.is_empty() {
                    ts >= start
                } else {
                    ts > start
                }) && ts <= end
            })
            .cloned()
            .collect();
        out.push((batch, end));
        start = end;
        end += BATCH_NS;
    }
    out
}

/// A run's engine and what its workload and host write.
struct Rig {
    engine: BacktestEngine,
    memory: Rc<RefCell<Memory>>,
    log: Rc<RefCell<Vec<String>>>,
}

fn host_callback(log: &Rc<RefCell<Vec<String>>>) -> TimeEventCallback {
    let log = Rc::clone(log);
    let callback: Rc<dyn Fn(TimeEvent)> = Rc::new(move |event: TimeEvent| {
        log.borrow_mut()
            .push(format!("{} host {}", event.ts_event, event.name));
    });
    TimeEventCallback::from(callback)
}

/// The setup a run and its resumption share: the venue, the instruments in order, the idle
/// instruments, the host's alert, the workload.
fn setup(idle: usize, memory: Memory, log: Vec<String>) -> Rig {
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
    let mut venue = SimulatedVenueConfig::builder()
        .venue(Venue::from("GLBX"))
        .oms_type(OmsType::Netting)
        .account_type(AccountType::Margin)
        .book_type(BookType::L2_MBP)
        .starting_balances(vec![Money::from("1_000_000 USD")])
        .fee_model(FeeModelAny::MakerTaker(MakerTakerFeeModel::zero()).into())
        .queue_position(true)
        .liquidity_consumption(true)
        .build()
        .unwrap();
    let latency = StaticLatencyModel::new(
        DurationNanos::default(),
        DurationNanos::new(5),
        DurationNanos::new(3),
        DurationNanos::new(2),
    )
    .with_event_latency(DurationNanos::new(4), DurationNanos::new(1));
    venue.latency_model = Some(LatencyModelHandle::new(latency));
    engine.add_venue(venue).unwrap();

    let first = future("ESZ21", BASE + 1_000_000_000);
    let second = future("ESH22", EXPIRY);
    let third = future("ESM22", IDLE_EXPIRY);
    let instruments = [first.id, second.id];
    for instrument in [first, second, third] {
        engine
            .add_instrument(&InstrumentAny::FuturesContract(instrument))
            .unwrap();
    }
    for n in 0..idle {
        let idle = future(&format!("IDLE{n:06}"), BASE + 2_000_000_000 + n as u64);
        engine
            .add_instrument(&InstrumentAny::FuturesContract(idle))
            .unwrap();
    }

    let log = Rc::new(RefCell::new(log));
    let memory = Rc::new(RefCell::new(memory));
    engine
        .kernel()
        .clock()
        .borrow_mut()
        .set_time_alert_ns(
            HOST_ALERT,
            UnixNanos::from(BASE + 1_255),
            Some(host_callback(&log)),
            None,
        )
        .unwrap();
    engine
        .add_strategy(ResumeWorkload::new(
            instruments,
            Rc::clone(&memory),
            Rc::clone(&log),
        ))
        .unwrap();
    Rig {
        engine,
        memory,
        log,
    }
}

fn run_batch(engine: &mut BacktestEngine, batch: &(Vec<Data>, u64), start: u64) {
    let (data, end) = batch;
    if !data.is_empty() {
        engine.add_data(data.clone(), None, true, false).unwrap();
        engine.sort_data();
    }
    engine
        .run(
            Some(UnixNanos::from(start)),
            Some(UnixNanos::from(*end)),
            Some("resume".to_string()),
            true,
        )
        .unwrap();
    engine.clear_data();
    engine.remove_settled_instruments();
}

fn batch_start(batches: &[(Vec<Data>, u64)], index: usize) -> u64 {
    if index == 0 {
        BASE
    } else {
        batches[index - 1].1
    }
}

/// Replaces every UUID inside `text` with a placeholder: UUIDs are unseeded and may differ after
/// a resume (and between any two runs: the venue's expiration close orders carry one in their
/// client order ID).
fn strip_uuids(text: &str) -> String {
    let bytes = text.as_bytes();
    let is_uuid_at = |start: usize| {
        bytes.len() >= start + 36
            && bytes[start..start + 36]
                .iter()
                .enumerate()
                .all(|(i, b)| match i {
                    8 | 13 | 18 | 23 => *b == b'-',
                    _ => b.is_ascii_hexdigit(),
                })
    };
    let mut out = String::with_capacity(text.len());
    let mut index = 0;
    while index < bytes.len() {
        if is_uuid_at(index) {
            out.push_str("<uuid>");
            index += 36;
        } else {
            let ch = text[index..].chars().next().unwrap();
            out.push(ch);
            index += ch.len_utf8();
        }
    }
    out
}

/// [`strip_uuids`] over every string of a JSON value, keys included.
fn without_uuids(value: Value) -> Value {
    match value {
        Value::String(text) if text.contains("funding_settlement:") => {
            Value::String("funding_settlement".to_string())
        }
        Value::String(text) => Value::String(strip_uuids(&text)),
        Value::Array(items) => Value::Array(items.into_iter().map(without_uuids).collect()),
        Value::Object(fields) => Value::Object(
            fields
                .into_iter()
                .map(|(key, field)| (strip_uuids(&key), without_uuids(field)))
                .collect(),
        ),
        other => other,
    }
}

/// What a finished run produced.
#[derive(Debug, PartialEq)]
struct Outcome {
    log: Vec<String>,
    canonical: Value,
    orders: Value,
    positions: Value,
    accounts: Value,
    iterations: usize,
}

fn finish(rig: Rig) -> Outcome {
    let Rig {
        mut engine, log, ..
    } = rig;
    engine.end().unwrap();
    let canonical = without_uuids(engine.get_canonical_result().unwrap().as_value().clone());
    let iterations = engine.get_result().iterations;
    let (orders, positions, accounts) = {
        let cache = engine.kernel().cache();
        let cache = cache.borrow();
        let mut orders: Vec<_> = cache
            .orders(None, None, None, None, None)
            .into_iter()
            .map(|order| order.clone())
            .collect();
        orders.sort_by_key(|order| order.client_order_id());
        let mut positions: Vec<_> = cache
            .positions(None, None, None, None, None)
            .into_iter()
            .map(|position| position.cloned())
            .collect();
        positions.sort_by_key(|position| position.id);
        let accounts: Vec<_> = cache
            .accounts(&nautilus_model::identifiers::AccountId::from("GLBX-001"))
            .into_iter()
            .map(|account| account.clone())
            .collect();
        (
            without_uuids(serde_json::to_value(&orders).unwrap()),
            without_uuids(serde_json::to_value(&positions).unwrap()),
            without_uuids(serde_json::to_value(&accounts).unwrap()),
        )
    };
    engine.dispose();
    let log = log.borrow().iter().map(|line| strip_uuids(line)).collect();
    Outcome {
        log,
        canonical,
        orders,
        positions,
        accounts,
        iterations,
    }
}

fn uninterrupted(idle: usize) -> Outcome {
    let data = workload_data(instruments());
    let batches = batches(&data);
    let mut rig = setup(idle, Memory::default(), Vec::new());
    for index in 0..batches.len() {
        run_batch(
            &mut rig.engine,
            &batches[index],
            batch_start(&batches, index),
        );
    }
    finish(rig)
}

fn instruments() -> [InstrumentId; 2] {
    [
        InstrumentId::from("ESZ21.GLBX"),
        InstrumentId::from("ESH22.GLBX"),
    ]
}

/// What a saved state carried across its boundary.
#[derive(Debug, Default, Clone, Copy)]
struct Crossing {
    inflight_commands: usize,
    held_events: usize,
    saved_engines: usize,
    removed_engines: usize,
    timers: usize,
    reservations: usize,
    open_orders: usize,
}

/// How a save at one boundary went.
struct Saved {
    crossing: Crossing,
    json: Vec<u8>,
    msgpack: Vec<u8>,
    export_secs: f64,
    import_secs: f64,
}

/// Runs to the end of batch `boundary`, saves, resumes in a fresh engine from the saved state
/// (round-tripped through JSON or MessagePack), and runs the rest.
fn resumed(idle: usize, boundary: usize, via_msgpack: bool) -> (Outcome, Saved) {
    let data = workload_data(instruments());
    let batches = batches(&data);
    let mut rig = setup(idle, Memory::default(), Vec::new());
    for index in 0..boundary {
        run_batch(
            &mut rig.engine,
            &batches[index],
            batch_start(&batches, index),
        );
    }
    let started = Instant::now();
    let state = rig.engine.export_state().unwrap();
    let export_secs = started.elapsed().as_secs_f64();
    let json = serde_json::to_vec(&state).unwrap();
    let msgpack = rmp_serde::to_vec_named(&state).unwrap();
    let venue = &state.venues[0].1;
    let crossing = Crossing {
        inflight_commands: venue.inflight_queue.len(),
        held_events: venue.outbound.events.len(),
        saved_engines: venue.matching_engines.engines.len(),
        removed_engines: venue.removed_engines.len(),
        timers: state
            .clocks
            .iter()
            .map(|(_, clock)| clock.timers.len())
            .sum(),
        reservations: state.risk_engine.reservations.len(),
        open_orders: state.cache.index.orders_open.len(),
    };
    let memory = rig.memory.borrow().clone();
    let log = rig.log.borrow().clone();
    rig.engine.dispose();
    drop(rig);

    let state: EngineState = if via_msgpack {
        rmp_serde::from_slice(&msgpack).unwrap()
    } else {
        serde_json::from_slice(&json).unwrap()
    };
    // Both encodings carry the same state
    assert_eq!(
        serde_json::to_vec(&state).unwrap(),
        json,
        "round trip changed the state"
    );

    let mut rig = setup(idle, memory, log);
    let host_log = Rc::clone(&rig.log);
    let callbacks = move |name: &str| (name == HOST_ALERT).then(|| host_callback(&host_log));
    let started = Instant::now();
    rig.engine.import_state(state, &callbacks).unwrap();
    let import_secs = started.elapsed().as_secs_f64();

    // Export → import → export is the identity
    let again = serde_json::to_vec(&rig.engine.export_state().unwrap()).unwrap();
    if again != json {
        let a: Value = serde_json::from_slice(&json).unwrap();
        let b: Value = serde_json::from_slice(&again).unwrap();
        panic!("re-export differs: {}", first_difference(&a, &b, ""));
    }

    for index in boundary..batches.len() {
        run_batch(
            &mut rig.engine,
            &batches[index],
            batch_start(&batches, index),
        );
    }
    (
        finish(rig),
        Saved {
            crossing,
            json,
            msgpack,
            export_secs,
            import_secs,
        },
    )
}

/// The path to the first difference between two JSON values.
fn first_difference(a: &Value, b: &Value, path: &str) -> String {
    match (a, b) {
        (Value::Object(x), Value::Object(y)) => {
            for (key, value) in x {
                match y.get(key) {
                    Some(other) if other == value => {}
                    Some(other) => return first_difference(value, other, &format!("{path}.{key}")),
                    None => return format!("{path}.{key} missing on the right"),
                }
            }
            format!("{path}: keys differ")
        }
        (Value::Array(x), Value::Array(y)) => {
            for (index, (left, right)) in x.iter().zip(y).enumerate() {
                if left != right {
                    return first_difference(left, right, &format!("{path}[{index}]"));
                }
            }
            format!("{path}: lengths {} and {}", x.len(), y.len())
        }
        _ => format!("{path}: {a} vs {b}"),
    }
}

fn assert_same(expected: &Outcome, actual: &Outcome, what: &str) {
    for (index, (want, got)) in expected.log.iter().zip(&actual.log).enumerate() {
        assert_eq!(got, want, "{what}: log line {index}");
    }
    assert_eq!(actual.log.len(), expected.log.len(), "{what}: log length");
    for (name, want, got) in [
        ("orders", &expected.orders, &actual.orders),
        ("positions", &expected.positions, &actual.positions),
        ("accounts", &expected.accounts, &actual.accounts),
        ("canonical", &expected.canonical, &actual.canonical),
    ] {
        assert!(
            want == got,
            "{what}: {name} differ at {}",
            first_difference(want, got, "")
        );
    }
    assert_eq!(actual.iterations, expected.iterations, "{what}: iterations");
}

#[test]
fn test_resume_at_every_boundary_gives_the_uninterrupted_run() {
    let reference = uninterrupted(0);

    // The workload exercises what a resume must carry
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
    assert!(count("timer") > 20, "timers: {}", count("timer"));
    assert!(count("host") == 1, "host alert: {}", count("host"));
    assert!(count("Rejected") > 0, "no venue rejection");
    let partial = reference
        .log
        .iter()
        .filter(|line| line.contains(" Filled ") && line.contains(" 1 @ "))
        .count();
    assert!(partial > 0, "no partial fill");

    let boundaries = batches(&workload_data(instruments())).len();
    let mut crossed = Crossing::default();
    for boundary in 1..=boundaries {
        for via_msgpack in [false, true] {
            let (outcome, saved) = resumed(0, boundary, via_msgpack);
            let what = format!(
                "boundary {boundary} via {}",
                if via_msgpack { "msgpack" } else { "json" }
            );
            assert_same(&reference, &outcome, &what);
            if !via_msgpack {
                println!(
                    "boundary {boundary}: export {:.3} ms, import {:.3} ms, json {} B, msgpack {} B, {:?}",
                    saved.export_secs * 1e3,
                    saved.import_secs * 1e3,
                    saved.json.len(),
                    saved.msgpack.len(),
                    saved.crossing,
                );
                let c = saved.crossing;
                crossed.inflight_commands += c.inflight_commands;
                crossed.held_events += c.held_events;
                crossed.removed_engines += c.removed_engines;
                crossed.saved_engines = crossed.saved_engines.max(c.saved_engines);
                crossed.timers = crossed.timers.max(c.timers);
                crossed.reservations += c.reservations;
                crossed.open_orders += c.open_orders;
            }
        }
    }
    // Commands and events were in flight across some cut, resting orders across others, and the
    // expired market's engine was released before one
    assert!(crossed.inflight_commands > 0, "{crossed:?}");
    assert!(crossed.held_events > 0, "{crossed:?}");
    assert!(crossed.open_orders > 0, "{crossed:?}");
    assert!(crossed.removed_engines > 0, "{crossed:?}");
    assert!(
        crossed.saved_engines > 0 && crossed.timers > 0,
        "{crossed:?}"
    );
}

#[test]
fn test_resume_with_idle_instruments_stays_flat() {
    const IDLE: usize = 50_000;
    let boundary = 3;
    let (_, small) = resumed(0, boundary, false);
    let reference = uninterrupted(IDLE);
    let (outcome, large) = resumed(IDLE, boundary, true);
    assert_same(&reference, &outcome, "idle instruments");
    println!(
        "{IDLE} idle instruments at boundary {boundary}: export {:.3} ms vs {:.3} ms, import {:.3} ms vs {:.3} ms, json {} B vs {} B, msgpack {} B vs {} B",
        large.export_secs * 1e3,
        small.export_secs * 1e3,
        large.import_secs * 1e3,
        small.import_secs * 1e3,
        large.json.len(),
        small.json.len(),
        large.msgpack.len(),
        small.msgpack.len()
    );
    let state: Value = serde_json::from_slice(&large.json).unwrap();
    let mut sizes: Vec<(String, usize)> = Vec::new();
    for (key, value) in state.as_object().unwrap() {
        if key == "cache" || key == "venues" {
            for (inner, value) in value
                .as_object()
                .map(|o| o.iter().collect::<Vec<_>>())
                .unwrap_or_default()
            {
                sizes.push((format!("{key}.{inner}"), value.to_string().len()));
            }
            if let Some(venues) = value.as_array() {
                for venue in venues {
                    for (inner, value) in venue[1].as_object().unwrap() {
                        sizes.push((format!("venue.{inner}"), value.to_string().len()));
                    }
                }
            }
        } else {
            sizes.push((key.clone(), value.to_string().len()));
        }
    }
    sizes.sort_by_key(|a| std::cmp::Reverse(a.1));
    println!(
        "largest parts of the saved state: {:?}",
        &sizes[..8.min(sizes.len())]
    );
    let engines = &state["venues"][0][1]["matching_engines"]["engines"];
    println!(
        "matching engines saved: {}",
        engines.as_array().unwrap().len()
    );
    // The idle instruments' registration lives in setup, and the replay never touched them: the
    // saved state does not grow with them
    assert!(
        large.json.len() < small.json.len() + 1_024,
        "the state grew with idle instruments: {} B vs {} B",
        large.json.len(),
        small.json.len()
    );
}
