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

//! Holds the order events a simulated venue emits until they would reach the trading node.
//!
//! A [`LatencyModel`] delays each command on its way to the venue; its
//! [`LatencyModel::get_event_latency`] leg delays each event on its way back. The venue emits an
//! event at its own instant, and [`OutboundEvents`] holds it until that instant plus its latency,
//! never ahead of an earlier event of the same order, so the node receives an order's events in
//! the order the venue emitted them.

use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

use ahash::AHashMap;
use nautilus_common::clock::Clock;
use nautilus_core::{DurationNanos, UnixNanos};
use nautilus_model::{events::OrderEventAny, identifiers::ClientOrderId};

use crate::models::latency::{LatencyModel, LatencyModelHandle};

/// What [`OutboundEvents::admit`] did with an event.
#[derive(Debug)]
pub enum Admitted {
    /// Deliver the event now: its latency is zero and none of its order's events is held.
    Deliver(Box<OrderEventAny>),
    /// The event is held until the returned release instant.
    Held(UnixNanos),
}

#[derive(Debug, Default)]
struct Queue {
    /// Held events by `(release instant, admission sequence)`.
    events: BTreeMap<(UnixNanos, u64), OrderEventAny>,
    next_seq: u64,
    /// The latest release instant held for each order.
    order_release: AHashMap<ClientOrderId, UnixNanos>,
}

/// Shared queue of a venue's order events on their way to the trading node.
#[derive(Debug, Clone, Default)]
pub struct OutboundEvents {
    queue: Rc<RefCell<Queue>>,
}

impl OutboundEvents {
    /// Admits `event`, emitted by the venue at `ts_now`, with venue-to-node `latency`.
    ///
    /// Returns it to deliver at once when `latency` is zero and none of its order's earlier
    /// events is still held. Otherwise holds it until the later of `ts_now + latency` and the
    /// release instant of its order's last held event, and returns that instant.
    pub fn admit(
        &self,
        event: OrderEventAny,
        ts_now: UnixNanos,
        latency: DurationNanos,
    ) -> Admitted {
        let mut queue = self.queue.borrow_mut();
        let client_order_id = event.client_order_id();
        let floor = queue.order_release.get(&client_order_id).copied();

        if latency == DurationNanos::ZERO && floor.is_none() {
            return Admitted::Deliver(Box::new(event));
        }

        let release = floor.map_or(ts_now + latency, |floor| floor.max(ts_now + latency));
        let seq = queue.next_seq;
        queue.next_seq += 1;
        queue.events.insert((release, seq), event);
        queue.order_release.insert(client_order_id, release);
        Admitted::Held(release)
    }

    /// Takes the next held event whose release instant is at or before `ts_now`, in release
    /// then admission order.
    pub fn pop_due(&self, ts_now: UnixNanos) -> Option<OrderEventAny> {
        let mut queue = self.queue.borrow_mut();
        let entry = queue.events.first_entry()?;
        let (release, _) = *entry.key();
        if release > ts_now {
            return None;
        }
        let event = entry.remove();
        let client_order_id = event.client_order_id();
        if queue
            .order_release
            .get(&client_order_id)
            .is_some_and(|last| *last <= release)
        {
            queue.order_release.remove(&client_order_id);
        }
        Some(event)
    }

    /// Returns whether any held event is due at or before `ts_now`.
    #[must_use]
    pub fn has_due(&self, ts_now: UnixNanos) -> bool {
        self.next_release().is_some_and(|release| release <= ts_now)
    }

    /// Returns the earliest release instant held, if any.
    #[must_use]
    pub fn next_release(&self) -> Option<UnixNanos> {
        self.queue
            .borrow()
            .events
            .first_key_value()
            .map(|((release, _), _)| *release)
    }

    /// Returns the latest release instant held, if any.
    #[must_use]
    pub fn last_release(&self) -> Option<UnixNanos> {
        self.queue
            .borrow()
            .events
            .last_key_value()
            .map(|((release, _), _)| *release)
    }

    /// Returns how many events are held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.queue.borrow().events.len()
    }

    /// Returns whether no event is held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queue.borrow().events.is_empty()
    }

    /// Discards every held event, when the venue discards its queues.
    pub fn clear(&self) {
        let mut queue = self.queue.borrow_mut();
        queue.events.clear();
        queue.order_release.clear();
    }
}

/// An [`OutboundEvents`]'s held events, for saving a backtest and resuming it.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OutboundEventsState {
    /// The held events in release then admission order, each with its release instant and
    /// admission sequence.
    pub events: Vec<(UnixNanos, u64, OrderEventAny)>,
    /// The next admission sequence.
    pub next_seq: u64,
    /// The latest release instant held for each order, by client order ID.
    pub order_release: Vec<(ClientOrderId, UnixNanos)>,
}

impl OutboundEvents {
    /// Returns the held events.
    #[must_use]
    pub fn export_state(&self) -> OutboundEventsState {
        let queue = self.queue.borrow();
        let Queue {
            events,
            next_seq,
            order_release,
        } = &*queue;
        let mut order_release: Vec<(ClientOrderId, UnixNanos)> = order_release
            .iter()
            .map(|(client_order_id, release)| (*client_order_id, *release))
            .collect();
        order_release.sort_unstable();
        OutboundEventsState {
            events: events
                .iter()
                .map(|((release, seq), event)| (*release, *seq, event.clone()))
                .collect(),
            next_seq: *next_seq,
            order_release,
        }
    }

    /// Replaces the held events with `state`'s.
    pub fn import_state(&self, state: OutboundEventsState) {
        let OutboundEventsState {
            events,
            next_seq,
            order_release,
        } = state;
        let mut queue = self.queue.borrow_mut();
        queue.events = events
            .into_iter()
            .map(|(release, seq, event)| ((release, seq), event))
            .collect();
        queue.next_seq = next_seq;
        queue.order_release = order_release.into_iter().collect();
    }
}

/// Returns an event handler that admits each event into `outbound` with the latency `model`
/// gives it at the instant `clock` reads: an event to deliver at once goes to `deliver`, and the
/// release instant of each held event goes to `on_held`, so the venue can wake to release it.
pub fn delaying_event_handler(
    outbound: OutboundEvents,
    model: LatencyModelHandle,
    clock: Rc<RefCell<dyn Clock>>,
    deliver: Rc<dyn Fn(OrderEventAny)>,
    on_held: Rc<dyn Fn(UnixNanos)>,
) -> Rc<dyn Fn(OrderEventAny)> {
    Rc::new(move |event: OrderEventAny| {
        let latency = model.get_event_latency(&event);
        let ts_now = clock.borrow().timestamp_ns();
        match outbound.admit(event, ts_now, latency) {
            Admitted::Deliver(event) => deliver(*event),
            Admitted::Held(release) => on_held(release),
        }
    })
}

#[cfg(test)]
mod tests {
    use nautilus_model::events::{OrderAccepted, OrderCanceled, order::spec::OrderFilledSpec};
    use rstest::rstest;

    use super::*;

    fn accepted(id: &str) -> OrderEventAny {
        OrderEventAny::Accepted(OrderAccepted {
            client_order_id: ClientOrderId::from(id),
            ..Default::default()
        })
    }

    fn filled(id: &str) -> OrderEventAny {
        OrderEventAny::Filled(
            OrderFilledSpec::builder()
                .client_order_id(ClientOrderId::from(id))
                .build(),
        )
    }

    fn canceled(id: &str) -> OrderEventAny {
        OrderEventAny::Canceled(OrderCanceled {
            client_order_id: ClientOrderId::from(id),
            ..Default::default()
        })
    }

    fn drain(outbound: &OutboundEvents, ts_now: u64) -> Vec<(String, &'static str)> {
        std::iter::from_fn(|| outbound.pop_due(UnixNanos::from(ts_now)))
            .map(|event| {
                let kind = match event {
                    OrderEventAny::Accepted(_) => "accepted",
                    OrderEventAny::Filled(_) => "filled",
                    OrderEventAny::Canceled(_) => "canceled",
                    _ => "other",
                };
                (event.client_order_id().to_string(), kind)
            })
            .collect()
    }

    #[rstest]
    fn test_zero_latency_delivers_at_once() {
        let outbound = OutboundEvents::default();
        let admitted = outbound.admit(accepted("O-1"), UnixNanos::from(10), DurationNanos::ZERO);
        assert!(matches!(admitted, Admitted::Deliver(_)));
        assert!(outbound.is_empty());
    }

    #[rstest]
    fn test_held_events_release_at_their_instant_in_order() {
        let outbound = OutboundEvents::default();
        let t = UnixNanos::from(100);
        assert!(matches!(
            outbound.admit(accepted("O-1"), t, DurationNanos::new(2)),
            Admitted::Held(release) if release == UnixNanos::from(102)
        ));
        assert!(matches!(
            outbound.admit(filled("O-1"), t, DurationNanos::new(7)),
            Admitted::Held(release) if release == UnixNanos::from(107)
        ));
        assert!(matches!(
            outbound.admit(accepted("O-2"), t, DurationNanos::new(2)),
            Admitted::Held(release) if release == UnixNanos::from(102)
        ));

        assert_eq!(outbound.next_release(), Some(UnixNanos::from(102)));
        assert_eq!(outbound.last_release(), Some(UnixNanos::from(107)));
        assert!(drain(&outbound, 101).is_empty());
        assert_eq!(
            drain(&outbound, 102),
            vec![
                ("O-1".to_string(), "accepted"),
                ("O-2".to_string(), "accepted")
            ]
        );
        assert!(!outbound.has_due(UnixNanos::from(106)));
        assert_eq!(drain(&outbound, 107), vec![("O-1".to_string(), "filled")]);
        assert!(outbound.is_empty());
    }

    #[rstest]
    fn test_an_order_never_overtakes_its_held_events() {
        let outbound = OutboundEvents::default();
        // A fill held 7 ns, then a cancel ack a nanosecond later held only 2: the cancel waits
        assert!(matches!(
            outbound.admit(filled("O-1"), UnixNanos::from(100), DurationNanos::new(7)),
            Admitted::Held(_)
        ));
        assert!(matches!(
            outbound.admit(canceled("O-1"), UnixNanos::from(101), DurationNanos::new(2)),
            Admitted::Held(release) if release == UnixNanos::from(107)
        ));
        // A zero-latency event of the same order is held behind them too
        assert!(matches!(
            outbound.admit(accepted("O-1"), UnixNanos::from(101), DurationNanos::ZERO),
            Admitted::Held(release) if release == UnixNanos::from(107)
        ));
        // Another order is not held behind them
        assert!(matches!(
            outbound.admit(accepted("O-2"), UnixNanos::from(101), DurationNanos::ZERO),
            Admitted::Deliver(_)
        ));

        assert!(drain(&outbound, 106).is_empty());
        assert_eq!(
            drain(&outbound, 107),
            vec![
                ("O-1".to_string(), "filled"),
                ("O-1".to_string(), "canceled"),
                ("O-1".to_string(), "accepted")
            ]
        );
        // Once released, the order's next zero-latency event goes at once again
        assert!(matches!(
            outbound.admit(canceled("O-1"), UnixNanos::from(108), DurationNanos::ZERO),
            Admitted::Deliver(_)
        ));
    }

    #[rstest]
    fn test_clear_discards_every_held_event() {
        let outbound = OutboundEvents::default();
        outbound.admit(filled("O-1"), UnixNanos::from(1), DurationNanos::new(5));
        outbound.clear();
        assert!(outbound.is_empty());
        assert!(matches!(
            outbound.admit(accepted("O-1"), UnixNanos::from(2), DurationNanos::ZERO),
            Admitted::Deliver(_)
        ));
    }
}
