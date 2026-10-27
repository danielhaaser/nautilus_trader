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

//! Deterministic clock for controlled time advancement.

use std::{
    collections::{BTreeMap, BinaryHeap},
    ops::Deref,
    sync::Arc,
};

use nautilus_core::{
    AtomicTime, DurationNanos, UUID4, UnixNanos, correctness::check_predicate_true,
    string::formatting::Separable,
};
use ustr::Ustr;

use super::{
    CallbackRegistry, Clock, replace_existing_timer, validate_and_prepare_schedule,
    validate_and_prepare_time_alert, validate_and_prepare_timer,
};
use crate::timer::{
    ScheduledTimeEvent, TimeEvent, TimeEventCallback, TimeEventHandler, TimerInterval,
    TimerSchedule, VirtualTimer, VirtualTimerState, create_valid_interval,
};

/// One timer of a [`VirtualClockState`]: its state, and whether it fires through a callback
/// registered under its name (otherwise through the clock's default handler).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClockTimerState {
    /// The timer's state.
    pub timer: VirtualTimerState,
    /// Whether a callback is registered under the timer's name.
    pub named_callback: bool,
}

/// A [`VirtualClock`]'s complete state as plain data, for saving a backtest and resuming it.
///
/// Callbacks are closures, so they are named rather than saved: an import asks its resolver for
/// the callback of each name listed.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VirtualClockState {
    /// The clock's time.
    pub time_ns: UnixNanos,
    /// The timers, in name order.
    pub timers: Vec<ClockTimerState>,
    /// The names, sorted, of callbacks still registered for no timer: a later timer of the same
    /// name set without a callback fires through it.
    pub stale_callbacks: Vec<Ustr>,
    /// Whether a default handler is registered.
    pub default_handler: bool,
}

/// A deterministic clock for controlled time advancement.
///
/// The clock stores a manual timestamp, schedules [`VirtualTimer`] instances, and returns due events
/// without waiting for wall-clock time.
///
/// # Threading
///
/// This clock is thread-affine; use it only from the thread that created it.
#[derive(Debug)]
pub struct VirtualClock {
    time: AtomicTime,
    timers: BTreeMap<Ustr, VirtualTimer>,
    pub(super) timer_queue: BinaryHeap<ScheduledTimeEvent>,
    callbacks: CallbackRegistry,
}

impl VirtualClock {
    /// Creates a virtual clock at the UNIX epoch with no timers or callbacks.
    #[must_use]
    pub fn new() -> Self {
        Self {
            time: AtomicTime::new(false, UnixNanos::default()),
            timers: BTreeMap::new(),
            timer_queue: BinaryHeap::new(),
            callbacks: CallbackRegistry::new(),
        }
    }

    /// Advances active timers through `to_time_ns` and returns their due events.
    ///
    /// Events at `to_time_ns` are included and returned in ascending order by event timestamp and
    /// timer name. If `set_time` is `true`, the stored clock timestamp is also set to `to_time_ns`;
    /// otherwise the stored timestamp remains unchanged.
    ///
    /// # Warnings
    ///
    /// Logs a warning if at least 1,000,000 time events are allocated during advancement.
    ///
    /// # Panics
    ///
    /// Panics if `to_time_ns` is less than the current internal clock time.
    pub fn advance_time(&mut self, to_time_ns: UnixNanos, set_time: bool) -> Vec<TimeEvent> {
        const WARN_TIME_EVENTS_THRESHOLD: usize = 1_000_000;

        let from_time_ns = self.time.get_time_ns();

        assert!(
            to_time_ns >= from_time_ns,
            "Invariant: time must be non-decreasing, `to_time_ns` {to_time_ns} < `from_time_ns` {from_time_ns}"
        );

        if set_time {
            self.time.set_time(to_time_ns);
        }

        let mut events: Vec<TimeEvent> = Vec::new();

        while self
            .timer_queue
            .peek()
            .is_some_and(|entry| entry.0.ts_event <= to_time_ns)
        {
            let entry = self
                .timer_queue
                .pop()
                .expect("timer queue peeked Some but pop returned None");

            let Some((event, next_event)) = self.advance_timer_from_entry(&entry.0) else {
                continue;
            };

            events.push(event);

            if let Some(next_event) = next_event {
                self.timer_queue.push(next_event);
            }
        }

        self.compact_timer_queue_if_needed();

        if events.len() >= WARN_TIME_EVENTS_THRESHOLD {
            log::warn!(
                "Allocated {} time events during clock advancement from {} to {}, \
                 consider stopping the timer between large time ranges with no data points",
                events.len().separate_with_commas(),
                from_time_ns,
                to_time_ns
            );
        }

        events.sort_by(|a, b| {
            a.ts_event
                .cmp(&b.ts_event)
                .then_with(|| a.name.cmp(&b.name))
        });

        events
    }

    /// Returns the instant of the earliest entry in the timer queue, if any.
    ///
    /// An entry can be stale (its timer was cancelled or replaced), so this is a lower bound on
    /// the next event: when it is `None` or later than `to_time_ns`, [`Self::advance_time`] to
    /// `to_time_ns` returns no event.
    #[must_use]
    pub fn next_scheduled_ns(&self) -> Option<UnixNanos> {
        self.timer_queue.peek().map(|entry| entry.0.ts_event)
    }

    /// Matches time events with their registered callbacks, preserving input order.
    ///
    /// A named callback takes precedence over the default callback for each event.
    ///
    /// # Panics
    ///
    /// Panics if an event has neither a named nor default callback.
    #[must_use]
    pub fn match_handlers(&self, events: Vec<TimeEvent>) -> Vec<TimeEventHandler> {
        events
            .into_iter()
            .map(|event| self.callbacks.get_handler(event))
            .collect()
    }

    fn replace_existing_timer_if_needed(&mut self, name: &Ustr) {
        replace_existing_timer(&mut self.timers, name);
        self.compact_timer_queue_if_needed();
    }

    fn insert_timer(&mut self, timer: VirtualTimer) {
        self.timer_queue.push(Self::scheduled_event(&timer));
        self.timers.insert(timer.name, timer);
        self.compact_timer_queue_if_needed();
    }

    fn advance_timer_from_entry(
        &mut self,
        entry: &TimeEvent,
    ) -> Option<(TimeEvent, Option<ScheduledTimeEvent>)> {
        let timer = self.timers.get_mut(&entry.name)?;
        if timer.next_time_ns() != entry.ts_event {
            return None;
        }

        let Some((event, _)) = timer.next() else {
            self.timers.remove(&entry.name);
            return None;
        };

        let next_entry = if timer.is_expired() {
            self.timers.remove(&entry.name);
            None
        } else {
            Some(Self::scheduled_event(timer))
        };

        Some((event, next_entry))
    }

    fn compact_timer_queue_if_needed(&mut self) {
        if self.timer_queue.len() > self.timers.len().saturating_mul(2) {
            self.compact_timer_queue();
        }
    }

    fn compact_timer_queue(&mut self) {
        self.timer_queue = self.timers.values().map(Self::scheduled_event).collect();
    }

    fn scheduled_event(timer: &VirtualTimer) -> ScheduledTimeEvent {
        ScheduledTimeEvent::new(TimeEvent::new(
            timer.name,
            UUID4::new(),
            timer.next_time_ns(),
            timer.next_time_ns(),
        ))
    }
    fn register_timer(
        &mut self,
        name: Ustr,
        interval: TimerInterval,
        start_time_ns: UnixNanos,
        stop_time_ns: Option<UnixNanos>,
        callback: Option<TimeEventCallback>,
        fire_immediately: bool,
    ) -> anyhow::Result<()> {
        check_predicate_true(
            callback.is_some() | self.callbacks.has_any_callback(&name),
            "No callbacks provided",
        )?;

        self.replace_existing_timer_if_needed(&name);
        if let Some(callback) = callback {
            self.callbacks.register_callback(name, callback);
        }

        let timer = VirtualTimer::new(
            name,
            interval,
            start_time_ns,
            stop_time_ns,
            fire_immediately,
        );
        self.insert_timer(timer);

        Ok(())
    }
}

/// A family of timers a [`VirtualClock`]'s owner arms again itself when it is rebuilt (setup's
/// alerts): every timer whose name starts with `prefix`, except those named in `armed_since`
/// (armed after setup, saved like any other). Such a timer does not change until it fires, which
/// removes it, so a rebuilt clock holds every one the saved clock still held, and those that fired
/// are exactly those due at or before the saved clock's time.
#[derive(Clone, Copy, Debug)]
pub struct RebuiltTimers<'a> {
    /// The name prefix of the family.
    pub prefix: &'a str,
    /// The family's timers armed after setup.
    pub armed_since: &'a ahash::AHashSet<Ustr>,
    /// The family's timers cancelled after setup.
    pub cancelled_since: &'a ahash::AHashSet<Ustr>,
}

impl RebuiltTimers<'_> {
    /// The bounds of the names starting with `prefix`: `[prefix, upper)`.
    fn bounds(&self) -> (Ustr, Ustr) {
        let mut upper = self.prefix.as_bytes().to_vec();
        if let Some(last) = upper.last_mut() {
            *last = last.saturating_add(1);
        }
        (
            Ustr::from(self.prefix),
            Ustr::from(String::from_utf8_lossy(&upper).as_ref()),
        )
    }

    fn rebuilt(&self, name: &Ustr) -> bool {
        name.as_str().starts_with(self.prefix) && !self.armed_since.contains(name)
    }
}

impl VirtualClock {
    /// Returns the clock's complete state.
    ///
    /// The timers of `rebuilt` are not listed (the owner arms them again) and the clock does not
    /// visit them: the state's size and the export's time do not grow with them. `keep_stale`
    /// selects which callbacks registered for no timer are listed: a callback whose name is always
    /// set again with its callback can never be reached, and need not be.
    ///
    /// # Errors
    ///
    /// Returns an error if a timer runs on a [`TimerSchedule`], or a live timer is due at or
    /// before the clock's time (its event has not fired: the clock is not at a boundary).
    pub fn export_state(
        &self,
        rebuilt: Option<RebuiltTimers<'_>>,
        keep_stale: &dyn Fn(&str) -> bool,
    ) -> anyhow::Result<VirtualClockState> {
        let Self {
            time,
            timers,
            timer_queue: _, // Derived: rebuilt from the timers (stale entries fire nothing)
            callbacks,
        } = self;
        let super::CallbackRegistry {
            default_callback,
            callbacks,
        } = callbacks;
        let time_ns = time.get_time_ns();
        let listed: Vec<&VirtualTimer> = match rebuilt {
            None => timers.values().collect(),
            Some(rebuilt) => {
                let (lower, upper) = rebuilt.bounds();
                let mut listed: Vec<&VirtualTimer> = timers
                    .range(..lower)
                    .chain(timers.range(upper..))
                    .map(|(_, timer)| timer)
                    .chain(
                        rebuilt
                            .armed_since
                            .iter()
                            .filter_map(|name| timers.get(name)),
                    )
                    .collect();
                listed.sort_unstable_by_key(|timer| timer.name);
                listed
            }
        };
        let timer_states = listed
            .into_iter()
            .map(|timer| {
                anyhow::ensure!(
                    timer.is_expired() || timer.next_time_ns() > time_ns,
                    "timer {} is due at {} and the clock is at {time_ns}: not at a boundary",
                    timer.name,
                    timer.next_time_ns()
                );
                Ok(ClockTimerState {
                    timer: timer.export_state()?,
                    named_callback: callbacks.contains_key(&timer.name),
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut stale_callbacks: Vec<Ustr> = callbacks
            .keys()
            .filter(|name| {
                !rebuilt.is_some_and(|rebuilt| rebuilt.rebuilt(name))
                    && keep_stale(name.as_str())
                    && !timers.contains_key(*name)
            })
            .copied()
            .collect();
        stale_callbacks.sort_unstable();
        Ok(VirtualClockState {
            time_ns,
            timers: timer_states,
            stale_callbacks,
            default_handler: default_callback.is_some(),
        })
    }

    /// Replaces the clock's time, timers and named callbacks with `state`'s.
    ///
    /// The clock must be the one its owner's setup built. Every timer setup armed is dropped,
    /// except those of `rebuilt` the saved clock still held (not cancelled since, and not due by
    /// the saved clock's time); `state`'s timers are armed. `resolve` gives the callback of each
    /// name `state` lists (each timer with a named callback, then each stale callback), told the
    /// callback registered under the name, if any; the callbacks of the timers kept stay as setup
    /// registered them. The default handler is not touched: the clock's owner registers it.
    ///
    /// # Errors
    ///
    /// Returns an error if the clock's default handler is not as `state` says, a timer is
    /// invalid, or `resolve` fails.
    pub fn import_state(
        &mut self,
        state: VirtualClockState,
        rebuilt: Option<RebuiltTimers<'_>>,
        resolve: &mut dyn FnMut(
            Ustr,
            Option<TimeEventCallback>,
        ) -> anyhow::Result<TimeEventCallback>,
    ) -> anyhow::Result<()> {
        let VirtualClockState {
            time_ns,
            timers,
            stale_callbacks,
            default_handler,
        } = state;
        anyhow::ensure!(
            self.callbacks.default_callback.is_some() == default_handler,
            "the clock {} a default handler, the saved clock {}",
            if default_handler { "has no" } else { "has" },
            if default_handler {
                "had one"
            } else {
                "had none"
            }
        );
        match rebuilt {
            None => {
                self.timers.clear();
                self.timer_queue.clear();
            }
            Some(rebuilt) => {
                // Setup's other timers go; of its rebuilt family, those that fired in the saved run
                // (due by its time) and those cancelled since go. The queue keeps entries for
                // timers dropped (stale entries fire nothing).
                let (lower, upper) = rebuilt.bounds();
                let others: Vec<Ustr> = self
                    .timers
                    .range(..lower)
                    .chain(self.timers.range(upper..))
                    .map(|(name, _)| *name)
                    .collect();
                for name in others {
                    self.timers.remove(&name);
                }
                while self
                    .timer_queue
                    .peek()
                    .is_some_and(|entry| entry.0.ts_event <= time_ns)
                {
                    let Some(entry) = self.timer_queue.pop() else {
                        break;
                    };
                    let fired = self
                        .timers
                        .get(&entry.0.name)
                        .is_some_and(|timer| timer.next_time_ns() == entry.0.ts_event);
                    if fired {
                        self.timers.remove(&entry.0.name);
                    }
                }
                for name in rebuilt.cancelled_since {
                    self.timers.remove(name);
                }
            }
        }
        for ClockTimerState {
            timer,
            named_callback,
        } in timers
        {
            let timer = VirtualTimer::from_state(timer)?;
            if named_callback {
                let before = self.callbacks.callbacks.get(&timer.name).cloned();
                let callback = resolve(timer.name, before)?;
                self.callbacks.register_callback(timer.name, callback);
            } else {
                anyhow::ensure!(
                    default_handler,
                    "timer {} has no callback and the clock no default handler",
                    timer.name
                );
                self.callbacks.callbacks.remove(&timer.name);
            }
            self.timer_queue.push(Self::scheduled_event(&timer));
            self.timers.insert(timer.name, timer);
        }
        for name in stale_callbacks {
            let before = self.callbacks.callbacks.get(&name).cloned();
            let callback = resolve(name, before)?;
            self.callbacks.register_callback(name, callback);
        }
        self.time.set_time(time_ns);
        Ok(())
    }
}

impl Default for VirtualClock {
    /// Creates a new default [`VirtualClock`] instance.
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for VirtualClock {
    type Target = AtomicTime;

    fn deref(&self) -> &Self::Target {
        &self.time
    }
}

impl Clock for VirtualClock {
    fn timestamp_ns(&self) -> UnixNanos {
        self.time.get_time_ns()
    }

    fn timestamp_us(&self) -> u64 {
        self.time.get_time_us()
    }

    fn timestamp_ms(&self) -> u64 {
        self.time.get_time_ms()
    }

    fn timestamp(&self) -> f64 {
        self.time.get_time()
    }

    fn timer_names(&self) -> Vec<&str> {
        self.timers
            .iter()
            .filter(|(_, timer)| !timer.is_expired())
            .map(|(k, _)| k.as_str())
            .collect()
    }

    fn timer_count(&self) -> usize {
        self.timers
            .iter()
            .filter(|(_, timer)| !timer.is_expired())
            .count()
    }

    fn timer_exists(&self, name: &Ustr) -> bool {
        self.timers
            .get(name)
            .is_some_and(|timer| !timer.is_expired())
    }

    fn register_default_handler(&mut self, callback: TimeEventCallback) {
        self.callbacks.register_default_handler(callback);
    }

    fn cancel_default_handler(&mut self) {
        self.callbacks.cancel_default_handler();
    }

    fn cancel_callbacks(&mut self) {
        self.callbacks.clear();
    }

    fn set_time_alert_ns(
        &mut self,
        name: &str,
        alert_time_ns: UnixNanos,
        callback: Option<TimeEventCallback>,
        allow_past: Option<bool>,
    ) -> anyhow::Result<()> {
        let ts_now = self.get_time_ns();
        let (name, alert_time_ns) =
            validate_and_prepare_time_alert(name, alert_time_ns, allow_past, ts_now)?;

        check_predicate_true(
            callback.is_some() | self.callbacks.has_any_callback(&name),
            "No callbacks provided",
        )?;

        self.replace_existing_timer_if_needed(&name);

        if let Some(callback) = callback {
            self.callbacks.register_callback(name, callback);
        }

        // Safe to calculate interval now that we've ensured alert_time_ns >= ts_now
        let interval_ns = create_valid_interval(alert_time_ns - ts_now);
        let fire_immediately = alert_time_ns == ts_now;

        let timer = VirtualTimer::new(
            name,
            interval_ns,
            ts_now,
            Some(alert_time_ns),
            fire_immediately,
        );
        self.insert_timer(timer);

        Ok(())
    }

    fn set_timer_ns(
        &mut self,
        name: &str,
        interval_ns: DurationNanos,
        start_time_ns: Option<UnixNanos>,
        stop_time_ns: Option<UnixNanos>,
        callback: Option<TimeEventCallback>,
        allow_past: Option<bool>,
        fire_immediately: Option<bool>,
    ) -> anyhow::Result<()> {
        let (name, start_time_ns, stop_time_ns, _, fire_immediately) = validate_and_prepare_timer(
            name,
            interval_ns,
            start_time_ns,
            stop_time_ns,
            allow_past,
            fire_immediately,
            self.get_time_ns(),
        )?;
        self.register_timer(
            name,
            TimerInterval::Fixed(create_valid_interval(interval_ns)),
            start_time_ns,
            stop_time_ns,
            callback,
            fire_immediately,
        )
    }

    fn set_timer_schedule(
        &mut self,
        name: &str,
        schedule: Arc<dyn TimerSchedule>,
        stop_time_ns: Option<UnixNanos>,
        callback: Option<TimeEventCallback>,
        allow_past: Option<bool>,
        fire_immediately: Option<bool>,
    ) -> anyhow::Result<()> {
        let start_time_ns = schedule.start_time_ns();
        let interval = TimerInterval::Schedule(schedule);
        let (name, start_time_ns, stop_time_ns, _, fire_immediately) =
            validate_and_prepare_schedule(
                name,
                &interval,
                Some(start_time_ns),
                stop_time_ns,
                allow_past,
                fire_immediately,
                self.get_time_ns(),
            )?;
        self.register_timer(
            name,
            interval,
            start_time_ns,
            stop_time_ns,
            callback,
            fire_immediately,
        )
    }

    fn next_time_ns(&self, name: &str) -> Option<UnixNanos> {
        self.timers
            .get(&Ustr::from(name))
            .filter(|timer| !timer.is_expired())
            .map(VirtualTimer::next_time_ns)
    }

    fn cancel_timer(&mut self, name: &str) {
        let timer = self.timers.remove(&Ustr::from(name));
        if let Some(mut timer) = timer {
            timer.cancel();
        }

        self.compact_timer_queue_if_needed();
    }

    fn cancel_timers(&mut self) {
        for timer in &mut self.timers.values_mut() {
            timer.cancel();
        }

        self.timers.clear();
        self.timer_queue.clear();
    }

    fn reset(&mut self) {
        self.time = AtomicTime::new(false, UnixNanos::default());
        self.timers = BTreeMap::new();
        self.timer_queue = BinaryHeap::new();
        self.callbacks.clear();
    }
}
