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

//! Type-safe topic routing for pub/sub messaging.
//!
//! This module provides [`TopicRouter<T>`] for routing messages of a specific type
//! to subscribed handlers based on topic patterns.

use std::{
    cmp::Ordering,
    fmt::Debug,
    hash::{Hash, Hasher},
};

use ahash::AHashMap;
use smallvec::SmallVec;
use ustr::Ustr;

use super::{
    matching::is_matching_backtracking,
    mstr::{MStr, Pattern, Topic},
    typed_handler::TypedHandler,
};

/// A typed subscription for pub/sub messaging.
///
/// Associates a handler with a topic pattern and priority.
#[derive(Clone)]
pub struct TypedSubscription<T: 'static> {
    /// The typed message handler.
    pub handler: TypedHandler<T>,
    /// Cached handler ID for faster equality checks.
    pub handler_id: Ustr,
    /// The pattern for matching topics.
    pub pattern: MStr<Pattern>,
    /// Higher priority handlers receive messages first.
    pub priority: u32,
}

impl<T: 'static> TypedSubscription<T> {
    /// Creates a new typed subscription.
    #[must_use]
    pub fn new(pattern: MStr<Pattern>, handler: TypedHandler<T>, priority: Option<u32>) -> Self {
        Self {
            handler_id: handler.id(),
            pattern,
            handler,
            priority: priority.unwrap_or(0),
        }
    }

    fn delivery_order(&self, other: &Self) -> Ordering {
        other
            .priority
            .cmp(&self.priority)
            .then_with(|| self.pattern.cmp(&other.pattern))
            .then_with(|| self.handler_id.cmp(&other.handler_id))
    }
}

impl<T: 'static> Debug for TypedSubscription<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(TypedSubscription))
            .field("handler_id", &self.handler_id)
            .field("pattern", &self.pattern)
            .field("priority", &self.priority)
            .field("type", &std::any::type_name::<T>())
            .finish()
    }
}

impl<T: 'static> PartialEq for TypedSubscription<T> {
    fn eq(&self, other: &Self) -> bool {
        self.pattern == other.pattern && self.handler_id == other.handler_id
    }
}

impl<T: 'static> Eq for TypedSubscription<T> {}

impl<T: 'static> PartialOrd for TypedSubscription<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: 'static> Ord for TypedSubscription<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.pattern
            .cmp(&other.pattern)
            .then_with(|| self.handler_id.cmp(&other.handler_id))
    }
}

impl<T: 'static> Hash for TypedSubscription<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.pattern.hash(state);
        self.handler_id.hash(state);
    }
}

/// Routes messages of type `T` to subscribed handlers based on topic patterns.
///
/// Supports wildcard patterns (`*` and `?`) and priority-based ordering.
///
/// Subscriptions are indexed by kind. A wildcard-free pattern matches exactly one topic (its
/// own text), so exact subscriptions live in a map keyed by that topic and a publish to it is a
/// single lookup. Wildcard subscriptions are kept in one list, and the handlers a topic matches
/// are cached per topic while any wildcard subscription exists. Subscribing or unsubscribing an
/// exact pattern invalidates only its own topic's cache entry; a wildcard change clears the
/// cache. Delivery order is unchanged: priority descending, then pattern, then handler ID.
pub struct TopicRouter<T: 'static> {
    /// Exact (wildcard-free) subscriptions by topic, each list in delivery order.
    exact: AHashMap<Ustr, SmallVec<[TypedSubscription<T>; 1]>>,
    /// Wildcard subscriptions, in delivery order.
    wildcards: Vec<TypedSubscription<T>>,
    /// Number of active subscriptions.
    count: usize,
    /// Handlers matching each published topic, in delivery order; used only while wildcard
    /// subscriptions exist (exact-only topics are a direct lookup).
    topic_cache: AHashMap<Ustr, SmallVec<[TypedHandler<T>; 4]>>,
}

impl<T: 'static> Debug for TopicRouter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(TopicRouter))
            .field("subscriptions", &self.count)
            .field("exact_topics", &self.exact.len())
            .field("wildcards", &self.wildcards.len())
            .field("cached_topics", &self.topic_cache.len())
            .finish()
    }
}

impl<T: 'static> Default for TopicRouter<T> {
    fn default() -> Self {
        Self::new()
    }
}

fn is_exact(pattern: MStr<Pattern>) -> bool {
    !pattern.as_bytes().iter().any(|&b| b == b'*' || b == b'?')
}

impl<T: 'static> TopicRouter<T> {
    /// Creates a new empty topic router.
    #[must_use]
    pub fn new() -> Self {
        Self {
            exact: AHashMap::new(),
            wildcards: Vec::new(),
            count: 0,
            topic_cache: AHashMap::new(),
        }
    }

    /// Returns the number of active subscriptions.
    #[must_use]
    pub fn subscription_count(&self) -> usize {
        self.count
    }

    /// Returns whether there are any subscriptions.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// All subscriptions in delivery order.
    fn ordered_subscriptions(&self) -> Vec<&TypedSubscription<T>> {
        let mut all: Vec<&TypedSubscription<T>> = self
            .exact
            .values()
            .flat_map(|subs| subs.iter())
            .chain(self.wildcards.iter())
            .collect();
        all.sort_by(|a, b| a.delivery_order(b));
        all
    }

    /// Returns all active subscription patterns.
    #[must_use]
    pub fn patterns(&self) -> Vec<&str> {
        self.ordered_subscriptions()
            .into_iter()
            .map(|s| s.pattern.as_str())
            .collect()
    }

    /// Returns all subscription handler IDs.
    #[must_use]
    pub fn handler_ids(&self) -> Vec<&str> {
        self.ordered_subscriptions()
            .into_iter()
            .map(|s| s.handler_id.as_str())
            .collect()
    }

    /// Subscribes a handler to a topic pattern.
    ///
    /// # Warning
    ///
    /// Assigning priority is an advanced feature. Higher priority handlers
    /// receive messages before lower priority handlers.
    pub fn subscribe(&mut self, pattern: MStr<Pattern>, handler: TypedHandler<T>, priority: u32) {
        let sub = TypedSubscription::new(pattern, handler, Some(priority));

        // Re-subscribing the same handler is expected (e.g. book deltas + snapshots
        // share one BookUpdater), so dedup at debug rather than warn.
        if self.find(pattern, sub.handler_id).is_some() {
            log::debug!("{sub:?} already exists; skipping duplicate subscription");
            return;
        }

        log::debug!("Subscribing {sub:?}");

        // Insert at the delivery position: priority descending, pattern ascending, then
        // handler ID ascending.
        if is_exact(pattern) {
            let subs = self.exact.entry(*pattern).or_default();
            let idx = subs.partition_point(|s| s.delivery_order(&sub) == Ordering::Less);
            subs.insert(idx, sub);
            self.topic_cache.remove(&*pattern);
        } else {
            let idx = self
                .wildcards
                .partition_point(|s| s.delivery_order(&sub) == Ordering::Less);
            self.wildcards.insert(idx, sub);
            self.topic_cache.clear();
        }
        self.count += 1;
    }

    /// Returns the position of the (pattern, handler ID) subscription in its list: the topic's
    /// exact list for a wildcard-free pattern, else the wildcard list.
    fn find(&self, pattern: MStr<Pattern>, handler_id: Ustr) -> Option<usize> {
        if is_exact(pattern) {
            self.exact
                .get(&*pattern)
                .and_then(|subs| subs.iter().position(|s| s.handler_id == handler_id))
        } else {
            self.wildcards
                .iter()
                .position(|s| s.pattern == pattern && s.handler_id == handler_id)
        }
    }

    /// Removes the (pattern, handler ID) subscription, returning whether one was removed.
    fn remove(&mut self, pattern: MStr<Pattern>, handler_id: Ustr) -> bool {
        let Some(idx) = self.find(pattern, handler_id) else {
            return false;
        };

        if is_exact(pattern) {
            let subs = self
                .exact
                .get_mut(&*pattern)
                .expect("found subscription's topic is indexed");
            subs.remove(idx);
            if subs.is_empty() {
                self.exact.remove(&*pattern);
            }
            self.topic_cache.remove(&*pattern);
        } else {
            self.wildcards.remove(idx);
            self.topic_cache.clear();
        }
        self.count -= 1;
        true
    }

    /// Unsubscribes a handler from a topic pattern.
    pub fn unsubscribe(&mut self, pattern: MStr<Pattern>, handler: &TypedHandler<T>) {
        log::debug!(
            "Unsubscribing handler {} from pattern '{pattern}'",
            handler.id()
        );

        if self.remove(pattern, handler.id()) {
            log::debug!("Handler for pattern '{pattern}' was removed");
        } else {
            log::debug!("No matching handler for pattern '{pattern}' was found");
        }
    }

    /// Removes a specific handler from a pattern by handler ID.
    pub fn remove_handler(&mut self, pattern: MStr<Pattern>, handler_id: Ustr) {
        if self.remove(pattern, handler_id) {
            log::debug!("Handler {handler_id} for pattern '{pattern}' was removed");
        }
    }

    /// Checks if a handler is subscribed to a pattern.
    #[must_use]
    pub fn is_subscribed(&self, pattern: MStr<Pattern>, handler: &TypedHandler<T>) -> bool {
        self.find(pattern, handler.id()).is_some()
    }

    /// Returns whether there are subscribers for the topic.
    #[must_use]
    pub fn has_subscribers(&self, topic: MStr<Topic>) -> bool {
        self.exact.contains_key(&*topic)
            || self
                .wildcards
                .iter()
                .any(|s| is_matching_backtracking(topic, s.pattern))
    }

    /// Returns the count of subscribers for a topic.
    #[must_use]
    pub fn subscriber_count(&self, topic: MStr<Topic>) -> usize {
        self.exact_subscriber_count(topic)
            + self
                .wildcards
                .iter()
                .filter(|s| is_matching_backtracking(topic, s.pattern))
                .count()
    }

    /// Returns the count of subscribers with an exact topic match,
    /// excluding wildcard pattern subscriptions.
    #[must_use]
    pub fn exact_subscriber_count(&self, topic: MStr<Topic>) -> usize {
        self.exact.get(&*topic).map_or(0, |subs| subs.len())
    }

    /// Handlers matching a topic, in delivery order: the topic's exact subscriptions merged
    /// with the wildcard subscriptions that match it.
    fn compute_handlers(
        exact: &AHashMap<Ustr, SmallVec<[TypedSubscription<T>; 1]>>,
        wildcards: &[TypedSubscription<T>],
        topic: MStr<Topic>,
    ) -> SmallVec<[TypedHandler<T>; 4]> {
        let exact_subs: &[TypedSubscription<T>] = exact.get(&*topic).map_or(&[], |s| s.as_slice());
        let mut exact_iter = exact_subs.iter().peekable();
        let mut handlers = SmallVec::new();

        for wildcard in wildcards
            .iter()
            .filter(|s| is_matching_backtracking(topic, s.pattern))
        {
            while let Some(sub) =
                exact_iter.next_if(|s| s.delivery_order(wildcard) == Ordering::Less)
            {
                handlers.push(sub.handler.clone());
            }
            handlers.push(wildcard.handler.clone());
        }
        handlers.extend(exact_iter.map(|s| s.handler.clone()));
        handlers
    }

    /// Publishes a message to all handlers subscribed to matching patterns.
    pub fn publish(&mut self, topic: MStr<Topic>, message: &T) {
        if self.wildcards.is_empty() {
            if let Some(subs) = self.exact.get(&*topic) {
                for sub in subs {
                    sub.handler.handle(message);
                }
            }
            return;
        }

        let Self {
            exact,
            wildcards,
            topic_cache,
            ..
        } = self;

        let handlers = topic_cache
            .entry(*topic)
            .or_insert_with(|| Self::compute_handlers(exact, wildcards, topic));

        for handler in handlers.iter() {
            handler.handle(message);
        }
    }

    /// Returns cloned handlers matching a topic for safe out-of-borrow calling.
    ///
    /// Use this when handlers may need to access the message bus during execution.
    /// Note: Allocates a Vec on each call. For hot paths, prefer the thread-local
    /// buffer pattern used by `publish_*` functions.
    pub fn get_matching_handlers(&mut self, topic: MStr<Topic>) -> Vec<TypedHandler<T>> {
        let mut buf: SmallVec<[TypedHandler<T>; 64]> = SmallVec::new();
        self.fill_matching_handlers(topic, &mut buf);
        buf.into_vec()
    }

    /// Fills a buffer with handlers matching a topic.
    pub(crate) fn fill_matching_handlers(
        &mut self,
        topic: MStr<Topic>,
        buf: &mut SmallVec<[TypedHandler<T>; 64]>,
    ) {
        if self.wildcards.is_empty() {
            if let Some(subs) = self.exact.get(&*topic) {
                buf.extend(subs.iter().map(|s| s.handler.clone()));
            }
            return;
        }

        let Self {
            exact,
            wildcards,
            topic_cache,
            ..
        } = self;

        let handlers = topic_cache
            .entry(*topic)
            .or_insert_with(|| Self::compute_handlers(exact, wildcards, topic));

        buf.extend(handlers.iter().cloned());
    }

    /// Clears all subscriptions and cache.
    pub fn clear(&mut self) {
        self.exact.clear();
        self.wildcards.clear();
        self.count = 0;
        self.topic_cache.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
        rc::Rc,
    };

    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_typed_subscription_ordering_laws() {
        let handler = TypedHandler::from_with_id("handler-b", |_: &i32| {});
        let base = TypedSubscription::new("pattern-b".into(), handler.clone(), Some(1));
        let different_priority = TypedSubscription::new("pattern-b".into(), handler, Some(2));
        let different_pattern = TypedSubscription::new(
            "pattern-a".into(),
            TypedHandler::from_with_id("handler-b", |_: &i32| {}),
            Some(1),
        );
        let different_handler = TypedSubscription::new(
            "pattern-b".into(),
            TypedHandler::from_with_id("handler-a", |_: &i32| {}),
            Some(1),
        );

        assert_eq!(base, different_priority);
        assert_eq!(base.cmp(&different_priority), Ordering::Equal);

        let mut base_hasher = DefaultHasher::new();
        base.hash(&mut base_hasher);
        let mut different_priority_hasher = DefaultHasher::new();
        different_priority.hash(&mut different_priority_hasher);
        assert_eq!(base_hasher.finish(), different_priority_hasher.finish());

        let variants = [
            base,
            different_priority,
            different_pattern,
            different_handler,
        ];

        for a in &variants {
            for b in &variants {
                assert_eq!(a == b, a.cmp(b).is_eq());
                assert_eq!(a.partial_cmp(b), Some(a.cmp(b)));
                assert_eq!(a.cmp(b), b.cmp(a).reverse());
            }
        }
    }

    #[rstest]
    fn test_topic_router_subscribe_and_publish() {
        let mut router = TopicRouter::<String>::new();
        let received = Rc::new(RefCell::new(Vec::new()));
        let received_clone = received.clone();

        let handler = TypedHandler::from(move |msg: &String| {
            received_clone.borrow_mut().push(msg.clone());
        });

        router.subscribe("data.quotes.*".into(), handler, 0);

        let topic: MStr<Topic> = "data.quotes.AAPL".into();
        router.publish(topic, &"quote1".to_string());
        router.publish(topic, &"quote2".to_string());

        assert_eq!(*received.borrow(), vec!["quote1", "quote2"]);
    }

    #[rstest]
    fn test_topic_router_publish_orders_by_full_delivery_key() {
        let mut router = TopicRouter::<i32>::new();
        let order = Rc::new(RefCell::new(Vec::new()));

        let low_order = order.clone();
        let low = TypedHandler::from_with_id("handler-z", move |_: &i32| {
            low_order.borrow_mut().push("low-z");
        });
        let exact_b_order = order.clone();
        let exact_b = TypedHandler::from_with_id("handler-b", move |_: &i32| {
            exact_b_order.borrow_mut().push("exact-b");
        });
        let exact_a_order = order.clone();
        let exact_a = TypedHandler::from_with_id("handler-a", move |_: &i32| {
            exact_a_order.borrow_mut().push("exact-a");
        });
        let wildcard_b_order = order.clone();
        let wildcard_b = TypedHandler::from_with_id("handler-b", move |_: &i32| {
            wildcard_b_order.borrow_mut().push("wildcard-b");
        });

        router.subscribe("delivery.*".into(), low, 1);
        router.subscribe("delivery.topic".into(), exact_b, 10);
        router.subscribe("delivery.topic".into(), exact_a, 10);
        router.subscribe("delivery.*".into(), wildcard_b, 10);

        let topic: MStr<Topic> = "delivery.topic".into();
        router.publish(topic, &42);

        assert_eq!(
            *order.borrow(),
            vec!["wildcard-b", "exact-a", "exact-b", "low-z"]
        );
    }

    #[rstest]
    fn test_topic_router_unsubscribe() {
        let mut router = TopicRouter::<String>::new();
        let received = Rc::new(RefCell::new(Vec::new()));
        let received_clone = received.clone();

        let handler = TypedHandler::from_with_id("test-handler", move |msg: &String| {
            received_clone.borrow_mut().push(msg.clone());
        });

        router.subscribe("data.*".into(), handler.clone(), 0);
        assert!(router.is_subscribed("data.*".into(), &handler));

        router.unsubscribe("data.*".into(), &handler);
        assert!(!router.is_subscribed("data.*".into(), &handler));

        let topic: MStr<Topic> = "data.test".into();
        router.publish(topic, &"test".to_string());

        // Should not receive anything after unsubscribe
        assert!(received.borrow().is_empty());
    }

    #[rstest]
    fn test_topic_router_duplicate_subscription() {
        let mut router = TopicRouter::<i32>::new();

        let handler1 = TypedHandler::from_with_id("dup-handler", |_: &i32| {});
        let handler2 = TypedHandler::from_with_id("dup-handler", |_: &i32| {});

        router.subscribe("test.*".into(), handler1, 0);
        router.subscribe("test.*".into(), handler2, 0);

        // Should only have one subscription
        assert_eq!(router.subscription_count(), 1);
    }

    #[rstest]
    fn test_topic_router_wildcard_patterns() {
        let mut router = TopicRouter::<String>::new();
        let received = Rc::new(RefCell::new(Vec::new()));
        let received_clone = received.clone();

        let handler = TypedHandler::from(move |msg: &String| {
            received_clone.borrow_mut().push(msg.clone());
        });

        router.subscribe("data.*.AAPL".into(), handler, 0);

        // Should match
        let topic1: MStr<Topic> = "data.quotes.AAPL".into();
        router.publish(topic1, &"match1".to_string());

        let topic2: MStr<Topic> = "data.trades.AAPL".into();
        router.publish(topic2, &"match2".to_string());

        // Should not match
        let topic3: MStr<Topic> = "data.quotes.MSFT".into();
        router.publish(topic3, &"no-match".to_string());

        assert_eq!(*received.borrow(), vec!["match1", "match2"]);
    }

    #[rstest]
    fn test_topic_router_cache_populated_on_publish() {
        let mut router = TopicRouter::<i32>::new();
        let handler = TypedHandler::from_with_id("cache-test", |_: &i32| {});

        router.subscribe("data.*".into(), handler, 0);

        // First publish populates cache
        let topic: MStr<Topic> = "data.quotes".into();
        router.publish(topic, &1);

        // Verify cache is used (subscriber_count uses cache if available)
        assert_eq!(router.subscriber_count(topic), 1);
    }

    #[rstest]
    fn test_topic_router_cache_invalidated_on_subscribe() {
        let mut router = TopicRouter::<i32>::new();
        let received = Rc::new(RefCell::new(0));

        let r1 = received.clone();
        let handler1 = TypedHandler::from_with_id("h1", move |_: &i32| {
            *r1.borrow_mut() += 1;
        });

        router.subscribe("data.*".into(), handler1, 0);

        // Publish to populate cache
        let topic: MStr<Topic> = "data.test".into();
        router.publish(topic, &1);
        assert_eq!(*received.borrow(), 1);

        // Subscribe new handler (should invalidate cache)
        let r2 = received.clone();
        let handler2 = TypedHandler::from_with_id("h2", move |_: &i32| {
            *r2.borrow_mut() += 10;
        });
        router.subscribe("data.*".into(), handler2, 0);

        // Publish again - both handlers should receive
        router.publish(topic, &2);
        assert_eq!(*received.borrow(), 12); // 1 + 1 + 10
    }

    #[rstest]
    fn test_topic_router_late_distinct_wildcard_receives_cached_topic() {
        let mut router = TopicRouter::<String>::new();
        let topic: MStr<Topic> = "data.instrument.POLYMARKET.TEST-SYMBOL".into();

        let early = Rc::new(RefCell::new(Vec::new()));
        let early_clone = early.clone();
        let early_handler = TypedHandler::from_with_id("early", move |msg: &String| {
            early_clone.borrow_mut().push(msg.clone());
        });
        router.subscribe("data.*.POLYMARKET.*".into(), early_handler, 0);

        router.publish(topic, &"ONE".to_string());

        let late = Rc::new(RefCell::new(Vec::new()));
        let late_clone = late.clone();
        let late_handler = TypedHandler::from_with_id("late", move |msg: &String| {
            late_clone.borrow_mut().push(msg.clone());
        });
        router.subscribe("data.instrument.POLYMARKET.*".into(), late_handler, 0);

        router.publish(topic, &"TWO".to_string());

        assert_eq!(*early.borrow(), vec!["ONE", "TWO"]);
        assert_eq!(*late.borrow(), vec!["TWO"]);
    }

    #[rstest]
    fn test_topic_router_cache_invalidated_on_unsubscribe() {
        let mut router = TopicRouter::<i32>::new();
        let received = Rc::new(RefCell::new(0));

        let r1 = received.clone();
        let handler1 = TypedHandler::from_with_id("h1", move |_: &i32| {
            *r1.borrow_mut() += 1;
        });

        let r2 = received.clone();
        let handler2 = TypedHandler::from_with_id("h2", move |_: &i32| {
            *r2.borrow_mut() += 10;
        });

        router.subscribe("data.*".into(), handler1.clone(), 0);
        router.subscribe("data.*".into(), handler2, 0);

        // Publish to populate cache
        let topic: MStr<Topic> = "data.test".into();
        router.publish(topic, &1);
        assert_eq!(*received.borrow(), 11); // 1 + 10

        // Unsubscribe handler1 (should invalidate cache)
        router.unsubscribe("data.*".into(), &handler1);

        // Publish again - only handler2 should receive
        router.publish(topic, &2);
        assert_eq!(*received.borrow(), 21); // 11 + 10
    }

    #[rstest]
    fn test_topic_router_has_subscribers() {
        let mut router = TopicRouter::<i32>::new();

        let topic: MStr<Topic> = "data.quotes.AAPL".into();
        assert!(!router.has_subscribers(topic));

        let handler = TypedHandler::from_with_id("test", |_: &i32| {});
        router.subscribe("data.quotes.*".into(), handler, 0);

        assert!(router.has_subscribers(topic));
    }

    #[rstest]
    fn test_topic_router_subscriber_count() {
        let mut router = TopicRouter::<i32>::new();

        let topic: MStr<Topic> = "data.quotes.AAPL".into();
        assert_eq!(router.subscriber_count(topic), 0);

        let handler1 = TypedHandler::from_with_id("h1", |_: &i32| {});
        let handler2 = TypedHandler::from_with_id("h2", |_: &i32| {});
        let handler3 = TypedHandler::from_with_id("h3", |_: &i32| {});

        router.subscribe("data.quotes.*".into(), handler1, 0);
        router.subscribe("data.*.AAPL".into(), handler2, 0);
        router.subscribe("events.*".into(), handler3, 0); // Won't match

        assert_eq!(router.subscriber_count(topic), 2);
    }

    #[rstest]
    fn test_topic_router_patterns_and_handler_ids() {
        let mut router = TopicRouter::<i32>::new();

        let handler1 = TypedHandler::from_with_id("handler-a", |_: &i32| {});
        let handler2 = TypedHandler::from_with_id("handler-b", |_: &i32| {});

        router.subscribe("pattern.one".into(), handler1, 0);
        router.subscribe("pattern.two".into(), handler2, 0);

        let patterns = router.patterns();
        assert!(patterns.contains(&"pattern.one"));
        assert!(patterns.contains(&"pattern.two"));

        let ids = router.handler_ids();
        assert!(ids.contains(&"handler-a"));
        assert!(ids.contains(&"handler-b"));
    }

    #[rstest]
    fn test_topic_router_clear() {
        let mut router = TopicRouter::<i32>::new();
        let handler = TypedHandler::from_with_id("clear-test", |_: &i32| {});

        router.subscribe("data.*".into(), handler, 0);

        // Populate cache
        let topic: MStr<Topic> = "data.test".into();
        router.publish(topic, &1);

        assert_eq!(router.subscription_count(), 1);
        assert!(!router.is_empty());

        router.clear();

        assert_eq!(router.subscription_count(), 0);
        assert!(router.is_empty());
        assert!(!router.has_subscribers(topic));
    }

    #[rstest]
    fn test_topic_router_multiple_patterns_same_topic() {
        let mut router = TopicRouter::<i32>::new();
        let received = Rc::new(RefCell::new(Vec::new()));

        let r1 = received.clone();
        let handler1 = TypedHandler::from_with_id("specific", move |v: &i32| {
            r1.borrow_mut().push(format!("specific:{v}"));
        });

        let r2 = received.clone();
        let handler2 = TypedHandler::from_with_id("wildcard", move |v: &i32| {
            r2.borrow_mut().push(format!("wildcard:{v}"));
        });

        let r3 = received.clone();
        let handler3 = TypedHandler::from_with_id("all", move |v: &i32| {
            r3.borrow_mut().push(format!("all:{v}"));
        });

        // All three patterns match "data.quotes.AAPL"
        router.subscribe("data.quotes.AAPL".into(), handler1, 0);
        router.subscribe("data.quotes.*".into(), handler2, 0);
        router.subscribe("data.*.*".into(), handler3, 0);

        let topic: MStr<Topic> = "data.quotes.AAPL".into();
        router.publish(topic, &42);

        let msgs = received.borrow();
        assert_eq!(msgs.len(), 3);
        assert!(msgs.contains(&"specific:42".to_string()));
        assert!(msgs.contains(&"wildcard:42".to_string()));
        assert!(msgs.contains(&"all:42".to_string()));
    }

    #[rstest]
    fn test_remove_handler_invalidates_cross_pattern_cache() {
        let mut router = TopicRouter::<i32>::new();
        let count_a = Rc::new(RefCell::new(0));
        let count_b = Rc::new(RefCell::new(0));

        let ca = count_a.clone();
        let handler_a = TypedHandler::from_with_id("ha", move |_: &i32| {
            *ca.borrow_mut() += 1;
        });
        let handler_a_id = Ustr::from("ha");

        let cb = count_b.clone();
        let handler_b = TypedHandler::from_with_id("hb", move |_: &i32| {
            *cb.borrow_mut() += 1;
        });

        router.subscribe("events.order.S-001".into(), handler_a, 0);
        router.subscribe("events.order.S-002".into(), handler_b, 0);

        let topic_a: MStr<Topic> = "events.order.S-001".into();
        let topic_b: MStr<Topic> = "events.order.S-002".into();
        router.publish(topic_a, &1);
        router.publish(topic_b, &1);
        assert_eq!(*count_a.borrow(), 1);
        assert_eq!(*count_b.borrow(), 1);

        // Remove handler_a - must invalidate ALL cached indices
        router.remove_handler("events.order.S-001".into(), handler_a_id);

        // handler_b must still dispatch correctly despite index shift
        router.publish(topic_b, &2);
        assert_eq!(*count_b.borrow(), 2);

        router.publish(topic_a, &3);
        assert_eq!(*count_a.borrow(), 1);
    }

    #[rstest]
    fn test_remove_handler_only_removes_targeted_handler() {
        let mut router = TopicRouter::<i32>::new();
        let count_own = Rc::new(RefCell::new(0));
        let count_other = Rc::new(RefCell::new(0));

        let co = count_own.clone();
        let handler_own = TypedHandler::from_with_id("strategy", move |_: &i32| {
            *co.borrow_mut() += 1;
        });
        let own_id = Ustr::from("strategy");

        let cother = count_other.clone();
        let handler_other = TypedHandler::from_with_id("exec-algo", move |_: &i32| {
            *cother.borrow_mut() += 1;
        });

        // Both handlers on the same pattern (same strategy topic)
        let pattern: MStr<Pattern> = "events.order.S-001".into();
        router.subscribe(pattern, handler_own, 0);
        router.subscribe(pattern, handler_other, 0);

        let topic: MStr<Topic> = "events.order.S-001".into();
        router.publish(topic, &1);
        assert_eq!(*count_own.borrow(), 1);
        assert_eq!(*count_other.borrow(), 1);

        router.remove_handler(pattern, own_id);

        router.publish(topic, &2);
        assert_eq!(*count_own.borrow(), 1);
        assert_eq!(*count_other.borrow(), 2);
    }

    #[rstest]
    fn test_unsubscribe_one_pattern_does_not_break_other_patterns() {
        let mut router = TopicRouter::<i32>::new();
        let received = Rc::new(RefCell::new(0));

        let alpha = TypedHandler::from_with_id("alpha", |_: &i32| {});

        let received_beta = received.clone();
        let beta = TypedHandler::from_with_id("beta", move |_: &i32| {
            *received_beta.borrow_mut() += 1;
        });

        router.subscribe("alpha.*".into(), alpha.clone(), 0);
        router.subscribe("beta.*".into(), beta, 0);

        let beta_topic: MStr<Topic> = "beta.topic".into();
        router.publish(beta_topic, &1);
        assert_eq!(*received.borrow(), 1);

        router.unsubscribe("alpha.*".into(), &alpha);

        router.publish(beta_topic, &2);
        assert_eq!(*received.borrow(), 2);
    }

    /// The router against a reference model of the pre-index design (one list sorted in
    /// delivery order, every subscription matched linearly per publish), over seeded random
    /// sequences of exact and wildcard subscribes, unsubscribes, handler removals and publishes.
    #[rstest]
    fn test_indexed_router_matches_linear_reference() {
        let topics = ["data.a.x", "data.a.y", "data.b.x", "events.a", "events.b.x"];
        let patterns = [
            "data.a.x",
            "data.a.y",
            "data.b.x",
            "events.a",
            "events.b.x",
            "data.*",
            "data.a.*",
            "*.x",
            "events.?",
            "*",
        ];
        let handler_names = ["h0", "h1", "h2", "h3"];

        let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut next = move |bound: usize| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            ((state >> 33) as usize) % bound
        };

        for _case in 0..200 {
            let received: Rc<RefCell<Vec<&'static str>>> = Rc::new(RefCell::new(Vec::new()));
            let handlers: Vec<TypedHandler<i32>> = handler_names
                .iter()
                .map(|&name| {
                    let received = received.clone();
                    TypedHandler::from_with_id(name, move |_: &i32| {
                        received.borrow_mut().push(name);
                    })
                })
                .collect();

            let mut router = TopicRouter::<i32>::new();
            let mut reference: Vec<TypedSubscription<i32>> = Vec::new();

            for _step in 0..60 {
                let pattern: MStr<Pattern> = patterns[next(patterns.len())].into();
                let handler = &handlers[next(handlers.len())];
                match next(4) {
                    0 | 1 => {
                        let priority = next(3) as u32;
                        router.subscribe(pattern, handler.clone(), priority);
                        let sub = TypedSubscription::new(pattern, handler.clone(), Some(priority));
                        if !reference.iter().any(|s| s == &sub) {
                            reference.push(sub);
                            reference.sort_by(TypedSubscription::delivery_order);
                        }
                    }
                    2 => {
                        if next(2) == 0 {
                            router.unsubscribe(pattern, handler);
                        } else {
                            router.remove_handler(pattern, handler.id());
                        }
                        reference
                            .retain(|s| !(s.pattern == pattern && s.handler_id == handler.id()));
                    }
                    _ => {
                        let topic: MStr<Topic> = topics[next(topics.len())].into();
                        received.borrow_mut().clear();
                        router.publish(topic, &1);
                        let expected: Vec<&str> = reference
                            .iter()
                            .filter(|s| is_matching_backtracking(topic, s.pattern))
                            .map(|s| s.handler_id.as_str())
                            .collect();
                        assert_eq!(*received.borrow(), expected, "publish to {topic}");
                        let mut buf = SmallVec::new();
                        router.fill_matching_handlers(topic, &mut buf);
                        let filled: Vec<&str> = buf.iter().map(|h| h.id().as_str()).collect();
                        assert_eq!(filled, expected, "fill for {topic}");
                        assert_eq!(router.subscriber_count(topic), expected.len());
                        assert_eq!(router.has_subscribers(topic), !expected.is_empty());
                        let exact = reference
                            .iter()
                            .filter(|s| s.pattern.as_str() == topic.as_str())
                            .count();
                        assert_eq!(router.exact_subscriber_count(topic), exact);
                    }
                }

                assert_eq!(router.subscription_count(), reference.len());
                assert_eq!(router.is_empty(), reference.is_empty());
                let ref_patterns: Vec<&str> =
                    reference.iter().map(|s| s.pattern.as_str()).collect();
                assert_eq!(router.patterns(), ref_patterns);
                let ref_ids: Vec<&str> = reference.iter().map(|s| s.handler_id.as_str()).collect();
                assert_eq!(router.handler_ids(), ref_ids);
                for sub in &reference {
                    assert!(router.is_subscribed(sub.pattern, &sub.handler));
                }
            }
        }
    }
}
