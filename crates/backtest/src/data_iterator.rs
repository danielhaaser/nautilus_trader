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

//! Static backtest replay using the shared ordered batch iterator.

use ahash::AHashMap;
use nautilus_data::replay::DataIterator;
use nautilus_model::data::{Data, DataBatch, DataRef};

/// Where the backtest engine delivers a stream's items.
///
/// A venue whose matching should run on a different clock than its subscribers' can replay two
/// copies of its data: one stream that only the simulated exchange sees, stamped with the instant
/// the venue applied each item, and one that only the data engine sees, stamped with the instant
/// the subscriber received it. Every stream is [`DataRoute::Both`] unless set otherwise, which is
/// the engine's behaviour without routes.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Hash)]
pub enum DataRoute {
    /// The simulated exchange, then the data engine (the default).
    #[default]
    Both,
    /// The simulated exchange only: the data engine never sees the item.
    ExchangeOnly,
    /// The data engine only: the simulated exchange never sees the item.
    DataOnly,
}

impl DataRoute {
    /// Whether the simulated exchange processes an item on this route.
    #[must_use]
    pub const fn to_exchange(self) -> bool {
        matches!(self, Self::Both | Self::ExchangeOnly)
    }

    /// Whether the data engine processes an item on this route.
    #[must_use]
    pub const fn to_data_engine(self) -> bool {
        matches!(self, Self::Both | Self::DataOnly)
    }
}

/// Multi-stream, time-ordered data iterator used by the backtest engine.
#[derive(Debug, Default)]
pub struct BacktestDataIterator {
    iterator: DataIterator,
    /// The routes set by stream name; a name absent here is [`DataRoute::Both`].
    routes: AHashMap<String, DataRoute>,
}

impl BacktestDataIterator {
    /// Creates a new empty [`BacktestDataIterator`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            iterator: DataIterator::new(),
            routes: AHashMap::new(),
        }
    }

    /// Adds (or replaces) a named data stream.
    ///
    /// When `append_data` is true the stream gets lower priority on equal replay keys;
    /// when false (prepend) it wins ties. Named replacements retain their original priority.
    pub fn add_data(&mut self, name: &str, data: Vec<Data>, append_data: bool) {
        self.routes.remove(name);
        self.iterator.add_data(name, data, append_data);
    }

    /// Adds (or replaces) a named typed data stream.
    ///
    /// Items are ordered by replay key before insertion. A batch that arrives out of order while
    /// sharing its backing allocation with another view is copied once, so the shared allocation
    /// is never reordered.
    pub fn add_data_batch(&mut self, name: &str, data: DataBatch, append_data: bool) {
        self.routes.remove(name);
        self.iterator.add_data_batch(name, data, append_data);
    }

    /// Removes a named data stream.
    pub fn remove_data(&mut self, name: &str, complete_remove: bool) {
        self.routes.remove(name);
        self.iterator.remove_data(name, complete_remove);
    }

    /// Sets the route of a named stream ([`DataRoute::Both`] until set). Adding or replacing a
    /// stream under a name resets its route to the default; a name with no stream is ignored.
    pub fn set_route(&mut self, name: &str, route: DataRoute) {
        if !self.iterator.contains(name) {
            return;
        }
        if route == DataRoute::Both {
            self.routes.remove(name);
        } else {
            self.routes.insert(name.to_owned(), route);
        }
    }

    /// Returns the route of the element [`peek`](Self::peek) returns, or the default when the
    /// iterator is exhausted.
    pub(crate) fn peek_route(&self) -> DataRoute {
        self.iterator
            .peek_name()
            .and_then(|name| self.routes.get(name).copied())
            .unwrap_or_default()
    }

    /// Sets the cursor of a named stream to `index` (0-based).
    pub fn set_index(&mut self, name: &str, index: usize) {
        self.iterator.set_index(name, index);
    }

    /// Resets all stream cursors to the beginning.
    pub fn reset_all_cursors(&mut self) {
        self.iterator.reset_all_cursors();
    }

    pub(crate) fn peek(&self) -> Option<DataRef<'_>> {
        self.iterator.peek()
    }

    pub(crate) fn advance(&mut self) {
        self.iterator.advance();
    }

    /// Returns the next market [`Data`] element across all streams in chronological order.
    #[expect(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Data> {
        self.iterator.next()
    }

    /// Returns whether all streams have been fully consumed.
    #[must_use]
    pub fn is_done(&self) -> bool {
        self.iterator.is_done()
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        data::QuoteTick,
        identifiers::InstrumentId,
        types::{Price, Quantity},
    };
    use rstest::rstest;

    use super::*;

    fn quote(id: &str, ts: u64) -> Data {
        Data::Quote(QuoteTick::new(
            InstrumentId::from(id),
            Price::from("1.0"),
            Price::from("1.0"),
            Quantity::from(100),
            Quantity::from(100),
            ts.into(),
            ts.into(),
        ))
    }

    #[rstest]
    fn test_routes_default_to_both_and_follow_the_peeked_stream() {
        let mut it = BacktestDataIterator::new();
        assert_eq!(it.peek_route(), DataRoute::Both);
        it.add_data("exchange", vec![quote("A.B", 1), quote("A.B", 3)], true);
        it.add_data("data", vec![quote("A.B", 2), quote("A.B", 3)], true);
        it.add_data("both", vec![quote("C.D", 4)], true);
        it.set_route("exchange", DataRoute::ExchangeOnly);
        it.set_route("data", DataRoute::DataOnly);
        it.set_route("absent", DataRoute::DataOnly);

        let mut seen = Vec::new();
        while let Some(item) = it.peek() {
            seen.push((item.ts_init().as_u64(), it.peek_route()));
            it.advance();
        }

        // At the tie (3) the stream added first wins, so the exchange copy precedes the data copy.
        assert_eq!(
            seen,
            vec![
                (1, DataRoute::ExchangeOnly),
                (2, DataRoute::DataOnly),
                (3, DataRoute::ExchangeOnly),
                (3, DataRoute::DataOnly),
                (4, DataRoute::Both),
            ]
        );
        assert_eq!(it.peek_route(), DataRoute::Both);
    }

    #[rstest]
    fn test_route_of_a_single_stream_and_reset_on_replace() {
        let mut it = BacktestDataIterator::new();
        it.add_data("only", vec![quote("A.B", 1)], true);
        it.set_route("only", DataRoute::ExchangeOnly);
        assert_eq!(it.peek_route(), DataRoute::ExchangeOnly);

        it.add_data("only", vec![quote("A.B", 2)], true);
        assert_eq!(it.peek_route(), DataRoute::Both);

        it.set_route("only", DataRoute::DataOnly);
        it.set_route("only", DataRoute::Both);
        assert_eq!(it.peek_route(), DataRoute::Both);

        it.set_route("only", DataRoute::DataOnly);
        it.remove_data("only", true);
        it.add_data("only", vec![quote("A.B", 3)], true);
        assert_eq!(it.peek_route(), DataRoute::Both);
    }

    #[rstest]
    fn test_data_route_destinations() {
        assert!(DataRoute::Both.to_exchange() && DataRoute::Both.to_data_engine());
        assert!(DataRoute::ExchangeOnly.to_exchange() && !DataRoute::ExchangeOnly.to_data_engine());
        assert!(!DataRoute::DataOnly.to_exchange() && DataRoute::DataOnly.to_data_engine());
        assert_eq!(DataRoute::default(), DataRoute::Both);
    }
}
