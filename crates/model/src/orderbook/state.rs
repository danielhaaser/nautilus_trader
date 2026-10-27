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

//! An [`OrderBook`]'s complete state as plain data, for saving a backtest and resuming it.

use nautilus_core::UnixNanos;
use serde::{Deserialize, Serialize};

use crate::{
    data::BookOrder,
    enums::{BookType, OrderSide},
    identifiers::InstrumentId,
    orderbook::{OrderBook, ladder::BookLadder},
    types::Price,
};

/// One side of an [`OrderBookState`]: its levels in ladder order (best first), each its price
/// and its orders in FIFO order, and the code of an unfinished L1 batch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookSideState {
    /// The levels, best first, each its price and orders in FIFO order.
    pub levels: Vec<(Price, Vec<BookOrder>)>,
    /// The unfinished L1 batch state's code (0 none, 1 MBP, 2 snapshot).
    pub batch_state: u8,
}

/// An [`OrderBook`]'s complete state: every order of each side in level and FIFO order, and the
/// book's sequence, last update instant and update count.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderBookState {
    /// The book's instrument.
    pub instrument_id: InstrumentId,
    /// The book's type.
    pub book_type: BookType,
    /// The last event sequence number.
    pub sequence: u64,
    /// The instant of the last event applied.
    pub ts_last: UnixNanos,
    /// The count of updates applied.
    pub update_count: u64,
    /// The bid side.
    pub bids: BookSideState,
    /// The ask side.
    pub asks: BookSideState,
}

impl OrderBook {
    /// Returns the book's complete state.
    #[must_use]
    pub fn export_state(&self) -> OrderBookState {
        let Self {
            instrument_id,
            book_type,
            sequence,
            ts_last,
            update_count,
            bids,
            asks,
        } = self;
        let side = |ladder: &BookLadder| {
            let (levels, batch_state) = ladder.export_levels();
            BookSideState {
                levels,
                batch_state,
            }
        };
        OrderBookState {
            instrument_id: *instrument_id,
            book_type: *book_type,
            sequence: *sequence,
            ts_last: *ts_last,
            update_count: *update_count,
            bids: side(bids),
            asks: side(asks),
        }
    }

    /// Builds the book `state` describes, level by level and order by order.
    ///
    /// # Errors
    ///
    /// Returns an error if a side's levels are inconsistent (an order on the wrong side or at
    /// another price than its level, a repeated order ID or level, an empty level) or its batch
    /// state is invalid for the book type.
    pub fn from_state(state: OrderBookState) -> anyhow::Result<Self> {
        let OrderBookState {
            instrument_id,
            book_type,
            sequence,
            ts_last,
            update_count,
            bids,
            asks,
        } = state;
        Ok(Self {
            instrument_id,
            book_type,
            sequence,
            ts_last,
            update_count,
            bids: BookLadder::from_levels(
                OrderSide::Buy,
                book_type,
                bids.levels,
                bids.batch_state,
            )?,
            asks: BookLadder::from_levels(
                OrderSide::Sell,
                book_type,
                asks.levels,
                asks.batch_state,
            )?,
        })
    }
}
