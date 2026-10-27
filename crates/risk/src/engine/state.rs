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

//! A [`RiskEngine`]'s state as plain data, for saving a backtest and resuming it.

use nautilus_common::throttler::ThrottlerState;
use nautilus_model::{
    enums::TradingState,
    identifiers::{AccountId, ClientOrderId, InstrumentId},
    types::Money,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::{Reservation, RiskEngine};

/// Free balance an approved order uses until its account reflects the order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReservationState {
    /// The order.
    pub client_order_id: ClientOrderId,
    /// The account.
    pub account_id: AccountId,
    /// The amount reserved.
    pub amount: Money,
    /// Whether the account is a wallet.
    pub wallet: bool,
}

/// A [`RiskEngine`]'s replay state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskEngineState {
    /// The trading state.
    pub trading_state: TradingState,
    /// The maximum notional per order, sorted by instrument.
    pub max_notional_per_order: Vec<(InstrumentId, Decimal)>,
    /// The submit throttler's window.
    pub throttler_submit: ThrottlerState,
    /// The modify throttler's window.
    pub throttler_modify: ThrottlerState,
    /// The reservations, in order.
    pub reservations: Vec<ReservationState>,
    /// Commands received.
    pub command_count: u64,
    /// Events received.
    pub event_count: u64,
}

impl RiskEngine {
    /// Returns the engine's replay state.
    ///
    /// # Errors
    ///
    /// Returns an error if a throttler is limiting.
    pub fn export_state(&self) -> anyhow::Result<RiskEngineState> {
        let Self {
            clock: _,     // Rebuilt by setup: the kernel's clock
            cache: _,     // Rebuilt by setup: the kernel's cache
            portfolio: _, // Rebuilt by setup: the kernel's portfolio
            trading_state,
            config: _, // Rebuilt by setup
            max_notional_per_order,
            throttler_submit,
            throttler_modify,
            reservations,
            command_count,
            event_count,
        } = self;
        let mut max_notional_per_order: Vec<(InstrumentId, Decimal)> = max_notional_per_order
            .iter()
            .map(|(instrument_id, notional)| (*instrument_id, *notional))
            .collect();
        max_notional_per_order.sort_unstable_by_key(|(instrument_id, _)| *instrument_id);
        Ok(RiskEngineState {
            trading_state: *trading_state,
            max_notional_per_order,
            throttler_submit: throttler_submit.export_state()?,
            throttler_modify: throttler_modify.export_state()?,
            reservations: reservations
                .iter()
                .map(|reservation| {
                    let Reservation {
                        client_order_id,
                        account_id,
                        amount,
                        wallet,
                    } = reservation;
                    ReservationState {
                        client_order_id: *client_order_id,
                        account_id: *account_id,
                        amount: *amount,
                        wallet: *wallet,
                    }
                })
                .collect(),
            command_count: *command_count,
            event_count: *event_count,
        })
    }

    /// Overwrites the engine's replay state with `state`.
    ///
    /// # Errors
    ///
    /// Returns an error if a throttler window exceeds its limit.
    pub fn import_state(&mut self, state: RiskEngineState) -> anyhow::Result<()> {
        let RiskEngineState {
            trading_state,
            max_notional_per_order,
            throttler_submit,
            throttler_modify,
            reservations,
            command_count,
            event_count,
        } = state;
        self.trading_state = trading_state;
        self.max_notional_per_order = max_notional_per_order.into_iter().collect();
        self.throttler_submit.import_state(throttler_submit)?;
        self.throttler_modify.import_state(throttler_modify)?;
        self.reservations = reservations
            .into_iter()
            .map(|reservation| {
                let ReservationState {
                    client_order_id,
                    account_id,
                    amount,
                    wallet,
                } = reservation;
                Reservation {
                    client_order_id,
                    account_id,
                    amount,
                    wallet,
                }
            })
            .collect();
        self.command_count = command_count;
        self.event_count = event_count;
        Ok(())
    }
}
