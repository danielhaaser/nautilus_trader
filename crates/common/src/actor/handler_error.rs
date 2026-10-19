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

//! A hook for the errors data handlers return.
//!
//! A [`super::DataActor`] handler (`on_bar`, `on_book_deltas`, ...) returns `anyhow::Result`, and the
//! dispatch only logs an `Err`: the actor carries on, and its host cannot tell. This module lets the
//! host see each one (which component, which handler, the error) and decide: a backtest may fail
//! the run, a live node may stop. With no hook set, nothing changes but one lock per error.

use std::sync::Mutex;

use nautilus_model::identifiers::ComponentId;

/// One error a data handler returned.
#[derive(Debug)]
pub struct HandlerError<'a> {
    /// The actor's component id, when the dispatch method can name it (the historical-data and
    /// response handlers cannot).
    pub component: Option<ComponentId>,
    /// The handler that returned it (`on_bar`, ...), or `indicators` for an indicator update.
    pub handler: &'static str,
    /// The error, as returned.
    pub error: &'a anyhow::Error,
}

type Hook = Box<dyn Fn(&HandlerError<'_>) + Send + Sync>;

static HOOK: Mutex<Option<Hook>> = Mutex::new(None);

/// Sets the process's handler-error hook, replacing any earlier one. It is called on the
/// dispatching thread after the error is logged, and must not itself dispatch to an actor.
pub fn set_handler_error_hook(hook: impl Fn(&HandlerError<'_>) + Send + Sync + 'static) {
    *HOOK.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Box::new(hook));
}

/// Clears the process's handler-error hook.
pub fn clear_handler_error_hook() {
    *HOOK.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = None;
}

pub(crate) fn report(component: Option<ComponentId>, handler: &'static str, error: &anyhow::Error) {
    let guard = HOOK.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(hook) = guard.as_ref() {
        hook(&HandlerError {
            component,
            handler,
            error,
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use rstest::rstest;

    use super::*;

    /// Each error the hook saw: component, handler and error text.
    type Seen = Arc<Mutex<Vec<(Option<String>, &'static str, String)>>>;

    #[rstest]
    fn test_hook_sees_each_error_and_clears() {
        let seen: Seen = Arc::default();
        let keep = Arc::clone(&seen);
        set_handler_error_hook(move |e| {
            keep.lock().unwrap().push((
                e.component.map(|c| c.to_string()),
                e.handler,
                e.error.to_string(),
            ));
        });
        report(
            Some(ComponentId::from("ACTOR-001")),
            "on_bar",
            &anyhow::anyhow!("boom"),
        );
        clear_handler_error_hook();
        report(None, "on_data", &anyhow::anyhow!("unseen"));
        assert_eq!(
            *seen.lock().unwrap(),
            vec![(Some("ACTOR-001".to_string()), "on_bar", "boom".to_string())]
        );
    }
}
