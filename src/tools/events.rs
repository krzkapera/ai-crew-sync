use std::time::Duration;

use rmcp::{
    ErrorData, Json, handler::server::wrapper::Parameters, service::RequestContext, tool,
    tool_router,
};
use schemars::JsonSchema;
use serde::Deserialize;
use sqlx::PgPool;
use uuid::Uuid;

use super::{Bus, auth_of};
use crate::{
    auth::AuthCtx,
    events::BusEvent,
    model::{WaitEvent, WaitResult},
};

const DEFAULT_TIMEOUT_SECS: i64 = 25;
/// Floor only — no ceiling. This fork drops the upstream sub-minute cap: it
/// existed to protect a reverse proxy sitting between caller and server, which
/// this deployment does not have (direct local connection).
const MIN_TIMEOUT_SECS: i64 = 1;

#[derive(Debug, Deserialize, JsonSchema)]
pub struct WaitArgs {
    /// How long to wait before giving up, in seconds (default 25). No upper
    /// bound: pass a large value to wait indefinitely for a slow teammate.
    #[serde(default)]
    pub timeout_seconds: Option<i64>,
    /// Restrict to certain event kinds: any of "message", "task", "lock",
    /// "note". Omit to wake on anything relevant to you. Ignored when
    /// `task_key`/`task_keys` is set.
    #[serde(default)]
    pub kinds: Option<Vec<String>>,
    /// Wake on channel messages from every channel, not only the one this
    /// session works in. Ignored when your session has no matching channel,
    /// where every channel already wakes you. Direct messages, tasks, locks
    /// and notes always wake you either way.
    #[serde(default)]
    pub all_channels: bool,
    /// Wait for one specific task instead of anything on the bus: wakes only
    /// on events about the task with this key, and returns immediately if it
    /// is already `done` or `cancelled`. Overrides `kinds` and `all_channels`.
    /// Use this to block until a task you delegated is finished, then read it
    /// with get_task. Errors if no task has this key. Can be combined with
    /// `task_keys` — the wait then covers the union of both.
    #[serde(default)]
    pub task_key: Option<String>,
    /// Like `task_key`, but wait for whichever of several tasks finishes
    /// first ("one_of"): returns as soon as any key in this list is already,
    /// or becomes, `done`/`cancelled`. Check which one with get_task on each
    /// key, or look at the returned event's summary. Errors if any key does
    /// not exist.
    #[serde(default)]
    pub task_keys: Option<Vec<String>>,
}

async fn unread_dms(pool: &PgPool, auth: &AuthCtx) -> Result<i64, sqlx::Error> {
    // This session's inbox: what is addressed to it by name, plus what is
    // addressed to the person. Another window's mail is not this window's
    // backlog, and each keeps its own cursor.
    sqlx::query_scalar(
        r#"
        SELECT count(*)
        FROM messages m
        LEFT JOIN read_cursors c ON c.agent_id = $1 AND c.scope = $3
        WHERE m.recipient_agent_id = $1
          AND m.id > COALESCE(c.last_message_id, 0)
          AND (m.recipient_session IS NULL OR m.recipient_session = $2)
        "#,
    )
    .bind(auth.agent_id)
    .bind(&auth.session)
    .bind(crate::store::messaging::cursor_scope(
        "inbox",
        &auth.session,
    ))
    .fetch_one(pool)
    .await
}

async fn unread_anything(
    pool: &PgPool,
    auth: &AuthCtx,
    focus: Option<Uuid>,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        r#"
        SELECT count(*)
        FROM messages m
        LEFT JOIN read_cursors c ON c.agent_id = $1 AND c.scope = $4
        WHERE m.team_id = $2
          AND m.id > COALESCE(c.last_message_id, 0)
          -- Same rule as the wake filter: only this session's own messages
          -- are excluded, so a sibling window's announcement still counts.
          AND NOT (m.sender_agent_id = $1 AND COALESCE(m.sender_session, '') = $3)
          AND ((m.channel_id IS NOT NULL
                -- Same rule as the wake filter: an announcement counts as
                -- pending whatever this session is focused on. Reporting a
                -- backlog the wait would not wake for is a lie the caller
                -- cannot act on.
                AND ($5::uuid IS NULL OR m.channel_id = $5 OR m.announce))
               OR (m.recipient_agent_id = $1
                   AND (m.recipient_session IS NULL OR m.recipient_session = $3)))
        "#,
    )
    .bind(auth.agent_id)
    .bind(auth.team_id)
    .bind(&auth.session)
    .bind(crate::store::messaging::cursor_scope("all", &auth.session))
    .bind(focus)
    .fetch_one(pool)
    .await
}

/// Is this event inside the session's channel focus?
///
/// Only channel messages are filtered: a direct message, a task, a lock or a
/// note is not tied to a channel, and silencing those would hide work rather
/// than noise.
fn in_focus(event: &BusEvent, focus: Option<Uuid>) -> bool {
    // An announcement is the sender saying "this one is for everyone, even if
    // you are concentrating". Filtering it by channel would silence exactly
    // the message the flag exists to deliver.
    if event.is_announcement() {
        return true;
    }
    match (focus, event.channel_id()) {
        (Some(channel), Some(posted_in)) => channel == posted_in,
        _ => true,
    }
}

/// Turn a raw bus event into a one-line summary, resolving ids to names.
async fn describe(pool: &PgPool, event: &BusEvent) -> Option<WaitEvent> {
    match event.kind() {
        "message" => {
            let id = event.message_id()?;
            let row: (String, Option<String>, Option<String>, String) =
                sqlx::query_as::<_, (String, Option<String>, Option<String>, String)>(
                    r#"
                    SELECT s.name, ch.name, r.name, left(m.body, 200)
                    FROM messages m
                    JOIN agents s ON s.id = m.sender_agent_id
                    LEFT JOIN channels ch ON ch.id = m.channel_id
                    LEFT JOIN agents r ON r.id = m.recipient_agent_id
                    WHERE m.id = $1
                    "#,
                )
                .bind(id)
                .fetch_optional(pool)
                .await
                .ok()
                .flatten()?;
            let (sender, channel, recipient, body) = row;
            let target = channel
                .map(|c| format!("#{c}"))
                .or(recipient.map(|r| format!("@{r}")))
                .unwrap_or_default();
            Some(WaitEvent {
                kind: "message".into(),
                summary: format!("{sender} → {target}: {body}"),
            })
        }
        "task" => {
            let key = event.0.get("key").and_then(|v| v.as_str())?;
            let status = event
                .0
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("?");
            let holder = match event.0.get("claimed_by").and_then(|v| v.as_str()) {
                Some(uuid) => {
                    sqlx::query_scalar::<_, String>("SELECT name FROM agents WHERE id = $1::uuid")
                        .bind(uuid)
                        .fetch_optional(pool)
                        .await
                        .ok()
                        .flatten()
                        .map(|n| format!(" by {n}"))
                        .unwrap_or_default()
                }
                None => String::new(),
            };
            Some(WaitEvent {
                kind: "task".into(),
                summary: format!("task '{key}' is now {status}{holder}"),
            })
        }
        "lock" => {
            let name = event.0.get("name").and_then(|v| v.as_str())?;
            let what = event
                .0
                .get("event")
                .and_then(|v| v.as_str())
                .unwrap_or("changed");
            Some(WaitEvent {
                kind: "lock".into(),
                summary: format!("lock '{name}' {what}"),
            })
        }
        "note" => {
            let scope = event
                .0
                .get("scope")
                .and_then(|v| v.as_str())
                .unwrap_or("global");
            let key = event.0.get("key").and_then(|v| v.as_str())?;
            Some(WaitEvent {
                kind: "note".into(),
                summary: format!("note {scope}/{key} was updated"),
            })
        }
        _ => None,
    }
}

fn suggestion_for(events: &[WaitEvent], unread: i64) -> String {
    if events.iter().any(|e| e.kind == "message") || unread > 0 {
        "Call read_messages to fetch the new messages.".into()
    } else if events.iter().any(|e| e.kind == "task") {
        "Call list_tasks (or get_task) to see what changed.".into()
    } else if events.iter().any(|e| e.kind == "lock") {
        "Call list_locks (or retry acquire_lock) now.".into()
    } else if events.iter().any(|e| e.kind == "note") {
        "Call get_note to read the updated note.".into()
    } else {
        "Nothing happened; do other work or wait again.".into()
    }
}

#[tool_router(router = events_router, vis = "pub")]
impl Bus {
    #[tool(
        description = "Block until something happens on the bus that concerns you (a message \
                       arrives, a task changes state, a lock is released, a note is updated) or \
                       the timeout elapses. Use this instead of polling read_messages in a loop: \
                       call it when you are waiting on teammates and idle. Returns immediately \
                       if you already have unread messages. Pass task_key to wait for one \
                       specific task to finish instead, or task_keys to wait for whichever of \
                       several finishes first."
    )]
    async fn wait_for_updates(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<WaitArgs>,
    ) -> Result<Json<WaitResult>, ErrorData> {
        let auth = auth_of(&ctx)?;
        let timeout = args
            .timeout_seconds
            .unwrap_or(DEFAULT_TIMEOUT_SECS)
            .max(MIN_TIMEOUT_SECS);
        // task_key and task_keys are the same feature at heart — "wait for
        // one of these keys to finish" — so both are folded into a single
        // set up front; task_key is just the one-element case of task_keys.
        let task_keys: Vec<&str> = args
            .task_key
            .iter()
            .map(String::as_str)
            .chain(args.task_keys.iter().flatten().map(String::as_str))
            .collect();
        let waiting_on_tasks = !task_keys.is_empty();
        let kind_filter: Option<Vec<String>> = args
            .kinds
            .map(|ks| ks.into_iter().map(|k| k.trim().to_lowercase()).collect());
        let wants = |kind: &str| {
            // A task_key/task_keys wait only ever cares about those tasks'
            // events; it replaces whatever `kinds` was asked for rather than
            // adding to it, since a chat message is not an answer to "is it
            // done".
            if waiting_on_tasks {
                return kind == "task";
            }
            kind_filter
                .as_ref()
                .map(|ks| ks.iter().any(|k| k == kind))
                .unwrap_or(true)
        };
        let matches_task_key = |event: &BusEvent| {
            !waiting_on_tasks
                || event
                    .0
                    .get("key")
                    .and_then(|v| v.as_str())
                    .is_some_and(|k| task_keys.contains(&k))
        };

        // The channel this session works in, when it has one and the caller
        // has not asked for the whole team. A window working on market-data
        // should not be woken by core-manager chatter — that is half the point
        // of naming a session after a repository.
        let focus = match args.all_channels {
            true => None,
            false => crate::store::messaging::default_channel(&self.db, &auth)
                .await?
                .map(|(id, _)| id),
        };

        // Subscribe before checking the database so nothing slips between the
        // check and the wait.
        let mut rx = self.hub.subscribe();

        // Same rule as the wake filter below: reporting a backlog the wait
        // would not have woken for is a lie the caller cannot act on.
        let pending = unread_anything(&self.db, &auth, focus).await.map_err(|e| {
            tracing::error!(error = %e, "unread check failed");
            ErrorData::internal_error("database error", None)
        })?;
        if pending > 0 && wants("message") {
            let dms = unread_dms(&self.db, &auth).await.unwrap_or(0);
            return Ok(Json(WaitResult {
                woke: true,
                timed_out: false,
                events: vec![WaitEvent {
                    kind: "message".into(),
                    summary: format!("{pending} unread message(s) already waiting"),
                }],
                unread_direct_messages: dms,
                suggestion: "Call read_messages to fetch them.".into(),
            }));
        }

        // Same race-safety rule as the message check above: rx is already
        // subscribed, so a completion that lands between this read and the
        // loop below is still caught by the loop rather than lost.
        for key in task_keys.iter().copied() {
            let detail = crate::store::tasks::get_task(&self.db, &auth, key).await?;
            if matches!(detail.task.status.as_str(), "done" | "cancelled") {
                let holder = detail
                    .task
                    .claimed_by
                    .as_deref()
                    .map(|n| format!(" by {n}"))
                    .unwrap_or_default();
                return Ok(Json(WaitResult {
                    woke: true,
                    timed_out: false,
                    events: vec![WaitEvent {
                        kind: "task".into(),
                        summary: format!("task '{key}' is already {}{holder}", detail.task.status),
                    }],
                    unread_direct_messages: unread_dms(&self.db, &auth).await.unwrap_or(0),
                    suggestion: "Call get_task to see the result.".into(),
                }));
            }
        }

        let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout as u64);
        let mut events: Vec<WaitEvent> = Vec::new();

        while events.is_empty() {
            let event = tokio::select! {
                _ = tokio::time::sleep_until(deadline) => break,
                recv = rx.recv() => match recv {
                    Ok(ev) => ev,
                    // Lagged: we missed events; report a generic wake so the
                    // caller re-syncs from the database.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        events.push(WaitEvent {
                            kind: "unknown".into(),
                            summary: "event stream lagged; re-check the bus".into(),
                        });
                        break;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                },
            };

            if !event.visible_to(auth.team_id, auth.agent_id, &auth.session)
                || !wants(event.kind())
                || !in_focus(&event, focus)
                || !matches_task_key(&event)
            {
                continue;
            }
            // Your own messages are not news to you — but "you" is this
            // session, not the person. Excluding by agent alone meant an
            // announcement from your general window never woke the repository
            // windows it was written for, which is the coordination pattern
            // sessions exist to enable.
            if event.kind() == "message"
                && event.sender_agent_id() == Some(auth.agent_id)
                && event.sender_session().unwrap_or("") == auth.session
            {
                continue;
            }
            if let Some(described) = describe(&self.db, &event).await {
                events.push(described);
                // Grace window: batch events that arrive together.
                let grace = tokio::time::Instant::now() + Duration::from_millis(150);
                while let Ok(Ok(more)) = tokio::time::timeout_at(grace, rx.recv()).await {
                    if more.visible_to(auth.team_id, auth.agent_id, &auth.session)
                        && in_focus(&more, focus)
                        && wants(more.kind())
                        && matches_task_key(&more)
                        && !(more.kind() == "message"
                            && more.sender_agent_id() == Some(auth.agent_id))
                        && let Some(d) = describe(&self.db, &more).await
                    {
                        events.push(d);
                    }
                }
            }
        }

        let unread = unread_dms(&self.db, &auth).await.unwrap_or(0);
        let woke = !events.is_empty();
        Ok(Json(WaitResult {
            woke,
            timed_out: !woke,
            suggestion: suggestion_for(&events, unread),
            events,
            unread_direct_messages: unread,
        }))
    }
}
