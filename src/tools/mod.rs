pub mod attachments;
pub mod events;
pub mod locks;
pub mod messaging;
pub mod notes;
pub mod presence;
pub mod schema;
pub mod tasks;

use std::{
    collections::HashMap,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use rmcp::{
    ErrorData, Json, ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{
        JsonObject, ProgressNotificationParam, ProgressToken, ServerCapabilities, ServerConfig,
    },
    service::{Peer, RequestContext},
    tool, tool_handler, tool_router,
};
use schemars::JsonSchema;
use serde::Deserialize;
use sqlx::PgPool;

use crate::{
    auth::AuthCtx,
    events::EventHub,
    model::{DigestResult, WhoAmI},
    store,
};

pub const INSTRUCTIONS: &str = r#"
Shared coordination bus for a team of AI coding agents. Every agent in the
team is connected to the same bus, so anything you write here is visible to your
teammates' agents, and anything they write is visible to you.

Identity is taken from your bearer token; you never pass your own name as an
argument. Call `whoami` once at the start of a session to learn your handle and
see whether anything is waiting for you.

Four capabilities:

- Messaging. `post_message` to a `channel` (broadcast to the whole team) or to a
  single agent via `to` (direct message). `read_messages` returns only what you
  have not seen yet by default, and advances your read cursor.
- Task coordination. Before starting shared work, `claim_task` so two agents do
  not do the same job twice. Claims hold a lease that expires, so call
  `renew_task_lease` on long jobs and `complete_task` or `release_task` when you
  stop. An expired lease can be taken over by anyone. Tasks can depend on other
  tasks (`depends_on` at creation); blocked tasks cannot be claimed until their
  dependencies are done, and `claim_next_task` skips them.
- Locks. `acquire_lock` before touching a contended resource ("deploy:staging",
  "schema:users"); `release_lock` when finished. Lighter than a task, expires on
  its own.
- Presence. `heartbeat` publishes what you are working on (repo, branch, a short
  activity string). `list_agents` shows who else is active right now.
- Shared notes. `set_note` / `get_note` / `search_notes` are the team's durable
  memory: decisions, gotchas, deploy state. Prefer a note over repeating the
  same explanation in chat.
- Waiting. When you are blocked on teammates, call `wait_for_updates` instead of
  polling: it blocks until a relevant message/task/lock/note event arrives or
  the timeout passes. `team_digest` summarises the last hours of team activity —
  useful at session start to catch up.
- Attachments. Small files (diffs, logs, configs — max 256 KiB each) travel
  with messages (`post_message` `attachments`) or tasks (`attach_file`), and
  are fetched with `get_attachment`. Share the artifact itself instead of
  describing it.
- Asking. When you need an answer from a specific teammate to continue,
  `ask_agent` sends them the question and waits for the reply in one call. If
  you receive a question (a direct message marked `"question": true`), answer
  promptly with `post_message` (`to` the asker, `reply_to` the question id) —
  their agent is blocked waiting on you.

Conventions worth following: keep messages short and factual, scope notes by
repository name, and use task keys that a human would recognise.
"#;

/// The MCP server. Cheap to clone: `PgPool` and `EventHub` are `Arc`s
/// internally, and the tool router is rebuilt per session by the transport's
/// service factory.
#[derive(Clone)]
pub struct Bus {
    pub db: PgPool,
    pub hub: EventHub,
    pub tool_router: ToolRouter<Self>,
}

impl Bus {
    pub fn new(db: PgPool, hub: EventHub) -> Self {
        let tool_router = Self::core_router()
            + Self::messaging_router()
            + Self::tasks_router()
            + Self::presence_router()
            + Self::notes_router()
            + Self::locks_router()
            + Self::events_router()
            + Self::attachments_router();
        let mut tool_router = tool_router;
        let portable = portable_schemas(&tool_router);
        for (name, route) in tool_router.map.iter_mut() {
            if let Some(schema) = portable.get(name.as_ref()) {
                route.attr.input_schema = schema.clone();
            }
        }
        Self {
            db,
            hub,
            tool_router,
        }
    }
}

/// Every tool's input schema, lowered once per process by
/// [`schema::portable_input_schema`]. The transport builds a fresh `Bus` for
/// every request, so rewriting 29 schemas each time would be pure waste; the
/// derived schemas never change while the process runs.
fn portable_schemas(router: &ToolRouter<Bus>) -> &'static HashMap<String, Arc<JsonObject>> {
    static CACHE: OnceLock<HashMap<String, Arc<JsonObject>>> = OnceLock::new();
    CACHE.get_or_init(|| {
        router
            .map
            .iter()
            .map(|(name, route)| {
                (
                    name.to_string(),
                    Arc::new(schema::portable_input_schema(&route.attr.input_schema)),
                )
            })
            .collect()
    })
}

/// How often a long wait tells the client it is still alive, in milliseconds.
///
/// Without it a JSON-mode response sends nothing — not even headers — until
/// the wait ends, and HTTP clients give up on a silent request long before a
/// long wait is over (Node's fetch after 300 s without response headers).
/// The first progress notification switches the response to an event stream,
/// so headers go out after one period and every later beat keeps the body
/// moving. Only a request that carries a progress token gets beats: a
/// notification about a token the client never issued is a protocol error,
/// and a request without one keeps its plain JSON response.
///
/// A static rather than a constant so the integration suite can shorten it;
/// nothing in the server writes it.
#[doc(hidden)]
pub static PROGRESS_HEARTBEAT_MS: AtomicU64 = AtomicU64::new(60_000);

/// Periodic `notifications/progress` for one long-running tool call.
pub struct ProgressHeartbeat {
    beat: Option<(ProgressToken, Peer<rmcp::RoleServer>, tokio::time::Interval)>,
    started: tokio::time::Instant,
    total_secs: Option<f64>,
    what: &'static str,
}

impl ProgressHeartbeat {
    /// `total_secs` is the wait's timeout, reported as the progress total so a
    /// client can show how far along the wait is. Inert when the request
    /// carried no progress token.
    pub fn new(
        ctx: &RequestContext<rmcp::RoleServer>,
        what: &'static str,
        total_secs: Option<i64>,
    ) -> Self {
        let period = Duration::from_millis(PROGRESS_HEARTBEAT_MS.load(Ordering::Relaxed).max(1));
        let beat = ctx.meta.get_progress_token().map(|token| {
            let mut interval =
                tokio::time::interval_at(tokio::time::Instant::now() + period, period);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            (token, ctx.peer.clone(), interval)
        });
        Self {
            beat,
            started: tokio::time::Instant::now(),
            total_secs: total_secs.map(|t| t as f64),
            what,
        }
    }

    /// Resolves when the next beat is due; never, when there is no token.
    pub async fn tick(&mut self) {
        match &mut self.beat {
            Some((_, _, interval)) => {
                interval.tick().await;
            }
            None => std::future::pending().await,
        }
    }

    /// Send one progress notification. A failure only means the client went
    /// away; the wait itself carries on and ends as it would have.
    pub async fn send(&self) {
        let Some((token, peer, _)) = &self.beat else {
            return;
        };
        let elapsed = self.started.elapsed().as_secs();
        let mut param = ProgressNotificationParam::new(token.clone(), elapsed as f64)
            .with_message(format!("{} still waiting ({elapsed}s elapsed)", self.what));
        param.total = self.total_secs;
        if let Err(e) = peer.notify_progress(param).await {
            tracing::debug!(error = %e, "progress notification not delivered");
        }
    }
}

/// Pull the authenticated identity out of the HTTP request that carried this
/// tool call. The bearer middleware put it there; if it is missing, something
/// is routing around authentication and we refuse rather than guess.
pub fn auth_of(ctx: &RequestContext<rmcp::RoleServer>) -> Result<AuthCtx, ErrorData> {
    ctx.extensions
        .get::<http::request::Parts>()
        .and_then(|parts| parts.extensions.get::<AuthCtx>())
        .cloned()
        .ok_or_else(|| {
            ErrorData::invalid_request(
                "no authentication context on this request; the server is misconfigured",
                None,
            )
        })
}

#[tool_router(router = core_router, vis = "pub")]
impl Bus {
    #[tool(
        description = "Identify yourself on the bus: your agent handle, your team, \
                       how many unread direct messages you have and how many tasks \
                       you currently hold. Call this first in a session."
    )]
    async fn whoami(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
    ) -> Result<Json<WhoAmI>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(store::whoami(&self.db, &auth).await?))
    }

    #[tool(
        description = "Summarise the team's last hours: channel activity, tasks that moved, \
                       notes touched, who was around, active locks. Call it at session start \
                       to catch up, or to prepare a standup. Direct messages are excluded."
    )]
    async fn team_digest(
        &self,
        ctx: RequestContext<rmcp::RoleServer>,
        Parameters(args): Parameters<DigestArgs>,
    ) -> Result<Json<DigestResult>, ErrorData> {
        let auth = auth_of(&ctx)?;
        Ok(Json(
            store::digest::team_digest(
                &self.db,
                &auth,
                args.hours.unwrap_or(24),
                args.all_channels,
            )
            .await?,
        ))
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DigestArgs {
    /// Window to summarise, in hours (1-336). Defaults to 24.
    #[serde(default)]
    pub hours: Option<i64>,
    /// Summarise every channel instead of only the one this session works in.
    /// Has no effect when your session has no matching channel, where the
    /// digest already covers the whole team.
    #[serde(default)]
    pub all_channels: bool,
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for Bus {
    fn get_info(&self) -> ServerConfig {
        let mut info = ServerConfig::new(ServerCapabilities::builder().enable_tools().build());
        info.instructions = Some(INSTRUCTIONS.trim().to_string());
        info
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;

    /// Walk a schema and collect every construct a function-declaration
    /// validator (Gemini's, strictest of the providers) may reject.
    fn problems(node: &Value, path: &str, out: &mut Vec<String>) {
        match node {
            Value::Object(map) => {
                for key in ["$ref", "$defs", "definitions", "anyOf", "oneOf", "allOf"] {
                    if map.contains_key(key) {
                        out.push(format!("{path}: uses {key}"));
                    }
                }
                match map.get("type") {
                    Some(Value::String(t)) => {
                        if t == "array" {
                            match map.get("items") {
                                Some(Value::Object(items)) if items.contains_key("type") => {}
                                _ => out.push(format!("{path}: array without typed items")),
                            }
                        }
                    }
                    Some(other) => out.push(format!("{path}: non-string type {other}")),
                    None => out.push(format!("{path}: no type")),
                }
                if map.get("default").is_some_and(Value::is_null) {
                    out.push(format!("{path}: default null"));
                }
                if let Some(Value::Object(props)) = map.get("properties") {
                    for (name, prop) in props {
                        problems(prop, &format!("{path}.{name}"), out);
                    }
                }
                if let Some(items) = map.get("items") {
                    problems(items, &format!("{path}[]"), out);
                }
            }
            other => out.push(format!("{path}: not an object schema: {other}")),
        }
    }

    #[tokio::test]
    async fn every_tool_input_schema_is_portable() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused@127.0.0.1:1/unused")
            .unwrap();
        let bus = Bus::new(pool, EventHub::new());
        let tools = bus.tool_router.list_all();
        assert!(tools.len() >= 29, "only {} tools", tools.len());
        let mut found = Vec::new();
        for tool in &tools {
            let schema = Value::Object((*tool.input_schema).clone());
            problems(&schema, &tool.name, &mut found);
        }
        assert!(
            found.is_empty(),
            "non-portable input schemas:\n{}",
            found.join("\n")
        );

        // The fields that broke Gemini, spelled out.
        let get = |name: &str| {
            tools
                .iter()
                .find(|t| t.name == name)
                .map(|t| Value::Object((*t.input_schema).clone()))
                .unwrap()
        };
        let post = get("post_message");
        assert_eq!(post["properties"]["attachments"]["type"], "array");
        assert_eq!(post["properties"]["attachments"]["items"]["type"], "object");
        assert_eq!(
            post["properties"]["attachments"]["items"]["required"],
            serde_json::json!(["filename", "data_base64"])
        );
        assert_eq!(
            get("set_note")["properties"]["tags"]["items"]["type"],
            "string"
        );
        assert_eq!(
            get("wait_for_updates")["properties"]["kinds"]["items"]["type"],
            "string"
        );
    }
}
