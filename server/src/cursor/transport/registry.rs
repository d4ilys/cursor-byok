//! Maps request IDs to active transport handles.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::{
    sync::{mpsc, Mutex, Notify},
    time::Instant,
};

use bytes::Bytes;

use crate::{
    cursor::{
        conversation::ConversationRegistry, prompting::PromptCompiler,
        services::observability::CursorTraceService,
    },
    plugin::PluginRegistry,
    provider::Provider,
    search::WebCache,
    store::Store,
    Result,
};

use super::{OutputHub, TransportHandle};

#[derive(Clone)]
pub struct TransportRegistry {
    inner: Arc<RegistryInner>,
}

struct RegistryInner {
    local: Mutex<HashMap<String, LocalTransport>>,
    next_local_generation: AtomicU64,
    upstream: Mutex<HashMap<String, u64>>,
    /// Terminal frames of recently finished local transports, kept briefly so
    /// a client that reconnects after its run ended receives the end-stream
    /// instead of hanging on a request id that no longer has a route.
    closed: Mutex<HashMap<String, ClosedRoute>>,
    route_changed: Notify,
    store: Store,
    traces: CursorTraceService,
    web_cache: WebCache,
    plugins: Option<PluginRegistry>,
    provider: Arc<dyn Provider>,
    conversations: ConversationRegistry,
}

/// How long a `RunSSE` connection waits for its `BidiAppend` to create a
/// route. The client normally opens the stream and appends within
/// milliseconds; waiting forever turns a torn-down request id into an
/// unrecoverable "Reconnecting…" loop.
const ROUTE_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);

/// Shorter grace for a request id that already has a terminal frame: the
/// client is most likely reconnecting, so a new append (if any) arrives fast.
const ROUTE_REATTACH_GRACE: Duration = Duration::from_secs(2);

/// How long a finished transport keeps its terminal frame for reconnects.
const CLOSED_ROUTE_TTL: Duration = Duration::from_secs(300);

struct ClosedRoute {
    terminal: Bytes,
    closed_at: Instant,
}

#[derive(Clone)]
struct LocalTransport {
    generation: u64,
    handle: TransportHandle,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportRoute {
    Local,
    Upstream(u64),
    /// No live transport, but a recent one finished: replay its terminal frame.
    Closed,
    /// No live or recent transport exists for this request id.
    Unknown,
}

impl TransportRegistry {
    pub fn new(store: Store, provider: Arc<dyn Provider>, compiler: PromptCompiler) -> Self {
        Self::with_web_cache(store, provider, compiler, WebCache::default())
    }

    pub fn with_web_cache(
        store: Store,
        provider: Arc<dyn Provider>,
        compiler: PromptCompiler,
        web_cache: WebCache,
    ) -> Self {
        Self::build(store, provider, compiler, web_cache, None, None)
    }

    /// 附带本地 rules 目录的构造;编译请求上下文时会合并该目录下的 md 规则。
    pub fn with_local_rules(
        store: Store,
        provider: Arc<dyn Provider>,
        compiler: PromptCompiler,
        local_rules_dir: std::path::PathBuf,
    ) -> Self {
        Self::build(
            store,
            provider,
            compiler,
            WebCache::default(),
            None,
            Some(local_rules_dir),
        )
    }

    pub fn with_plugins(
        store: Store,
        provider: Arc<dyn Provider>,
        compiler: PromptCompiler,
        web_cache: WebCache,
        plugins: PluginRegistry,
        local_rules_dir: std::path::PathBuf,
    ) -> Self {
        Self::build(
            store,
            provider,
            compiler,
            web_cache,
            Some(plugins),
            Some(local_rules_dir),
        )
    }

    fn build(
        store: Store,
        provider: Arc<dyn Provider>,
        compiler: PromptCompiler,
        web_cache: WebCache,
        plugins: Option<PluginRegistry>,
        local_rules_dir: Option<std::path::PathBuf>,
    ) -> Self {
        Self {
            inner: Arc::new(RegistryInner {
                local: Mutex::new(HashMap::new()),
                next_local_generation: AtomicU64::new(1),
                upstream: Mutex::new(HashMap::new()),
                closed: Mutex::new(HashMap::new()),
                route_changed: Notify::new(),
                traces: CursorTraceService::new(store.clone()),
                conversations: ConversationRegistry::new(
                    store.clone(),
                    provider.clone(),
                    compiler,
                    web_cache.clone(),
                    local_rules_dir,
                ),
                provider,
                store,
                web_cache,
                plugins,
            }),
        }
    }

    pub fn store(&self) -> &Store {
        &self.inner.store
    }

    pub fn provider(&self) -> Arc<dyn Provider> {
        self.inner.provider.clone()
    }

    pub fn trace(
        &self,
        request_id: &str,
    ) -> crate::cursor::services::observability::CursorTraceRecorder {
        self.inner.traces.recorder(request_id)
    }

    pub fn web_cache(&self) -> &WebCache {
        &self.inner.web_cache
    }

    pub fn plugins(&self) -> Option<&PluginRegistry> {
        self.inner.plugins.as_ref()
    }

    pub fn conversations(&self) -> &ConversationRegistry {
        &self.inner.conversations
    }

    pub async fn get_or_create(&self, request_id: &str) -> Result<TransportHandle> {
        self.get_or_create_for_append(request_id, false).await
    }

    pub(crate) async fn get_or_create_for_append(
        &self,
        request_id: &str,
        replace_closing: bool,
    ) -> Result<TransportHandle> {
        let mut local = self.inner.local.lock().await;
        if let Some(transport) = local.get(request_id) {
            if transport.handle.accepting_appends() || !replace_closing {
                return Ok(transport.handle.clone());
            }
        }
        local.remove(request_id);
        // A new turn owns this request id now; the previous terminal frame
        // must no longer be replayable.
        let _ = self.inner.closed.lock().await.remove(request_id);
        let (commands, receiver) = mpsc::channel(128);
        let output = Arc::new(OutputHub::default());
        let trace = self.inner.traces.recorder(request_id);
        trace.resume();
        let handle = TransportHandle::new(request_id.into(), commands, output, trace);
        let generation = self
            .inner
            .next_local_generation
            .fetch_add(1, Ordering::Relaxed);
        local.insert(
            request_id.into(),
            LocalTransport {
                generation,
                handle: handle.clone(),
            },
        );
        drop(local);
        self.inner.route_changed.notify_waiters();
        self.inner
            .conversations
            .bind_transport(handle.clone(), receiver);

        let registry = Arc::downgrade(&self.inner);
        let request_id = request_id.to_string();
        let lifecycle = handle.clone();
        tokio::spawn(async move {
            lifecycle.wait_transport_closed().await;
            let Some(registry) = registry.upgrade() else {
                return;
            };
            let mut local = registry.local.lock().await;
            let owned = local
                .get(&request_id)
                .is_some_and(|transport| transport.generation == generation);
            if owned {
                local.remove(&request_id);
            }
            drop(local);
            if !owned {
                return;
            }
            let terminal = lifecycle
                .terminal_frame()
                .unwrap_or_else(crate::cursor::protocol::connect::encode_end_stream);
            let mut closed = registry.closed.lock().await;
            closed.retain(|_, route| route.closed_at.elapsed() < CLOSED_ROUTE_TTL);
            closed.insert(
                request_id,
                ClosedRoute {
                    terminal,
                    closed_at: Instant::now(),
                },
            );
            drop(closed);
            registry.route_changed.notify_waiters();
        });
        Ok(handle)
    }

    pub async fn local(&self, request_id: &str) -> Option<TransportHandle> {
        self.inner
            .local
            .lock()
            .await
            .get(request_id)
            .map(|transport| transport.handle.clone())
    }

    pub async fn mark_upstream(&self, request_id: &str) {
        let mut upstream = self.inner.upstream.lock().await;
        let generation = upstream.get(request_id).copied().unwrap_or_default() + 1;
        upstream.insert(request_id.into(), generation);
        drop(upstream);
        self.inner.route_changed.notify_waiters();
    }

    pub async fn upstream(&self, request_id: &str) -> bool {
        self.inner.upstream.lock().await.contains_key(request_id)
    }

    pub async fn wait_route(&self, request_id: &str) -> TransportRoute {
        let wait = if self.has_closed(request_id).await {
            ROUTE_REATTACH_GRACE
        } else {
            ROUTE_STARTUP_TIMEOUT
        };
        self.wait_route_within(request_id, wait).await
    }

    /// Waits at most `wait` for a route to appear.
    ///
    /// Returns [`TransportRoute::Closed`] when only a recent terminal frame
    /// remains, and [`TransportRoute::Unknown`] when the request id has no
    /// live or recent transport at all. Callers must answer the latter instead
    /// of waiting again, otherwise a torn-down conversation can never recover.
    pub async fn wait_route_within(&self, request_id: &str, wait: Duration) -> TransportRoute {
        let deadline = Instant::now() + wait;
        loop {
            let changed = self.inner.route_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(route) = self.current_route(request_id).await {
                return route;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_ok() {
                continue;
            }
            if let Some(route) = self.current_route(request_id).await {
                return route;
            }
            let mut closed = self.inner.closed.lock().await;
            closed.retain(|_, route| route.closed_at.elapsed() < CLOSED_ROUTE_TTL);
            return if closed.contains_key(request_id) {
                TransportRoute::Closed
            } else {
                TransportRoute::Unknown
            };
        }
    }

    async fn current_route(&self, request_id: &str) -> Option<TransportRoute> {
        if self.inner.local.lock().await.contains_key(request_id) {
            return Some(TransportRoute::Local);
        }
        self.inner
            .upstream
            .lock()
            .await
            .get(request_id)
            .copied()
            .map(TransportRoute::Upstream)
    }

    /// Terminal frame of a recently finished transport, replayed to a
    /// reconnecting client.
    pub async fn closed_terminal(&self, request_id: &str) -> Option<Bytes> {
        let mut closed = self.inner.closed.lock().await;
        closed.retain(|_, route| route.closed_at.elapsed() < CLOSED_ROUTE_TTL);
        closed.get(request_id).map(|route| route.terminal.clone())
    }

    async fn has_closed(&self, request_id: &str) -> bool {
        let mut closed = self.inner.closed.lock().await;
        closed.retain(|_, route| route.closed_at.elapsed() < CLOSED_ROUTE_TTL);
        closed.contains_key(request_id)
    }

    pub fn finish_upstream(&self, request_id: String, generation: u64) {
        let registry = self.clone();
        tokio::spawn(async move {
            let mut upstream = registry.inner.upstream.lock().await;
            if upstream.get(&request_id) == Some(&generation) {
                upstream.remove(&request_id);
            }
        });
    }

    pub async fn shutdown(&self) {
        self.inner.conversations.shutdown().await;
        let handles = std::mem::take(&mut *self.inner.local.lock().await);
        self.inner.upstream.lock().await.clear();
        self.inner.closed.lock().await.clear();
        for transport in handles.into_values() {
            transport.handle.disconnect().await;
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                transport.handle.wait_transport_closed(),
            )
            .await;
        }
    }
}
