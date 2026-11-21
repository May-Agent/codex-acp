//! Server configuration and pluggable `codex app-server` connector traits.

use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use aither_acp::Implementation;
use codex_sdk::{AnyServerRequestHandler, AppServer, ServerRequestHandler, SpawnConfig};
use futures_lite::io::{AsyncRead, AsyncWrite};

/// Connected [`AppServer`] paired with its companion I/O driver future.
pub struct ConnectedAppServer {
    /// Cloneable `codex-sdk` client handle for app-server requests and events.
    pub server: AppServer,
    pub(crate) driver: Pin<Box<dyn Future<Output = ()> + Send>>,
}

impl fmt::Debug for ConnectedAppServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectedAppServer")
            .field("server", &self.server)
            .finish_non_exhaustive()
    }
}

impl ConnectedAppServer {
    /// Pair an existing [`AppServer`] with its companion `driver` future.
    #[must_use]
    pub fn new<F>(server: AppServer, driver: F) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Self {
            server,
            driver: Box::pin(driver),
        }
    }

    /// Attach an [`AppServer`] directly to an `AsyncRead + AsyncWrite` stream
    /// pair (for example an in-memory mock `codex app-server` in tests).
    #[must_use]
    pub fn attach<R, W, H>(read: R, write: W, handler: H) -> Self
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
        H: ServerRequestHandler,
    {
        let (server, driver) = AppServer::attach(read, write, handler);
        Self::new(server, driver)
    }
}

/// Factory that launches or attaches a [`ConnectedAppServer`] for a
/// [`CodexAgent`](crate::CodexAgent) connection.
///
/// Production servers use [`ProcessAppServerConnector`] (enabled by the default
/// `process` cargo feature), while deterministic integration tests inject an
/// in-memory mock `codex app-server` via [`CodexAcpConfig::connector`].
pub trait AppServerConnector: Send + Sync + 'static {
    /// Establish a `codex app-server` connection from `config`, routing
    /// server-initiated requests (approvals, elicitations) to `handler`.
    fn connect(
        &self,
        config: SpawnConfig,
        handler: AnyServerRequestHandler,
    ) -> impl Future<Output = codex_sdk::Result<ConnectedAppServer>> + Send;
}

impl<F, Fut> AppServerConnector for F
where
    F: Fn(SpawnConfig, AnyServerRequestHandler) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = codex_sdk::Result<ConnectedAppServer>> + Send + 'static,
{
    fn connect(
        &self,
        config: SpawnConfig,
        handler: AnyServerRequestHandler,
    ) -> impl Future<Output = codex_sdk::Result<ConnectedAppServer>> + Send {
        (self)(config, handler)
    }
}

/// Default [`AppServerConnector`] that spawns a local `codex app-server` child
/// process via [`AppServer::spawn`].
#[cfg(feature = "process")]
#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessAppServerConnector;

#[cfg(feature = "process")]
impl AppServerConnector for ProcessAppServerConnector {
    fn connect(
        &self,
        config: SpawnConfig,
        handler: AnyServerRequestHandler,
    ) -> impl Future<Output = codex_sdk::Result<ConnectedAppServer>> + Send {
        let res = AppServer::spawn(&config, handler)
            .map(|(server, driver)| ConnectedAppServer::new(server, driver));
        std::future::ready(res)
    }
}

#[cfg(not(feature = "process"))]
#[derive(Debug, Clone, Copy, Default)]
struct DisabledProcessConnector;

#[cfg(not(feature = "process"))]
impl AppServerConnector for DisabledProcessConnector {
    fn connect(
        &self,
        _config: SpawnConfig,
        _handler: AnyServerRequestHandler,
    ) -> impl Future<Output = codex_sdk::Result<ConnectedAppServer>> + Send {
        std::future::ready(Err(codex_sdk::SdkError::Protocol(
            "spawning a `codex` child process requires the `process` cargo feature; configure a custom `AppServerConnector` via `CodexAcpConfig::connector`"
                .to_string(),
        )))
    }
}

trait AppServerConnectorImpl: Send + Sync + 'static {
    fn connect_dyn<'a>(
        &'a self,
        config: SpawnConfig,
        handler: AnyServerRequestHandler,
    ) -> Pin<Box<dyn Future<Output = codex_sdk::Result<ConnectedAppServer>> + Send + 'a>>;
}

impl<C: AppServerConnector> AppServerConnectorImpl for C {
    fn connect_dyn<'a>(
        &'a self,
        config: SpawnConfig,
        handler: AnyServerRequestHandler,
    ) -> Pin<Box<dyn Future<Output = codex_sdk::Result<ConnectedAppServer>> + Send + 'a>> {
        Box::pin(self.connect(config, handler))
    }
}

/// Type-erased wrapper around any [`AppServerConnector`], keeping boxed futures
/// strictly internal.
#[derive(Clone)]
pub struct AnyAppServerConnector {
    inner: Arc<dyn AppServerConnectorImpl>,
}

impl fmt::Debug for AnyAppServerConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AnyAppServerConnector")
            .finish_non_exhaustive()
    }
}

impl AnyAppServerConnector {
    /// Erase the concrete type of `connector` behind [`AnyAppServerConnector`].
    #[must_use]
    pub fn new<C: AppServerConnector>(connector: C) -> Self {
        Self {
            inner: Arc::new(connector),
        }
    }
}

impl AppServerConnector for AnyAppServerConnector {
    fn connect(
        &self,
        config: SpawnConfig,
        handler: AnyServerRequestHandler,
    ) -> impl Future<Output = codex_sdk::Result<ConnectedAppServer>> + Send {
        let inner = Arc::clone(&self.inner);
        async move { inner.connect_dyn(config, handler).await }
    }
}

fn default_connector() -> AnyAppServerConnector {
    #[cfg(feature = "process")]
    {
        AnyAppServerConnector::new(ProcessAppServerConnector)
    }
    #[cfg(not(feature = "process"))]
    {
        AnyAppServerConnector::new(DisabledProcessConnector)
    }
}

/// Configuration for [`CodexAgent`](crate::CodexAgent), [`serve`](crate::serve),
/// and `serve_stdio`.
#[derive(Debug, Clone)]
pub struct CodexAcpConfig {
    /// Base [`SpawnConfig`] used to launch `codex app-server` and perform its
    /// `initialize` handshake.
    pub spawn: SpawnConfig,
    /// Optional initial ACP session mode identifier (`"read-only"`,
    /// `"workspace-write"`, `"agent"`, or `"agent-full-access"`). Defaults to
    /// `"agent"`.
    pub default_mode: Option<String>,
    /// Agent [`Implementation`] metadata advertised in ACP `initialize`.
    pub agent_info: Implementation,
    pub(crate) connector: AnyAppServerConnector,
}

impl Default for CodexAcpConfig {
    fn default() -> Self {
        let mut info = Implementation::new(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
        info.title = Some("Codex".to_string());
        Self {
            spawn: SpawnConfig::new()
                .client_info(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
                .experimental_api(true),
            default_mode: None,
            agent_info: info,
            connector: default_connector(),
        }
    }
}

impl CodexAcpConfig {
    /// Construct a default [`CodexAcpConfig`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the [`SpawnConfig`] used when launching `codex app-server`.
    #[must_use]
    pub fn spawn(mut self, spawn: SpawnConfig) -> Self {
        self.spawn = spawn;
        self
    }

    /// Override the `codex` executable path on [`Self::spawn`].
    #[must_use]
    pub fn codex_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.spawn.codex_path = Some(path.into());
        self
    }

    /// Set the default ACP session mode identifier (`"read-only"`,
    /// `"workspace-write"`, `"agent"`, or `"agent-full-access"`).
    #[must_use]
    pub fn default_mode(mut self, mode: impl Into<String>) -> Self {
        self.default_mode = Some(mode.into());
        self
    }

    /// Override the [`Implementation`] metadata advertised in ACP `initialize`.
    #[must_use]
    pub fn agent_info(mut self, info: Implementation) -> Self {
        self.agent_info = info;
        self
    }

    /// Inject a custom [`AppServerConnector`] (for example to attach an
    /// in-memory mock `codex app-server` in integration tests).
    #[must_use]
    pub fn connector<C: AppServerConnector>(mut self, connector: C) -> Self {
        self.connector = AnyAppServerConnector::new(connector);
        self
    }
}
