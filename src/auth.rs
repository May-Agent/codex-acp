//! Authentication methods (`chat-gpt`, `chat-gpt-device-code`, `api-key`,
//! `gateway`), credential state, and `_auth/status_update` notifications.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use aither_acp::domain::ids::AuthMethodId;
use aither_acp::domain::{AgentAuth, AgentCapabilities, AuthKind, AuthMethod, Open};
use aither_acp::ext::auth_status::{
    self, AuthStatus, AuthStatusAccount, AuthStatusKind, CAPABILITY as AUTH_STATUS_CAPABILITY,
    METHOD as AUTH_STATUS_METHOD,
};
use aither_acp::ext::steering::CAPABILITY as STEERING_CAPABILITY;
use aither_acp::rpc::Peer;
use aither_acp::server::AuthRequest;
use aither_acp::vendor::codex::AuthenticationStatus;
use aither_acp::vendor::codex::auth::{
    API_KEY, CHAT_GPT, CHAT_GPT_DEVICE_CODE, GATEWAY, GATEWAY_META,
};
use codex_codes::{Account, GetAccountParams, Notification, PlanType, protocol::methods};
use codex_sdk::{AppServer, METHOD_NOT_FOUND_CODE, SdkError, SpawnConfig, ThreadEvent};
use serde::Deserialize;
use serde_json::{Value, json};
use tracing::info;

use crate::error::{CodexAcpError, Result};

const VENDOR_OPENAI: &str = "OpenAI";

/// Custom model gateway configuration supplied via `auth/login` (`gateway`).
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    /// Gateway base URL.
    pub base_url: String,
    /// Extra HTTP headers sent to the gateway.
    pub headers: HashMap<String, String>,
    /// Human-readable gateway provider name.
    pub provider_name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InboundGatewayMeta {
    base_url: String,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default)]
    provider_name: Option<String>,
}

/// Mutable connection-level authentication state tracked by [`crate::CodexAgent`].
#[derive(Debug, Clone)]
pub struct AuthState {
    logged_out: bool,
    gateway: Option<GatewayConfig>,
    status: AuthStatus,
    legacy: AuthenticationStatus,
}

impl AuthState {
    /// Initialize [`AuthState`] from the base [`SpawnConfig`] and process environment.
    pub(crate) fn from_spawn(spawn: &SpawnConfig) -> Self {
        let configured_key = spawn
            .env
            .get("CODEX_API_KEY")
            .or_else(|| spawn.env.get("OPENAI_API_KEY"))
            .filter(|v| !v.trim().is_empty());

        if configured_key.is_some() {
            return Self {
                logged_out: false,
                gateway: None,
                status: build_status(AuthStatusKind::ApiKey, Some("OpenAI API Key"), None, None),
                legacy: AuthenticationStatus::ApiKey,
            };
        }

        let ambient_env = ["CODEX_API_KEY", "OPENAI_API_KEY"]
            .into_iter()
            .find(|k| std::env::var_os(k).is_some_and(|v| !v.is_empty()));

        if let Some(env_name) = ambient_env {
            return Self {
                logged_out: false,
                gateway: None,
                status: build_status(AuthStatusKind::External, Some(env_name), None, None),
                legacy: AuthenticationStatus::ApiKey,
            };
        }

        Self {
            logged_out: false,
            gateway: None,
            status: build_status(AuthStatusKind::None, None, None, None),
            legacy: AuthenticationStatus::Unauthenticated,
        }
    }

    /// Snapshot the current [`AuthStatus`].
    pub(crate) fn status(&self) -> AuthStatus {
        self.status.clone()
    }

    /// Snapshot the current legacy [`AuthenticationStatus`].
    pub(crate) fn legacy_status(&self) -> AuthenticationStatus {
        self.legacy.clone()
    }

    /// Active gateway configuration, if authenticated via `gateway`.
    pub(crate) const fn gateway(&self) -> Option<&GatewayConfig> {
        self.gateway.as_ref()
    }

    /// Merge active custom gateway settings into a session `config` map.
    pub(crate) fn apply_gateway_to_config(&self, config_map: &mut serde_json::Map<String, Value>) {
        let Some(gw) = &self.gateway else {
            return;
        };
        config_map.insert("model_provider".to_string(), json!("gateway"));
        let mut gw_entry = json!({
            "name": gw.provider_name,
            "base_url": gw.base_url,
            "wire_api": "responses",
        });
        if !gw.headers.is_empty()
            && let Some(obj) = gw_entry.as_object_mut()
        {
            obj.insert(
                "http_headers".to_string(),
                serde_json::to_value(&gw.headers).unwrap_or_else(|_| json!({})),
            );
        }
        config_map.insert(
            "model_providers".to_string(),
            json!({ "gateway": gw_entry }),
        );
    }

    fn set_api_key(&mut self) -> AuthStatus {
        self.logged_out = false;
        self.gateway = None;
        self.legacy = AuthenticationStatus::ApiKey;
        self.status = build_status(AuthStatusKind::ApiKey, Some("OpenAI API Key"), None, None);
        self.status.clone()
    }

    fn set_chatgpt(&mut self, email: Option<String>, plan: Option<String>) -> AuthStatus {
        self.logged_out = false;
        self.gateway = None;
        let email_label = email.clone().unwrap_or_else(|| "ChatGPT".to_string());
        self.legacy = AuthenticationStatus::ChatGpt {
            email: email.clone().unwrap_or_default(),
        };
        let account = (email.is_some() || plan.is_some()).then_some(AuthStatusAccount {
            email,
            organization: None,
            plan,
            extra: BTreeMap::new(),
        });
        self.status = build_status(AuthStatusKind::Account, Some(&email_label), None, account);
        self.status.clone()
    }

    fn set_gateway(&mut self, gw: GatewayConfig) -> AuthStatus {
        let label = gw.provider_name.clone();
        let detail = gw.base_url.clone();
        self.logged_out = false;
        self.legacy = AuthenticationStatus::Gateway {
            name: label.clone(),
        };
        self.gateway = Some(gw);
        self.status = build_status(AuthStatusKind::Gateway, Some(&label), Some(&detail), None);
        self.status.clone()
    }

    /// Reset credentials and mark the agent signed out.
    pub(crate) fn set_logged_out(&mut self) -> AuthStatus {
        self.logged_out = true;
        self.gateway = None;
        self.legacy = AuthenticationStatus::Unauthenticated;
        self.status = build_status(AuthStatusKind::None, None, None, None);
        self.status.clone()
    }
}

/// Attach `auth.logout`, `_meta.authStatus`, and `_meta.steering` onto
/// [`AgentCapabilities`].
pub fn apply_capabilities(caps: &mut AgentCapabilities) {
    caps.auth = Some(json!({"logout": true}));
    let mut cap_meta = caps
        .meta
        .take()
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default();
    cap_meta.insert(AUTH_STATUS_CAPABILITY.to_string(), json!({}));
    cap_meta.insert(STEERING_CAPABILITY.to_string(), json!({"supported": true}));
    caps.meta = Some(Value::Object(cap_meta));
}

/// Build the list of [`AuthMethod`]s advertised in ACP `initialize`.
#[must_use]
pub fn auth_methods() -> Vec<AuthMethod> {
    [
        (CHAT_GPT, "ChatGPT", "Sign in with a ChatGPT account"),
        (
            CHAT_GPT_DEVICE_CODE,
            "ChatGPT (Device Code)",
            "Sign in with a ChatGPT account using a one-time device code",
        ),
        (
            API_KEY,
            "API key",
            "Authenticate with an OpenAI API key (`CODEX_API_KEY` or `OPENAI_API_KEY`)",
        ),
        (
            GATEWAY,
            "Model Gateway",
            "Authenticate through a custom OpenAI-compatible model gateway",
        ),
    ]
    .into_iter()
    .map(|(id, name, desc)| AuthMethod {
        method_id: AuthMethodId::from_string(id),
        name: name.to_string(),
        description: Some(desc.to_string()),
        kind: Open::Known(AuthKind::Agent(AgentAuth::default())),
        meta: None,
        extra: BTreeMap::new(),
    })
    .collect()
}

/// Handle an [`AuthRequest`] (`auth/login` or `auth/logout`) against `app_server`
/// and emit `_auth/status_update` when `peer` is attached.
///
/// # Errors
///
/// Propagates credential validation or app-server RPC errors.
pub async fn handle_auth_request(
    app_server: &AppServer,
    auth_state: &Mutex<AuthState>,
    spawn: &SpawnConfig,
    peer: Option<&Peer>,
    request: AuthRequest,
) -> Result<()> {
    match request {
        AuthRequest::Login(req) => {
            authenticate_with_server(
                app_server,
                auth_state,
                spawn,
                peer,
                req.method_id.as_str(),
                req.meta.as_ref(),
            )
            .await
        }
        AuthRequest::Logout(_) => logout_with_server(app_server, auth_state, peer).await,
    }
}

async fn authenticate_with_server(
    app_server: &AppServer,
    auth_state: &Mutex<AuthState>,
    spawn: &SpawnConfig,
    peer: Option<&Peer>,
    method_id: &str,
    meta: Option<&Value>,
) -> Result<()> {
    match method_id {
        API_KEY => {
            let key = resolve_api_key(meta, spawn)?;
            app_server
                .call::<_, Value>(
                    methods::ACCOUNT_LOGIN_START,
                    &json!({"type": "apiKey", "apiKey": key}),
                )
                .await?;
            let status = auth_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .set_api_key();
            if let Some(peer) = peer {
                notify_auth_status(peer, &status).await?;
            }
            Ok(())
        }
        method @ (CHAT_GPT | CHAT_GPT_DEVICE_CODE) => {
            authenticate_chatgpt_flow(app_server, auth_state, peer, method, meta).await
        }
        GATEWAY => authenticate_gateway_flow(app_server, auth_state, peer, meta).await,
        other => Err(CodexAcpError::InvalidParams(format!(
            "unsupported auth method `{other}`"
        ))),
    }
}

async fn authenticate_chatgpt_flow(
    app_server: &AppServer,
    auth_state: &Mutex<AuthState>,
    peer: Option<&Peer>,
    method: &str,
    req_meta: Option<&Value>,
) -> Result<()> {
    let login_type = if method == CHAT_GPT_DEVICE_CODE {
        "chatgptDeviceCode"
    } else {
        "chatgpt"
    };
    let rx = app_server.global_events();
    let resp: Value = app_server
        .call(methods::ACCOUNT_LOGIN_START, &json!({"type": login_type}))
        .await?;

    let auth_url = resp
        .get("authUrl")
        .or_else(|| resp.get("verificationUrl"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let login_id = resp
        .get("loginId")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);

    info!(url = %auth_url, method, "started ChatGPT login flow");

    while let Ok(ev) = rx.recv().await {
        if let Some((completed_id, success, error)) = extract_login_completed(&ev)
            && (login_id.is_none() || completed_id.is_none() || completed_id == login_id)
        {
            if !success {
                return Err(CodexAcpError::Auth(
                    error.unwrap_or_else(|| "ChatGPT login failed".to_string()),
                ));
            }
            let (read_email, read_plan) = fetch_account_details(app_server).await;
            let fallback_email = req_meta
                .and_then(|m| m.get("email"))
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(ToOwned::to_owned);
            let status = auth_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .set_chatgpt(read_email.or(fallback_email), read_plan);
            if let Some(peer) = peer {
                notify_auth_status(peer, &status).await?;
            }
            return Ok(());
        }
    }

    Err(CodexAcpError::Sdk(SdkError::Closed))
}

async fn authenticate_gateway_flow(
    app_server: &AppServer,
    auth_state: &Mutex<AuthState>,
    peer: Option<&Peer>,
    req_meta: Option<&Value>,
) -> Result<()> {
    let gw_val = req_meta.and_then(|m| m.get(GATEWAY_META)).ok_or_else(|| {
        CodexAcpError::InvalidParams("gateway authentication requires `_meta.gateway`".to_string())
    })?;
    let parsed: InboundGatewayMeta = serde_json::from_value(gw_val.clone()).map_err(|err| {
        CodexAcpError::InvalidParams(format!("invalid `_meta.gateway` payload: {err}"))
    })?;
    let base_url = parsed.base_url.trim().to_string();
    if base_url.is_empty() {
        return Err(CodexAcpError::InvalidParams(
            "`_meta.gateway.baseUrl` must not be empty".to_string(),
        ));
    }
    let provider_name = parsed
        .provider_name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("Gateway")
        .to_string();

    let write_res = app_server
        .call::<_, Value>(
            methods::MODELPROVIDER_CONFIG_WRITE,
            &json!({
                "settings": {
                    "type": "customGateway",
                    "providerId": "gateway",
                    "name": provider_name,
                    "baseUrl": base_url,
                }
            }),
        )
        .await;

    match write_res {
        Ok(resp) => {
            let pid = resp
                .get("provider")
                .and_then(|p| p.get("id"))
                .and_then(Value::as_str)
                .unwrap_or("gateway");
            let _ = app_server
                .call::<_, Value>(
                    methods::MODELPROVIDER_DEFAULT_SET,
                    &json!({"providerId": pid}),
                )
                .await;
        }
        Err(SdkError::Remote(rem)) if rem.code == METHOD_NOT_FOUND_CODE => {}
        Err(err) => return Err(CodexAcpError::Sdk(err)),
    }

    let status = auth_state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .set_gateway(GatewayConfig {
            base_url,
            headers: parsed.headers,
            provider_name,
        });
    if let Some(peer) = peer {
        notify_auth_status(peer, &status).await?;
    }
    Ok(())
}

/// Sign out on `app_server`, reset [`AuthState`], and push `AuthStatusKind::None`.
///
/// # Errors
///
/// Propagates app-server RPC or transport notification errors.
pub async fn logout_with_server(
    app_server: &AppServer,
    auth_state: &Mutex<AuthState>,
    peer: Option<&Peer>,
) -> Result<()> {
    match app_server.account_logout().await {
        Ok(_) => {}
        Err(SdkError::Remote(rem)) if rem.code == METHOD_NOT_FOUND_CODE => {}
        Err(err) => return Err(CodexAcpError::Sdk(err)),
    }
    let status = auth_state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .set_logged_out();
    if let Some(peer) = peer {
        notify_auth_status(peer, &status).await?;
    }
    Ok(())
}

/// Query legacy `authentication/status` (`aither_acp::vendor::codex::AuthenticationStatus`).
///
/// # Errors
///
/// Propagates app-server RPC errors other than `method not found`.
pub async fn read_legacy_authentication_status(
    app_server: &AppServer,
    auth_state: &Mutex<AuthState>,
) -> Result<AuthenticationStatus> {
    {
        let guard = auth_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if guard.gateway().is_some() || guard.logged_out {
            return Ok(guard.legacy_status());
        }
    }

    match app_server
        .account_read(&GetAccountParams {
            refresh_token: None,
        })
        .await
    {
        Ok(resp) => match resp.account {
            Some(Account::Chatgpt { email, .. }) => Ok(AuthenticationStatus::ChatGpt {
                email: email.unwrap_or_default(),
            }),
            Some(_) => Ok(AuthenticationStatus::ApiKey),
            None => Ok(auth_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .legacy_status()),
        },
        Err(SdkError::Remote(rem)) if rem.code == METHOD_NOT_FOUND_CODE => Ok(auth_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .legacy_status()),
        Err(err) => Err(CodexAcpError::Sdk(err)),
    }
}

/// Push an `_auth/status_update` notification over `peer`.
///
/// # Errors
///
/// Propagates peer transport errors.
pub async fn notify_auth_status(peer: &Peer, status: &AuthStatus) -> Result<()> {
    let payload = json!({
        auth_status::CAPABILITY: status,
    });
    peer.notify(AUTH_STATUS_METHOD.as_str(), &payload).await?;
    Ok(())
}

/// Push an `_auth/status_update` notification through a session context.
///
/// # Errors
///
/// Propagates session notification transport errors.
pub async fn notify_auth_status_cx(
    cx: &aither_acp::SessionContext,
    status: &AuthStatus,
) -> Result<()> {
    let payload = json!({
        auth_status::CAPABILITY: status,
    });
    cx.notify(AUTH_STATUS_METHOD.as_str(), Some(payload))
        .await
        .map_err(|e| CodexAcpError::Server(e.into()))?;
    Ok(())
}

fn resolve_api_key(meta: Option<&Value>, spawn: &SpawnConfig) -> Result<String> {
    if let Some(key) = meta
        .and_then(Value::as_object)
        .and_then(|obj| {
            obj.get("apiKey")
                .or_else(|| obj.get("api_key"))
                .or_else(|| obj.get("token"))
        })
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        return Ok(key.to_string());
    }

    if let Some(key) = spawn
        .env
        .get("CODEX_API_KEY")
        .or_else(|| spawn.env.get("OPENAI_API_KEY"))
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        return Ok(key.to_string());
    }

    for env_var in ["CODEX_API_KEY", "OPENAI_API_KEY"] {
        if let Ok(val) = std::env::var(env_var) {
            let trimmed = val.trim();
            if !trimmed.is_empty() {
                return Ok(trimmed.to_string());
            }
        }
    }

    Err(CodexAcpError::InvalidParams(
        "API key required in `_meta.apiKey`, `CODEX_API_KEY`, or `OPENAI_API_KEY`".to_string(),
    ))
}

fn extract_login_completed(ev: &ThreadEvent) -> Option<(Option<String>, bool, Option<String>)> {
    match ev {
        ThreadEvent::Notification(Notification::AccountLoginCompleted(n)) => {
            Some((n.login_id.clone(), n.success, n.error.clone()))
        }
        ThreadEvent::Notification(Notification::Unknown { method, params })
            if method == methods::ACCOUNT_LOGIN_COMPLETED =>
        {
            Some(extract_raw_login_completed(params.as_ref()?))
        }
        ThreadEvent::RawNotification { method, params }
            if method == methods::ACCOUNT_LOGIN_COMPLETED =>
        {
            Some(extract_raw_login_completed(params))
        }
        _ => None,
    }
}

fn extract_raw_login_completed(p: &Value) -> (Option<String>, bool, Option<String>) {
    let id = p
        .get("loginId")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let ok = p.get("success").and_then(Value::as_bool).unwrap_or(true);
    let err = p
        .get("error")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    (id, ok, err)
}

async fn fetch_account_details(app_server: &AppServer) -> (Option<String>, Option<String>) {
    let Ok(resp) = app_server
        .account_read(&GetAccountParams {
            refresh_token: None,
        })
        .await
    else {
        return (None, None);
    };
    if let Some(Account::Chatgpt {
        email, plan_type, ..
    }) = resp.account
    {
        let plan = match plan_type {
            PlanType::Free => "free".to_string(),
            PlanType::Plus => "plus".to_string(),
            PlanType::Pro => "pro".to_string(),
            PlanType::Team => "team".to_string(),
            PlanType::Business => "business".to_string(),
            PlanType::Enterprise => "enterprise".to_string(),
            PlanType::Edu => "edu".to_string(),
            other => serde_json::to_value(other)
                .ok()
                .and_then(|v| v.as_str().map(ToOwned::to_owned))
                .unwrap_or_else(|| "chatgpt".to_string()),
        };
        return (email, Some(plan));
    }
    (None, None)
}

fn build_status(
    kind: AuthStatusKind,
    label: Option<&str>,
    detail: Option<&str>,
    account: Option<AuthStatusAccount>,
) -> AuthStatus {
    AuthStatus {
        kind,
        label: label.map(ToOwned::to_owned),
        detail: detail.map(ToOwned::to_owned),
        account,
        vendor: Some(VENDOR_OPENAI.to_string()),
        extra: BTreeMap::new(),
    }
}
