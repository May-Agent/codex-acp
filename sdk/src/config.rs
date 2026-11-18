//! Process launch and `initialize` handshake configuration for `codex app-server`.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::PathBuf;

use codex_codes::{ClientInfo, InitializeCapabilities, InitializeParams};

/// Configuration for launching `codex app-server` and completing the
/// `initialize` / `initialized` handshake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpawnConfig {
    /// Explicit path to the `codex` executable; falls back to `CODEX_PATH`,
    /// then `"codex"`.
    pub codex_path: Option<PathBuf>,
    /// Extra CLI arguments appended after `app-server` and `-c` overrides.
    pub args: Vec<String>,
    /// Deterministic `-c key=value` configuration overrides passed to `app-server`.
    pub overrides: BTreeMap<String, String>,
    /// Environment variables set on the spawned child process.
    pub env: BTreeMap<String, String>,
    /// Working directory for the spawned child process.
    pub cwd: Option<PathBuf>,
    /// Client name reported in `initialize` (`clientInfo.name`).
    pub client_name: String,
    /// Client version reported in `initialize` (`clientInfo.version`).
    pub client_version: String,
    /// Whether to opt into `capabilities.experimentalApi` during `initialize`.
    pub experimental_api: bool,
}

impl Default for SpawnConfig {
    fn default() -> Self {
        Self {
            codex_path: None,
            args: Vec::new(),
            overrides: BTreeMap::new(),
            env: BTreeMap::new(),
            cwd: None,
            client_name: "codex-sdk".to_string(),
            client_version: env!("CARGO_PKG_VERSION").to_string(),
            experimental_api: false,
        }
    }
}

impl SpawnConfig {
    /// Construct a default [`SpawnConfig`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the path to the `codex` executable.
    #[must_use]
    pub fn codex_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.codex_path = Some(path.into());
        self
    }

    /// Append one raw CLI argument after `app-server` and `-c` overrides.
    #[must_use]
    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    /// Append multiple raw CLI arguments after `app-server` and `-c` overrides.
    #[must_use]
    pub fn args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    /// Insert a `-c key=value` configuration override.
    #[must_use]
    pub fn override_config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.overrides.insert(key.into(), value.into());
        self
    }

    /// Insert multiple `-c key=value` configuration overrides.
    #[must_use]
    pub fn overrides<I, K, V>(mut self, entries: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        for (key, value) in entries {
            self.overrides.insert(key.into(), value.into());
        }
        self
    }

    /// Insert an environment variable for the spawned child process.
    #[must_use]
    pub fn env_var(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }

    /// Insert multiple environment variables for the spawned child process.
    #[must_use]
    pub fn envs<I, K, V>(mut self, vars: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        for (key, value) in vars {
            self.env.insert(key.into(), value.into());
        }
        self
    }

    /// Set the working directory for the spawned child process.
    #[must_use]
    pub fn cwd(mut self, cwd: impl Into<PathBuf>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    /// Set `clientInfo.name` sent during `initialize`.
    #[must_use]
    pub fn client_name(mut self, name: impl Into<String>) -> Self {
        self.client_name = name.into();
        self
    }

    /// Set `clientInfo.version` sent during `initialize`.
    #[must_use]
    pub fn client_version(mut self, version: impl Into<String>) -> Self {
        self.client_version = version.into();
        self
    }

    /// Set both `clientInfo.name` and `clientInfo.version` sent during `initialize`.
    #[must_use]
    pub fn client_info(mut self, name: impl Into<String>, version: impl Into<String>) -> Self {
        self.client_name = name.into();
        self.client_version = version.into();
        self
    }

    /// Enable or disable `capabilities.experimentalApi` during `initialize`.
    #[must_use]
    pub const fn experimental_api(mut self, enabled: bool) -> Self {
        self.experimental_api = enabled;
        self
    }

    /// Resolve the executable path: `self.codex_path` (when non-empty), else
    /// non-empty `CODEX_PATH` from the process environment, else `"codex"`.
    #[must_use]
    pub fn resolve_executable(&self) -> PathBuf {
        self.resolve_executable_with(|key| std::env::var_os(key))
    }

    fn resolve_executable_with<F>(&self, lookup_env: F) -> PathBuf
    where
        F: FnOnce(&str) -> Option<OsString>,
    {
        if let Some(path) = &self.codex_path
            && !path.as_os_str().is_empty()
        {
            return path.clone();
        }
        if let Some(env_val) = lookup_env("CODEX_PATH")
            && !env_val.is_empty()
        {
            return PathBuf::from(env_val);
        }
        PathBuf::from("codex")
    }

    /// Render the deterministic CLI argument list passed to the `codex` binary:
    /// `["app-server", "-c", "k=v", ..., <args>...]`.
    #[must_use]
    pub fn build_args(&self) -> Vec<String> {
        let mut out = Vec::with_capacity(1 + self.overrides.len() * 2 + self.args.len());
        out.push("app-server".to_string());
        for (key, value) in &self.overrides {
            out.push("-c".to_string());
            out.push(format!("{key}={value}"));
        }
        out.extend(self.args.iter().cloned());
        out
    }

    /// Build typed [`InitializeParams`] corresponding to this configuration.
    #[must_use]
    pub fn initialize_params(&self) -> InitializeParams {
        InitializeParams {
            client_info: ClientInfo {
                name: self.client_name.clone(),
                version: self.client_version.clone(),
                title: None,
            },
            capabilities: Some(InitializeCapabilities {
                experimental_api: Some(self.experimental_api),
                ..InitializeCapabilities::default()
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn executable_resolution_follows_precedence() {
        let explicit = SpawnConfig::new().codex_path("/opt/bin/codex-custom");
        assert_eq!(
            explicit.resolve_executable_with(|_| Some(OsString::from("/env/codex"))),
            PathBuf::from("/opt/bin/codex-custom")
        );

        let default_cfg = SpawnConfig::new();
        assert_eq!(
            default_cfg.resolve_executable_with(|_| Some(OsString::from("/env/codex"))),
            PathBuf::from("/env/codex")
        );
        assert_eq!(
            default_cfg.resolve_executable_with(|_| Some(OsString::new())),
            PathBuf::from("codex")
        );
        assert_eq!(
            default_cfg.resolve_executable_with(|_| None),
            PathBuf::from("codex")
        );
    }

    #[test]
    fn build_args_emits_sorted_overrides_then_extra_args() {
        let cfg = SpawnConfig::new()
            .override_config("sandbox_mode", "workspace-write")
            .override_config("approval_policy", "on-request")
            .args(["--strict-config"]);

        assert_eq!(
            cfg.build_args(),
            vec![
                "app-server",
                "-c",
                "approval_policy=on-request",
                "-c",
                "sandbox_mode=workspace-write",
                "--strict-config",
            ]
        );
    }
}
