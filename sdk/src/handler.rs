//! Callback trait for server-initiated `codex app-server` JSON-RPC requests
//! (command/file approvals, user-input prompts, MCP elicitations, and dynamic
//! tool calls).

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use codex_codes::{
    ApplyPatchApprovalParams, ApplyPatchApprovalResponse, AttestationGenerateParams,
    AttestationGenerateResponse, ChatgptAuthTokensRefreshParams, ChatgptAuthTokensRefreshResponse,
    CommandExecutionRequestApprovalParams, CommandExecutionRequestApprovalResponse,
    DynamicToolCallParams, DynamicToolCallResponse, ExecCommandApprovalParams,
    ExecCommandApprovalResponse, FileChangeRequestApprovalParams,
    FileChangeRequestApprovalResponse, McpServerElicitationRequestParams,
    McpServerElicitationRequestResponse, PermissionsRequestApprovalParams,
    PermissionsRequestApprovalResponse, ServerRequest, ToolRequestUserInputParams,
    ToolRequestUserInputResponse, protocol::methods,
};
use serde::Serialize;
use serde_json::Value;

use crate::error::RemoteError;

/// Result returned by [`ServerRequestHandler`] methods (`Ok(payload)` sends a
/// JSON-RPC `result` frame; `Err(RemoteError)` sends a JSON-RPC `error` frame).
pub type HandlerResult<T> = std::result::Result<T, RemoteError>;

/// Asynchronous handler for server-initiated requests arriving from
/// `codex app-server`.
///
/// The [`AppServer`](crate::AppServer) driver invokes handler futures
/// concurrently, so approvals, elicitations, or token refreshes are answered
/// promptly both mid-turn and outside an active turn without blocking outbound
/// calls or inbound notifications.
///
/// Every method defaults to returning a JSON-RPC `-32601` (`Method not found`)
/// [`RemoteError`]. Implement individual typed methods, or override
/// [`Self::handle_request`] directly when routing on [`ServerRequest`] as a
/// single enum.
pub trait ServerRequestHandler: Send + Sync + 'static {
    /// Handle `item/commandExecution/requestApproval`.
    fn command_execution_approval(
        &self,
        _params: CommandExecutionRequestApprovalParams,
    ) -> impl Future<Output = HandlerResult<CommandExecutionRequestApprovalResponse>> + Send {
        std::future::ready(Err(RemoteError::method_not_found(
            methods::CMD_EXEC_APPROVAL,
        )))
    }

    /// Handle `item/fileChange/requestApproval`.
    fn file_change_approval(
        &self,
        _params: FileChangeRequestApprovalParams,
    ) -> impl Future<Output = HandlerResult<FileChangeRequestApprovalResponse>> + Send {
        std::future::ready(Err(RemoteError::method_not_found(
            methods::FILE_CHANGE_APPROVAL,
        )))
    }

    /// Handle legacy `execCommandApproval`.
    fn exec_command_approval(
        &self,
        _params: ExecCommandApprovalParams,
    ) -> impl Future<Output = HandlerResult<ExecCommandApprovalResponse>> + Send {
        std::future::ready(Err(RemoteError::method_not_found(
            methods::EXEC_COMMAND_APPROVAL,
        )))
    }

    /// Handle legacy `applyPatchApproval`.
    fn apply_patch_approval(
        &self,
        _params: ApplyPatchApprovalParams,
    ) -> impl Future<Output = HandlerResult<ApplyPatchApprovalResponse>> + Send {
        std::future::ready(Err(RemoteError::method_not_found(
            methods::APPLY_PATCH_APPROVAL,
        )))
    }

    /// Handle `item/tool/requestUserInput`.
    fn request_user_input(
        &self,
        _params: ToolRequestUserInputParams,
    ) -> impl Future<Output = HandlerResult<ToolRequestUserInputResponse>> + Send {
        std::future::ready(Err(RemoteError::method_not_found(
            methods::TOOL_REQUEST_USER_INPUT,
        )))
    }

    /// Handle `mcpServer/elicitation/request`.
    fn mcp_elicitation(
        &self,
        _params: McpServerElicitationRequestParams,
    ) -> impl Future<Output = HandlerResult<McpServerElicitationRequestResponse>> + Send {
        std::future::ready(Err(RemoteError::method_not_found(
            methods::MCP_SERVER_ELICITATION_REQUEST,
        )))
    }

    /// Handle `item/permissions/requestApproval`.
    fn permissions_approval(
        &self,
        _params: PermissionsRequestApprovalParams,
    ) -> impl Future<Output = HandlerResult<PermissionsRequestApprovalResponse>> + Send {
        std::future::ready(Err(RemoteError::method_not_found(
            methods::PERMISSIONS_REQUEST_APPROVAL,
        )))
    }

    /// Handle `item/tool/call` dynamic tool invocation.
    fn dynamic_tool_call(
        &self,
        _params: DynamicToolCallParams,
    ) -> impl Future<Output = HandlerResult<DynamicToolCallResponse>> + Send {
        std::future::ready(Err(RemoteError::method_not_found(methods::ITEM_TOOL_CALL)))
    }

    /// Handle `account/chatgptAuthTokens/refresh`.
    fn chatgpt_auth_tokens_refresh(
        &self,
        _params: ChatgptAuthTokensRefreshParams,
    ) -> impl Future<Output = HandlerResult<ChatgptAuthTokensRefreshResponse>> + Send {
        std::future::ready(Err(RemoteError::method_not_found(
            methods::CHATGPT_AUTH_TOKENS_REFRESH,
        )))
    }

    /// Handle `attestation/generate`.
    fn attestation_generate(
        &self,
        _params: AttestationGenerateParams,
    ) -> impl Future<Output = HandlerResult<AttestationGenerateResponse>> + Send {
        std::future::ready(Err(RemoteError::method_not_found(
            methods::ATTESTATION_GENERATE,
        )))
    }

    /// Handle an unmodeled server request method (`ServerRequest::Unknown`).
    fn unknown_request(
        &self,
        method: String,
        _params: Option<Value>,
    ) -> impl Future<Output = HandlerResult<Value>> + Send {
        std::future::ready(Err(RemoteError::method_not_found(&method)))
    }

    /// Dispatch any decoded [`ServerRequest`] and return its JSON response value.
    ///
    /// By default, routes each variant to its typed trait method (and
    /// [`ServerRequest::Unknown`] to [`Self::unknown_request`]). Override this
    /// method directly if you prefer handling [`ServerRequest`] in one match.
    fn handle_request(
        &self,
        request: ServerRequest,
    ) -> impl Future<Output = HandlerResult<Value>> + Send {
        async move {
            match request {
                ServerRequest::CmdExecApproval(p) => {
                    encode_reply(self.command_execution_approval(p).await?)
                }
                ServerRequest::FileChangeApproval(p) => {
                    encode_reply(self.file_change_approval(p).await?)
                }
                ServerRequest::ExecCommandApproval(p) => {
                    encode_reply(self.exec_command_approval(p).await?)
                }
                ServerRequest::ApplyPatchApproval(p) => {
                    encode_reply(self.apply_patch_approval(p).await?)
                }
                ServerRequest::ToolRequestUserInput(p) => {
                    encode_reply(self.request_user_input(p).await?)
                }
                ServerRequest::McpServerElicitationRequest(p) => {
                    encode_reply(self.mcp_elicitation(p).await?)
                }
                ServerRequest::PermissionsRequestApproval(p) => {
                    encode_reply(self.permissions_approval(p).await?)
                }
                ServerRequest::ItemToolCall(p) => encode_reply(self.dynamic_tool_call(p).await?),
                ServerRequest::ChatgptAuthTokensRefresh(p) => {
                    encode_reply(self.chatgpt_auth_tokens_refresh(p).await?)
                }
                ServerRequest::AttestationGenerate(p) => {
                    encode_reply(self.attestation_generate(p).await?)
                }
                ServerRequest::Unknown { method, params } => {
                    self.unknown_request(method, params).await
                }
            }
        }
    }
}

fn encode_reply<T: Serialize>(value: T) -> HandlerResult<Value> {
    serde_json::to_value(value)
        .map_err(|err| RemoteError::internal_error(format!("failed to serialize response: {err}")))
}

/// Default [`ServerRequestHandler`] replying with JSON-RPC `-32601`
/// (`Method not found`) to every server-initiated request.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopServerRequestHandler;

impl ServerRequestHandler for NoopServerRequestHandler {}

type BoxHandlerFuture<'a, T> = Pin<Box<dyn Future<Output = HandlerResult<T>> + Send + 'a>>;

trait ServerRequestHandlerImpl: Send + Sync + 'static {
    fn command_execution_approval_dyn(
        &self,
        params: CommandExecutionRequestApprovalParams,
    ) -> BoxHandlerFuture<'_, CommandExecutionRequestApprovalResponse>;

    fn file_change_approval_dyn(
        &self,
        params: FileChangeRequestApprovalParams,
    ) -> BoxHandlerFuture<'_, FileChangeRequestApprovalResponse>;

    fn exec_command_approval_dyn(
        &self,
        params: ExecCommandApprovalParams,
    ) -> BoxHandlerFuture<'_, ExecCommandApprovalResponse>;

    fn apply_patch_approval_dyn(
        &self,
        params: ApplyPatchApprovalParams,
    ) -> BoxHandlerFuture<'_, ApplyPatchApprovalResponse>;

    fn request_user_input_dyn(
        &self,
        params: ToolRequestUserInputParams,
    ) -> BoxHandlerFuture<'_, ToolRequestUserInputResponse>;

    fn mcp_elicitation_dyn(
        &self,
        params: McpServerElicitationRequestParams,
    ) -> BoxHandlerFuture<'_, McpServerElicitationRequestResponse>;

    fn permissions_approval_dyn(
        &self,
        params: PermissionsRequestApprovalParams,
    ) -> BoxHandlerFuture<'_, PermissionsRequestApprovalResponse>;

    fn dynamic_tool_call_dyn(
        &self,
        params: DynamicToolCallParams,
    ) -> BoxHandlerFuture<'_, DynamicToolCallResponse>;

    fn chatgpt_auth_tokens_refresh_dyn(
        &self,
        params: ChatgptAuthTokensRefreshParams,
    ) -> BoxHandlerFuture<'_, ChatgptAuthTokensRefreshResponse>;

    fn attestation_generate_dyn(
        &self,
        params: AttestationGenerateParams,
    ) -> BoxHandlerFuture<'_, AttestationGenerateResponse>;

    fn unknown_request_dyn(
        &self,
        method: String,
        params: Option<Value>,
    ) -> BoxHandlerFuture<'_, Value>;

    fn handle_request_dyn(&self, request: ServerRequest) -> BoxHandlerFuture<'_, Value>;
}

impl<H: ServerRequestHandler> ServerRequestHandlerImpl for H {
    fn command_execution_approval_dyn(
        &self,
        params: CommandExecutionRequestApprovalParams,
    ) -> BoxHandlerFuture<'_, CommandExecutionRequestApprovalResponse> {
        Box::pin(self.command_execution_approval(params))
    }

    fn file_change_approval_dyn(
        &self,
        params: FileChangeRequestApprovalParams,
    ) -> BoxHandlerFuture<'_, FileChangeRequestApprovalResponse> {
        Box::pin(self.file_change_approval(params))
    }

    fn exec_command_approval_dyn(
        &self,
        params: ExecCommandApprovalParams,
    ) -> BoxHandlerFuture<'_, ExecCommandApprovalResponse> {
        Box::pin(self.exec_command_approval(params))
    }

    fn apply_patch_approval_dyn(
        &self,
        params: ApplyPatchApprovalParams,
    ) -> BoxHandlerFuture<'_, ApplyPatchApprovalResponse> {
        Box::pin(self.apply_patch_approval(params))
    }

    fn request_user_input_dyn(
        &self,
        params: ToolRequestUserInputParams,
    ) -> BoxHandlerFuture<'_, ToolRequestUserInputResponse> {
        Box::pin(self.request_user_input(params))
    }

    fn mcp_elicitation_dyn(
        &self,
        params: McpServerElicitationRequestParams,
    ) -> BoxHandlerFuture<'_, McpServerElicitationRequestResponse> {
        Box::pin(self.mcp_elicitation(params))
    }

    fn permissions_approval_dyn(
        &self,
        params: PermissionsRequestApprovalParams,
    ) -> BoxHandlerFuture<'_, PermissionsRequestApprovalResponse> {
        Box::pin(self.permissions_approval(params))
    }

    fn dynamic_tool_call_dyn(
        &self,
        params: DynamicToolCallParams,
    ) -> BoxHandlerFuture<'_, DynamicToolCallResponse> {
        Box::pin(self.dynamic_tool_call(params))
    }

    fn chatgpt_auth_tokens_refresh_dyn(
        &self,
        params: ChatgptAuthTokensRefreshParams,
    ) -> BoxHandlerFuture<'_, ChatgptAuthTokensRefreshResponse> {
        Box::pin(self.chatgpt_auth_tokens_refresh(params))
    }

    fn attestation_generate_dyn(
        &self,
        params: AttestationGenerateParams,
    ) -> BoxHandlerFuture<'_, AttestationGenerateResponse> {
        Box::pin(self.attestation_generate(params))
    }

    fn unknown_request_dyn(
        &self,
        method: String,
        params: Option<Value>,
    ) -> BoxHandlerFuture<'_, Value> {
        Box::pin(self.unknown_request(method, params))
    }

    fn handle_request_dyn(&self, request: ServerRequest) -> BoxHandlerFuture<'_, Value> {
        Box::pin(self.handle_request(request))
    }
}

/// Type-erased wrapper around any [`ServerRequestHandler`] keeping boxed futures
/// strictly internal.
#[derive(Clone)]
pub struct AnyServerRequestHandler {
    inner: Arc<dyn ServerRequestHandlerImpl>,
}

impl fmt::Debug for AnyServerRequestHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AnyServerRequestHandler")
            .finish_non_exhaustive()
    }
}

impl AnyServerRequestHandler {
    /// Erase the concrete type of `handler` behind [`AnyServerRequestHandler`].
    #[must_use]
    pub fn new<H: ServerRequestHandler>(handler: H) -> Self {
        Self {
            inner: Arc::new(handler),
        }
    }
}

impl ServerRequestHandler for AnyServerRequestHandler {
    fn command_execution_approval(
        &self,
        params: CommandExecutionRequestApprovalParams,
    ) -> impl Future<Output = HandlerResult<CommandExecutionRequestApprovalResponse>> + Send {
        let inner = Arc::clone(&self.inner);
        async move { inner.command_execution_approval_dyn(params).await }
    }

    fn file_change_approval(
        &self,
        params: FileChangeRequestApprovalParams,
    ) -> impl Future<Output = HandlerResult<FileChangeRequestApprovalResponse>> + Send {
        let inner = Arc::clone(&self.inner);
        async move { inner.file_change_approval_dyn(params).await }
    }

    fn exec_command_approval(
        &self,
        params: ExecCommandApprovalParams,
    ) -> impl Future<Output = HandlerResult<ExecCommandApprovalResponse>> + Send {
        let inner = Arc::clone(&self.inner);
        async move { inner.exec_command_approval_dyn(params).await }
    }

    fn apply_patch_approval(
        &self,
        params: ApplyPatchApprovalParams,
    ) -> impl Future<Output = HandlerResult<ApplyPatchApprovalResponse>> + Send {
        let inner = Arc::clone(&self.inner);
        async move { inner.apply_patch_approval_dyn(params).await }
    }

    fn request_user_input(
        &self,
        params: ToolRequestUserInputParams,
    ) -> impl Future<Output = HandlerResult<ToolRequestUserInputResponse>> + Send {
        let inner = Arc::clone(&self.inner);
        async move { inner.request_user_input_dyn(params).await }
    }

    fn mcp_elicitation(
        &self,
        params: McpServerElicitationRequestParams,
    ) -> impl Future<Output = HandlerResult<McpServerElicitationRequestResponse>> + Send {
        let inner = Arc::clone(&self.inner);
        async move { inner.mcp_elicitation_dyn(params).await }
    }

    fn permissions_approval(
        &self,
        params: PermissionsRequestApprovalParams,
    ) -> impl Future<Output = HandlerResult<PermissionsRequestApprovalResponse>> + Send {
        let inner = Arc::clone(&self.inner);
        async move { inner.permissions_approval_dyn(params).await }
    }

    fn dynamic_tool_call(
        &self,
        params: DynamicToolCallParams,
    ) -> impl Future<Output = HandlerResult<DynamicToolCallResponse>> + Send {
        let inner = Arc::clone(&self.inner);
        async move { inner.dynamic_tool_call_dyn(params).await }
    }

    fn chatgpt_auth_tokens_refresh(
        &self,
        params: ChatgptAuthTokensRefreshParams,
    ) -> impl Future<Output = HandlerResult<ChatgptAuthTokensRefreshResponse>> + Send {
        let inner = Arc::clone(&self.inner);
        async move { inner.chatgpt_auth_tokens_refresh_dyn(params).await }
    }

    fn attestation_generate(
        &self,
        params: AttestationGenerateParams,
    ) -> impl Future<Output = HandlerResult<AttestationGenerateResponse>> + Send {
        let inner = Arc::clone(&self.inner);
        async move { inner.attestation_generate_dyn(params).await }
    }

    fn unknown_request(
        &self,
        method: String,
        params: Option<Value>,
    ) -> impl Future<Output = HandlerResult<Value>> + Send {
        let inner = Arc::clone(&self.inner);
        async move { inner.unknown_request_dyn(method, params).await }
    }

    fn handle_request(
        &self,
        request: ServerRequest,
    ) -> impl Future<Output = HandlerResult<Value>> + Send {
        let inner = Arc::clone(&self.inner);
        async move { inner.handle_request_dyn(request).await }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::error::METHOD_NOT_FOUND_CODE;

    #[tokio::test]
    async fn noop_handler_returns_method_not_found() {
        let handler = AnyServerRequestHandler::new(NoopServerRequestHandler);
        let err = handler
            .handle_request(ServerRequest::Unknown {
                method: "custom/req".to_string(),
                params: Some(json!({"x": 1})),
            })
            .await
            .unwrap_err();
        assert_eq!(err.code, METHOD_NOT_FOUND_CODE);
    }
}
