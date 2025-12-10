//! Connection startup and authentication state machine.

use crate::error::{Error, Result};
use crate::opts::{Opts, SslMode};
use crate::protocol::backend::{
    AuthenticationMessage, BackendKeyData, ErrorResponse, ParameterStatus, RawMessage,
    ReadyForQuery, msg_type,
};
use crate::protocol::frontend::auth::{ScramClient, md5_password};
use crate::protocol::frontend::{
    startup::write_ssl_request, write_password, write_sasl_initial_response, write_sasl_response,
    write_startup,
};
use crate::protocol::types::TransactionStatus;

use super::action::{Action, AsyncMessage};
use super::simple_query::BufferSet;

/// Connection state during startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Initial,
    WaitingSslResponse,
    SslHandshake,
    WaitingAuth,
    SaslInProgress,
    WaitingAuthResult,
    WaitingReady,
    Ready,
    Failed,
}

/// Connection startup state machine.
pub struct ConnectionStateMachine {
    state: ConnectionState,
    options: Opts,
    backend_key: Option<BackendKeyData>,
    server_params: Vec<(String, String)>,
    transaction_status: TransactionStatus,
    scram_client: Option<ScramClient>,
    write_buffer: Vec<u8>,
}

impl ConnectionStateMachine {
    /// Create a new connection state machine.
    pub fn new(options: Opts) -> Self {
        Self {
            state: ConnectionState::Initial,
            options,
            backend_key: None,
            server_params: Vec::new(),
            transaction_status: TransactionStatus::Idle,
            scram_client: None,
            write_buffer: Vec::new(),
        }
    }

    /// Get the current connection state.
    pub fn state(&self) -> ConnectionState {
        self.state
    }

    /// Get the backend key data (for cancellation).
    pub fn backend_key(&self) -> Option<&BackendKeyData> {
        self.backend_key.as_ref()
    }

    /// Get server parameters.
    pub fn server_params(&self) -> &[(String, String)] {
        &self.server_params
    }

    /// Get the current transaction status.
    pub fn transaction_status(&self) -> TransactionStatus {
        self.transaction_status
    }

    /// Start the connection process.
    ///
    /// Returns the initial action to perform.
    pub fn start(&mut self) -> Action<'_> {
        self.write_buffer.clear();

        match self.options.ssl_mode {
            SslMode::Disable => {
                // Send startup message directly
                self.write_startup_message();
                self.state = ConnectionState::WaitingAuth;
                Action::WritePacket(&self.write_buffer)
            }
            SslMode::Prefer | SslMode::Require => {
                // Send SSL request first
                write_ssl_request(&mut self.write_buffer);
                self.state = ConnectionState::WaitingSslResponse;
                Action::WritePacket(&self.write_buffer)
            }
        }
    }

    /// Process SSL response byte ('S' or 'N').
    pub fn process_ssl_response(&mut self, response: u8) -> Result<SslAction<'_>> {
        match response {
            b'S' => {
                self.state = ConnectionState::SslHandshake;
                Ok(SslAction::StartHandshake)
            }
            b'N' => {
                if self.options.ssl_mode == SslMode::Require {
                    self.state = ConnectionState::Failed;
                    return Err(Error::Auth(
                        "SSL required but not supported by server".into(),
                    ));
                }

                // SSL not supported, continue with plain connection
                self.write_buffer.clear();
                self.write_startup_message();
                self.state = ConnectionState::WaitingAuth;
                Ok(SslAction::SendStartup(self.write_buffer.as_slice()))
            }
            _ => {
                self.state = ConnectionState::Failed;
                Err(Error::Protocol(format!(
                    "Unexpected SSL response: {}",
                    response
                )))
            }
        }
    }

    /// Called after SSL handshake completes.
    pub fn ssl_handshake_complete(&mut self) -> Action<'_> {
        self.write_buffer.clear();
        self.write_startup_message();
        self.state = ConnectionState::WaitingAuth;
        Action::WritePacket(&self.write_buffer)
    }

    /// Process a message from the server.
    ///
    /// The caller should fill buffer_set.read_buffer with the message payload
    /// and set buffer_set.type_byte to the message type.
    pub fn step<'buf>(&'buf mut self, buffer_set: &'buf mut BufferSet) -> Result<Action<'buf>> {
        let type_byte = buffer_set.type_byte;

        // Handle async messages that can arrive at any time
        if RawMessage::is_async_type(type_byte) {
            let msg = RawMessage::new(type_byte, &buffer_set.read_buffer);
            return self.handle_async_message(&msg);
        }

        // Handle error response
        if type_byte == msg_type::ERROR_RESPONSE {
            let error = ErrorResponse::parse(&buffer_set.read_buffer)?;
            self.state = ConnectionState::Failed;
            return Err(error.into_error());
        }

        match self.state {
            ConnectionState::WaitingAuth => self.handle_auth_message(buffer_set),
            ConnectionState::SaslInProgress => self.handle_sasl_message(buffer_set),
            ConnectionState::WaitingAuthResult => self.handle_auth_result(buffer_set),
            ConnectionState::WaitingReady => self.handle_ready_message(buffer_set),
            _ => Err(Error::Protocol(format!(
                "Unexpected message in state {:?}",
                self.state
            ))),
        }
    }

    fn write_startup_message(&mut self) {
        let mut params: Vec<(&str, &str)> = vec![
            ("user", &self.options.user),
            ("client_encoding", "UTF8"),
        ];

        if let Some(ref db) = self.options.database {
            params.push(("database", db));
        }

        if let Some(ref app) = self.options.application_name {
            params.push(("application_name", app));
        }

        for (name, value) in &self.options.params {
            params.push((name, value));
        }

        write_startup(&mut self.write_buffer, &params);
    }

    fn handle_auth_message<'buf>(
        &'buf mut self,
        buffer_set: &'buf mut BufferSet,
    ) -> Result<Action<'buf>> {
        let type_byte = buffer_set.type_byte;
        if type_byte != msg_type::AUTHENTICATION {
            return Err(Error::Protocol(format!(
                "Expected Authentication message, got '{}'",
                type_byte as char
            )));
        }

        let auth = AuthenticationMessage::parse(&buffer_set.read_buffer)?;

        match auth {
            AuthenticationMessage::Ok => {
                self.state = ConnectionState::WaitingReady;
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
            }
            AuthenticationMessage::CleartextPassword => {
                let password = self
                    .options
                    .password
                    .as_ref()
                    .ok_or_else(|| Error::Auth("Password required but not provided".into()))?;

                self.write_buffer.clear();
                write_password(&mut self.write_buffer, password);
                self.state = ConnectionState::WaitingAuthResult;
                Ok(Action::WritePacket(&self.write_buffer))
            }
            AuthenticationMessage::Md5Password { salt } => {
                let password = self
                    .options
                    .password
                    .as_ref()
                    .ok_or_else(|| Error::Auth("Password required but not provided".into()))?;

                let hashed = md5_password(&self.options.user, password, &salt);
                self.write_buffer.clear();
                write_password(&mut self.write_buffer, &hashed);
                self.state = ConnectionState::WaitingAuthResult;
                Ok(Action::WritePacket(&self.write_buffer))
            }
            AuthenticationMessage::Sasl { mechanisms } => {
                // Check if SCRAM-SHA-256 is supported
                if !mechanisms.iter().any(|m| *m == "SCRAM-SHA-256") {
                    return Err(Error::Auth(format!(
                        "No supported SASL mechanism. Server offers: {:?}",
                        mechanisms
                    )));
                }

                let password = self
                    .options
                    .password
                    .as_ref()
                    .ok_or_else(|| Error::Auth("Password required but not provided".into()))?;

                let scram = ScramClient::new(password);
                let client_first = scram.client_first_message();

                self.write_buffer.clear();
                write_sasl_initial_response(
                    &mut self.write_buffer,
                    "SCRAM-SHA-256",
                    client_first.as_bytes(),
                );

                self.scram_client = Some(scram);
                self.state = ConnectionState::SaslInProgress;
                Ok(Action::WritePacket(&self.write_buffer))
            }
            _ => Err(Error::Unsupported(format!(
                "Unsupported authentication method: {:?}",
                auth
            ))),
        }
    }

    fn handle_sasl_message<'buf>(
        &'buf mut self,
        buffer_set: &'buf mut BufferSet,
    ) -> Result<Action<'buf>> {
        let type_byte = buffer_set.type_byte;
        if type_byte != msg_type::AUTHENTICATION {
            return Err(Error::Protocol(format!(
                "Expected Authentication message, got '{}'",
                type_byte as char
            )));
        }

        let auth = AuthenticationMessage::parse(&buffer_set.read_buffer)?;

        match auth {
            AuthenticationMessage::SaslContinue { data } => {
                let scram = self
                    .scram_client
                    .as_mut()
                    .ok_or_else(|| Error::Protocol("SCRAM client not initialized".into()))?;

                let server_first = simdutf8::compat::from_utf8(data)
                    .map_err(|e| Error::Auth(format!("Invalid server-first-message: {}", e)))?;

                let client_final = scram
                    .process_server_first(server_first)
                    .map_err(Error::Auth)?;

                self.write_buffer.clear();
                write_sasl_response(&mut self.write_buffer, client_final.as_bytes());
                Ok(Action::WritePacket(&self.write_buffer))
            }
            AuthenticationMessage::SaslFinal { data } => {
                let scram = self
                    .scram_client
                    .as_ref()
                    .ok_or_else(|| Error::Protocol("SCRAM client not initialized".into()))?;

                let server_final = simdutf8::compat::from_utf8(data)
                    .map_err(|e| Error::Auth(format!("Invalid server-final-message: {}", e)))?;

                scram
                    .verify_server_final(server_final)
                    .map_err(Error::Auth)?;

                self.state = ConnectionState::WaitingAuthResult;
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
            }
            _ => Err(Error::Protocol(format!(
                "Unexpected SASL message: {:?}",
                auth
            ))),
        }
    }

    fn handle_auth_result<'buf>(
        &mut self,
        buffer_set: &'buf mut BufferSet,
    ) -> Result<Action<'buf>> {
        let type_byte = buffer_set.type_byte;
        if type_byte != msg_type::AUTHENTICATION {
            return Err(Error::Protocol(format!(
                "Expected AuthenticationOk, got '{}'",
                type_byte as char
            )));
        }

        let auth = AuthenticationMessage::parse(&buffer_set.read_buffer)?;

        match auth {
            AuthenticationMessage::Ok => {
                self.state = ConnectionState::WaitingReady;
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
            }
            _ => Err(Error::Auth(format!("Unexpected auth result: {:?}", auth))),
        }
    }

    fn handle_ready_message<'buf>(
        &mut self,
        buffer_set: &'buf mut BufferSet,
    ) -> Result<Action<'buf>> {
        let type_byte = buffer_set.type_byte;
        let payload = &buffer_set.read_buffer;

        match type_byte {
            msg_type::BACKEND_KEY_DATA => {
                let key = BackendKeyData::parse(payload)?;
                self.backend_key = Some(*key);
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
            }
            msg_type::PARAMETER_STATUS => {
                let param = ParameterStatus::parse(payload)?;
                self.server_params
                    .push((param.name.to_string(), param.value.to_string()));
                Ok(Action::NeedPacket(&mut buffer_set.read_buffer))
            }
            msg_type::READY_FOR_QUERY => {
                let ready = ReadyForQuery::parse(payload)?;
                self.transaction_status = ready.transaction_status().unwrap_or_default();
                self.state = ConnectionState::Ready;
                Ok(Action::Finished)
            }
            _ => Err(Error::Protocol(format!(
                "Unexpected message during startup: '{}'",
                type_byte as char
            ))),
        }
    }

    fn handle_async_message(&mut self, msg: &RawMessage<'_>) -> Result<Action<'_>> {
        match msg.type_byte {
            msg_type::NOTICE_RESPONSE => {
                let notice = crate::protocol::backend::NoticeResponse::parse(msg.payload)?;
                Ok(Action::AsyncMessage(AsyncMessage::Notice(notice.0)))
            }
            msg_type::PARAMETER_STATUS => {
                let param = ParameterStatus::parse(msg.payload)?;
                // Update our cached value
                if let Some(entry) = self.server_params.iter_mut().find(|(n, _)| n == param.name) {
                    entry.1 = param.value.to_string();
                } else {
                    self.server_params
                        .push((param.name.to_string(), param.value.to_string()));
                }
                Ok(Action::AsyncMessage(AsyncMessage::ParameterChanged {
                    name: param.name.to_string(),
                    value: param.value.to_string(),
                }))
            }
            msg_type::NOTIFICATION_RESPONSE => {
                let notification =
                    crate::protocol::backend::auth::NotificationResponse::parse(msg.payload)?;
                Ok(Action::AsyncMessage(AsyncMessage::Notification {
                    pid: notification.pid,
                    channel: notification.channel.to_string(),
                    payload: notification.payload.to_string(),
                }))
            }
            _ => Err(Error::Protocol(format!(
                "Unknown async message type: '{}'",
                msg.type_byte as char
            ))),
        }
    }
}

/// SSL negotiation action.
#[derive(Debug)]
pub enum SslAction<'a> {
    /// Start TLS handshake
    StartHandshake,
    /// Send startup message (SSL not supported)
    SendStartup(&'a [u8]),
}
