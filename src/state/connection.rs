//! Connection startup and authentication state machine.

use crate::error::{Error, Result};
use crate::opts::{Opts, SslMode};
use crate::protocol::backend::{
    AuthenticationMessage, BackendKeyData, ErrorResponse, NegotiateProtocolVersion,
    ParameterStatus, RawMessage, ReadyForQuery, msg_type,
};
use crate::protocol::frontend::auth::{ScramClient, md5_password};
use crate::protocol::frontend::{
    startup::write_ssl_request, write_password, write_sasl_initial_response, write_sasl_response,
    write_startup,
};
use crate::protocol::types::TransactionStatus;

use super::StateMachine;
use super::action::{Action, AsyncMessage};
use crate::buffer_set::BufferSet;

/// Connection state during startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Initial,
    WaitingSslResponse,
    WaitingTlsHandshake,
    WaitingAuthRead,
    WaitingAuth,
    SaslInProgressRead,
    SaslInProgress,
    WaitingAuthResultRead,
    WaitingAuthResult,
    WaitingReady,
    Finished,
}

/// Connection startup state machine.
pub struct ConnectionStateMachine {
    state: State,
    options: Opts,
    backend_key: Option<BackendKeyData>,
    server_params: Vec<(String, String)>,
    transaction_status: TransactionStatus,
    scram_client: Option<ScramClient>,
    /// SSL response byte, set by driver after ReadByte
    ssl_response: u8,
}

impl ConnectionStateMachine {
    /// Create a new connection state machine.
    pub fn new(options: Opts) -> Self {
        Self {
            state: State::Initial,
            options,
            backend_key: None,
            server_params: Vec::new(),
            transaction_status: TransactionStatus::Idle,
            scram_client: None,
            ssl_response: 0,
        }
    }

    /// Get the backend key data (for cancellation).
    pub fn backend_key(&self) -> Option<&BackendKeyData> {
        self.backend_key.as_ref()
    }

    /// Take server parameters.
    pub fn take_server_params(&mut self) -> Vec<(String, String)> {
        std::mem::take(&mut self.server_params)
    }

    /// Set the SSL response byte (called by driver after ReadByte).
    pub fn set_ssl_response(&mut self, response: u8) {
        self.ssl_response = response;
    }

    fn handle_initial(&mut self, buffer_set: &mut BufferSet) -> Result<Action> {
        buffer_set.write_buffer.clear();

        let client_supports_tls = cfg!(any(feature = "sync-tls", feature = "tokio-tls"));

        let send_ssl_request = match self.options.ssl_mode {
            SslMode::Disable => false,
            SslMode::Prefer => client_supports_tls,
            SslMode::Require if !client_supports_tls => {
                return Err(Error::Unsupported(
                    "SSL required but TLS feature not enabled".into(),
                ));
            }
            SslMode::Require => true,
        };

        if send_ssl_request {
            write_ssl_request(&mut buffer_set.write_buffer);
            self.state = State::WaitingSslResponse;
            Ok(Action::WriteAndReadByte)
        } else {
            self.write_startup_message(&mut buffer_set.write_buffer);
            self.state = State::WaitingAuthRead;
            Ok(Action::Write)
        }
    }

    fn handle_ssl_response(&mut self, buffer_set: &mut BufferSet) -> Result<Action> {
        match self.ssl_response {
            b'S' => {
                self.state = State::WaitingTlsHandshake;
                Ok(Action::TlsHandshake)
            }
            b'N' => {
                if self.options.ssl_mode == SslMode::Require {
                    return Err(Error::Auth(
                        "SSL required but not supported by server".into(),
                    ));
                }
                // SSL not supported, continue with plain connection
                buffer_set.write_buffer.clear();
                self.write_startup_message(&mut buffer_set.write_buffer);
                self.state = State::WaitingAuthRead;
                Ok(Action::Write)
            }
            _ => Err(Error::Protocol(format!(
                "Unexpected SSL response: {}",
                self.ssl_response
            ))),
        }
    }

    fn handle_tls_handshake_complete(&mut self, buffer_set: &mut BufferSet) -> Result<Action> {
        buffer_set.write_buffer.clear();
        self.write_startup_message(&mut buffer_set.write_buffer);
        self.state = State::WaitingAuthRead;
        Ok(Action::Write)
    }

    fn write_startup_message(&self, write_buffer: &mut Vec<u8>) {
        let mut params: Vec<(&str, &str)> =
            vec![("user", &self.options.user), ("client_encoding", "UTF8")];

        if let Some(db) = &self.options.database {
            params.push(("database", db));
        }

        if let Some(app) = &self.options.application_name {
            params.push(("application_name", app));
        }

        for (name, value) in &self.options.params {
            params.push((name, value));
        }

        write_startup(write_buffer, &params);
    }

    fn handle_auth_message(&mut self, buffer_set: &mut BufferSet) -> Result<Action> {
        let type_byte = buffer_set.type_byte;

        // Handle NegotiateProtocolVersion - server doesn't support our protocol version
        if type_byte == msg_type::NEGOTIATE_PROTOCOL_VERSION {
            let negotiate = NegotiateProtocolVersion::parse(&buffer_set.read_buffer)?;
            // Server sends the newest minor version it supports (0 for 3.0, 1 for 3.1, etc.)
            return Err(Error::Protocol(format!(
                "Server does not support protocol 3.2 (requires PostgreSQL 17+). \
                 Server supports protocol 3.{}. Unrecognized options: {:?}",
                negotiate.newest_minor_version, negotiate.unrecognized_options
            )));
        }

        if type_byte != msg_type::AUTHENTICATION {
            return Err(Error::Protocol(format!(
                "Expected Authentication message, got '{}'",
                type_byte as char
            )));
        }

        let auth = AuthenticationMessage::parse(&buffer_set.read_buffer)?;

        match auth {
            AuthenticationMessage::Ok => {
                self.state = State::WaitingReady;
                Ok(Action::ReadMessage)
            }
            AuthenticationMessage::CleartextPassword => {
                let password = self
                    .options
                    .password
                    .as_ref()
                    .ok_or_else(|| Error::Auth("Password required but not provided".into()))?;

                buffer_set.write_buffer.clear();
                write_password(&mut buffer_set.write_buffer, password);
                self.state = State::WaitingAuthResultRead;
                Ok(Action::Write)
            }
            AuthenticationMessage::Md5Password { salt } => {
                let password = self
                    .options
                    .password
                    .as_ref()
                    .ok_or_else(|| Error::Auth("Password required but not provided".into()))?;

                let hashed = md5_password(&self.options.user, password, &salt);
                buffer_set.write_buffer.clear();
                write_password(&mut buffer_set.write_buffer, &hashed);
                self.state = State::WaitingAuthResultRead;
                Ok(Action::Write)
            }
            AuthenticationMessage::Sasl { mechanisms } => {
                // Check if SCRAM-SHA-256 is supported
                if !mechanisms.contains(&"SCRAM-SHA-256") {
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

                buffer_set.write_buffer.clear();
                write_sasl_initial_response(
                    &mut buffer_set.write_buffer,
                    "SCRAM-SHA-256",
                    client_first.as_bytes(),
                );

                self.scram_client = Some(scram);
                self.state = State::SaslInProgressRead;
                Ok(Action::Write)
            }
            _ => Err(Error::Unsupported(format!(
                "Unsupported authentication method: {:?}",
                auth
            ))),
        }
    }

    fn handle_sasl_message(&mut self, buffer_set: &mut BufferSet) -> Result<Action> {
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

                buffer_set.write_buffer.clear();
                write_sasl_response(&mut buffer_set.write_buffer, client_final.as_bytes());
                self.state = State::SaslInProgressRead;
                Ok(Action::Write)
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

                self.state = State::WaitingAuthResult;
                Ok(Action::ReadMessage)
            }
            _ => Err(Error::Protocol(format!(
                "Unexpected SASL message: {:?}",
                auth
            ))),
        }
    }

    fn handle_auth_result(&mut self, buffer_set: &BufferSet) -> Result<Action> {
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
                self.state = State::WaitingReady;
                Ok(Action::ReadMessage)
            }
            _ => Err(Error::Auth(format!("Unexpected auth result: {:?}", auth))),
        }
    }

    fn handle_ready_message(&mut self, buffer_set: &BufferSet) -> Result<Action> {
        let type_byte = buffer_set.type_byte;
        let payload = &buffer_set.read_buffer;

        match type_byte {
            msg_type::BACKEND_KEY_DATA => {
                let key = BackendKeyData::parse(payload)?;
                self.backend_key = Some(key);
                Ok(Action::ReadMessage)
            }
            msg_type::PARAMETER_STATUS => {
                let param = ParameterStatus::parse(payload)?;
                self.server_params
                    .push((param.name.to_string(), param.value.to_string()));
                Ok(Action::ReadMessage)
            }
            msg_type::READY_FOR_QUERY => {
                let ready = ReadyForQuery::parse(payload)?;
                self.transaction_status = ready.transaction_status().unwrap_or_default();
                self.state = State::Finished;
                Ok(Action::Finished)
            }
            _ => Err(Error::Protocol(format!(
                "Unexpected message during startup: '{}'",
                type_byte as char
            ))),
        }
    }

    fn handle_async_message(&self, msg: &RawMessage<'_>) -> Result<Action> {
        match msg.type_byte {
            msg_type::NOTICE_RESPONSE => {
                let notice = crate::protocol::backend::NoticeResponse::parse(msg.payload)?;
                Ok(Action::HandleAsyncMessageAndReadMessage(
                    AsyncMessage::Notice(notice.0),
                ))
            }
            msg_type::PARAMETER_STATUS => {
                let param = ParameterStatus::parse(msg.payload)?;
                Ok(Action::HandleAsyncMessageAndReadMessage(
                    AsyncMessage::ParameterChanged {
                        name: param.name.to_string(),
                        value: param.value.to_string(),
                    },
                ))
            }
            msg_type::NOTIFICATION_RESPONSE => {
                let notification =
                    crate::protocol::backend::auth::NotificationResponse::parse(msg.payload)?;
                Ok(Action::HandleAsyncMessageAndReadMessage(
                    AsyncMessage::Notification {
                        pid: notification.pid,
                        channel: notification.channel.to_string(),
                        payload: notification.payload.to_string(),
                    },
                ))
            }
            _ => Err(Error::Protocol(format!(
                "Unknown async message type: '{}'",
                msg.type_byte as char
            ))),
        }
    }
}

impl StateMachine for ConnectionStateMachine {
    fn step(&mut self, buffer_set: &mut BufferSet) -> Result<Action> {
        // Handle states that don't need to read buffer_set
        match self.state {
            State::Initial => return self.handle_initial(buffer_set),
            State::WaitingSslResponse => return self.handle_ssl_response(buffer_set),
            State::WaitingTlsHandshake => return self.handle_tls_handshake_complete(buffer_set),
            State::WaitingAuthRead => {
                self.state = State::WaitingAuth;
                return Ok(Action::ReadMessage);
            }
            State::SaslInProgressRead => {
                self.state = State::SaslInProgress;
                return Ok(Action::ReadMessage);
            }
            State::WaitingAuthResultRead => {
                self.state = State::WaitingAuthResult;
                return Ok(Action::ReadMessage);
            }
            _ => {}
        }

        let type_byte = buffer_set.type_byte;

        // Handle async messages that can arrive at any time
        // Note: PARAMETER_STATUS during WaitingReady is part of normal startup, not async
        if RawMessage::is_async_type(type_byte)
            && !(self.state == State::WaitingReady && type_byte == msg_type::PARAMETER_STATUS)
        {
            let msg = RawMessage::new(type_byte, &buffer_set.read_buffer);
            return self.handle_async_message(&msg);
        }

        // Handle error response
        if type_byte == msg_type::ERROR_RESPONSE {
            let error = ErrorResponse::parse(&buffer_set.read_buffer)?;
            return Err(error.into_error());
        }

        match self.state {
            State::WaitingAuth => self.handle_auth_message(buffer_set),
            State::SaslInProgress => self.handle_sasl_message(buffer_set),
            State::WaitingAuthResult => self.handle_auth_result(buffer_set),
            State::WaitingReady => self.handle_ready_message(buffer_set),
            _ => Err(Error::Protocol(format!(
                "Unexpected state {:?}",
                self.state
            ))),
        }
    }

    fn transaction_status(&self) -> TransactionStatus {
        self.transaction_status
    }
}
