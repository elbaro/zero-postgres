//! Authentication messages.

use crate::protocol::codec::MessageBuilder;

/// Write a PasswordMessage (cleartext or MD5 hashed password).
pub fn write_password(buf: &mut Vec<u8>, password: &str) {
    let mut msg = MessageBuilder::new(buf, super::msg_type::PASSWORD);
    msg.write_cstr(password);
    msg.finish();
}

/// Compute MD5 password hash.
///
/// PostgreSQL MD5 password format: "md5" + md5(md5(password + username) + salt)
pub fn md5_password(username: &str, password: &str, salt: &[u8; 4]) -> String {
    use md5::{Digest, Md5};

    // First hash: md5(password + username)
    let mut hasher = Md5::new();
    hasher.update(password.as_bytes());
    hasher.update(username.as_bytes());
    let first_hash = hasher.finalize();
    let first_hash_hex = format!("{:x}", first_hash);

    // Second hash: md5(first_hash_hex + salt)
    let mut hasher = Md5::new();
    hasher.update(first_hash_hex.as_bytes());
    hasher.update(salt);
    let second_hash = hasher.finalize();

    format!("md5{:x}", second_hash)
}

/// Write a SASLInitialResponse message.
///
/// mechanism: SASL mechanism name (e.g., "SCRAM-SHA-256")
/// initial_response: Client-first-message for SCRAM
pub fn write_sasl_initial_response(buf: &mut Vec<u8>, mechanism: &str, initial_response: &[u8]) {
    let mut msg = MessageBuilder::new(buf, super::msg_type::PASSWORD);
    msg.write_cstr(mechanism);
    msg.write_i32(initial_response.len() as i32);
    msg.write_bytes(initial_response);
    msg.finish();
}

/// Write a SASLResponse message.
///
/// response: Client-final-message for SCRAM
pub fn write_sasl_response(buf: &mut Vec<u8>, response: &[u8]) {
    let mut msg = MessageBuilder::new(buf, super::msg_type::PASSWORD);
    msg.write_bytes(response);
    msg.finish();
}

/// SCRAM-SHA-256 client implementation.
pub struct ScramClient {
    /// Client nonce
    nonce: String,
    /// Channel binding flag
    channel_binding: String,
    /// Password
    password: String,
    /// Server-first-message (stored for later)
    server_first: Option<String>,
    /// Auth message for signature verification
    auth_message: Option<String>,
    /// Salted password for server signature verification
    salted_password: Option<Vec<u8>>,
}

impl ScramClient {
    /// Create a new SCRAM client.
    pub fn new(password: &str) -> Self {
        use rand::Rng;

        // Generate 24-byte random nonce, base64 encoded
        let mut nonce_bytes = [0u8; 24];
        rand::thread_rng().fill(&mut nonce_bytes);
        let nonce = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, nonce_bytes);

        Self {
            nonce,
            channel_binding: "n,,".to_string(), // No channel binding
            password: password.to_string(),
            server_first: None,
            auth_message: None,
            salted_password: None,
        }
    }

    /// Create a new SCRAM client with channel binding.
    pub fn new_with_channel_binding(password: &str, channel_binding_data: &[u8]) -> Self {
        use rand::Rng;

        let mut nonce_bytes = [0u8; 24];
        rand::thread_rng().fill(&mut nonce_bytes);
        let nonce = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, nonce_bytes);

        // p=tls-server-end-point,,
        let cb_data =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, channel_binding_data);

        Self {
            nonce,
            channel_binding: format!("p=tls-server-end-point,,{}", cb_data),
            password: password.to_string(),
            server_first: None,
            auth_message: None,
            salted_password: None,
        }
    }

    /// Generate the client-first-message.
    pub fn client_first_message(&self) -> String {
        // n,,n=,r=<nonce>
        // Note: username is empty because PostgreSQL ignores it in SCRAM
        format!("{}n=,r={}", self.channel_binding, self.nonce)
    }

    /// Get the bare client-first-message (without channel binding prefix).
    fn client_first_message_bare(&self) -> String {
        format!("n=,r={}", self.nonce)
    }

    /// Process server-first-message and generate client-final-message.
    pub fn process_server_first(&mut self, server_first: &str) -> Result<String, String> {
        use base64::Engine;
        use hmac::{Hmac, Mac};
        use pbkdf2::pbkdf2_hmac;
        use sha2::{Digest, Sha256};

        self.server_first = Some(server_first.to_string());

        // Parse server-first-message: r=<nonce>,s=<salt>,i=<iterations>
        let mut combined_nonce = None;
        let mut salt_b64 = None;
        let mut iterations = None;

        for part in server_first.split(',') {
            if let Some(value) = part.strip_prefix("r=") {
                combined_nonce = Some(value);
            } else if let Some(value) = part.strip_prefix("s=") {
                salt_b64 = Some(value);
            } else if let Some(value) = part.strip_prefix("i=") {
                iterations = value.parse().ok();
            }
        }

        let combined_nonce = combined_nonce.ok_or("Missing nonce in server-first-message")?;
        let salt_b64 = salt_b64.ok_or("Missing salt in server-first-message")?;
        let iterations: u32 = iterations.ok_or("Missing iterations in server-first-message")?;

        // Verify nonce starts with our client nonce
        if !combined_nonce.starts_with(&self.nonce) {
            return Err("Server nonce doesn't start with client nonce".to_string());
        }

        // Decode salt
        let salt = base64::engine::general_purpose::STANDARD
            .decode(salt_b64)
            .map_err(|e| format!("Invalid salt: {}", e))?;

        // Compute SaltedPassword = Hi(Normalize(password), salt, iterations)
        let mut salted_password = vec![0u8; 32];
        pbkdf2_hmac::<Sha256>(
            self.password.as_bytes(),
            &salt,
            iterations,
            &mut salted_password,
        );

        self.salted_password = Some(salted_password.clone());

        // ClientKey = HMAC(SaltedPassword, "Client Key")
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&salted_password)
            .map_err(|e| format!("HMAC error: {}", e))?;
        mac.update(b"Client Key");
        let client_key = mac.finalize().into_bytes();

        // StoredKey = H(ClientKey)
        let stored_key = Sha256::digest(&client_key);

        // channel-binding = base64(channel-binding-flag)
        let channel_binding_b64 = base64::engine::general_purpose::STANDARD
            .encode(self.channel_binding.as_bytes());

        // client-final-message-without-proof = c=<channel-binding>,r=<nonce>
        let client_final_without_proof = format!("c={},r={}", channel_binding_b64, combined_nonce);

        // AuthMessage = client-first-message-bare + "," + server-first-message + "," + client-final-message-without-proof
        let auth_message = format!(
            "{},{},{}",
            self.client_first_message_bare(),
            server_first,
            client_final_without_proof
        );
        self.auth_message = Some(auth_message.clone());

        // ClientSignature = HMAC(StoredKey, AuthMessage)
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&stored_key)
            .map_err(|e| format!("HMAC error: {}", e))?;
        mac.update(auth_message.as_bytes());
        let client_signature = mac.finalize().into_bytes();

        // ClientProof = ClientKey XOR ClientSignature
        let mut client_proof = [0u8; 32];
        for i in 0..32 {
            client_proof[i] = client_key[i] ^ client_signature[i];
        }

        let proof_b64 = base64::engine::general_purpose::STANDARD.encode(client_proof);

        // client-final-message = client-final-message-without-proof + ",p=" + base64(ClientProof)
        Ok(format!("{},p={}", client_final_without_proof, proof_b64))
    }

    /// Verify server-final-message.
    pub fn verify_server_final(&self, server_final: &str) -> Result<(), String> {
        use base64::Engine;
        use hmac::{Hmac, Mac};

        // Parse server-final-message: v=<server-signature>
        let server_signature_b64 = server_final
            .strip_prefix("v=")
            .ok_or("Invalid server-final-message format")?;

        let server_signature = base64::engine::general_purpose::STANDARD
            .decode(server_signature_b64)
            .map_err(|e| format!("Invalid server signature: {}", e))?;

        // Compute expected ServerSignature
        let salted_password = self
            .salted_password
            .as_ref()
            .ok_or("Missing salted password")?;
        let auth_message = self.auth_message.as_ref().ok_or("Missing auth message")?;

        // ServerKey = HMAC(SaltedPassword, "Server Key")
        let mut mac = <Hmac<sha2::Sha256> as Mac>::new_from_slice(salted_password)
            .map_err(|e| format!("HMAC error: {}", e))?;
        mac.update(b"Server Key");
        let server_key = mac.finalize().into_bytes();

        // ServerSignature = HMAC(ServerKey, AuthMessage)
        let mut mac = <Hmac<sha2::Sha256> as Mac>::new_from_slice(&server_key)
            .map_err(|e| format!("HMAC error: {}", e))?;
        mac.update(auth_message.as_bytes());
        let expected_signature = mac.finalize().into_bytes();

        if server_signature.as_slice() != expected_signature.as_slice() {
            return Err("Server signature verification failed".to_string());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_md5_password() {
        // Test vector from PostgreSQL
        let result = md5_password("postgres", "password", &[0x01, 0x02, 0x03, 0x04]);
        assert!(result.starts_with("md5"));
        assert_eq!(result.len(), 35); // "md5" + 32 hex chars
    }

    #[test]
    fn test_password_message() {
        let mut buf = Vec::new();
        write_password(&mut buf, "secret");

        assert_eq!(buf[0], b'p');
        // Check that password is null-terminated in the message
        assert!(buf.ends_with(&[0]));
    }
}
