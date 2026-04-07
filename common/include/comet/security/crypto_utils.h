#pragma once

#include <cstdint>
#include <string>

namespace comet {

struct SigningKeypair {
  std::string public_key_base64;
  std::string private_key_base64;
};

struct EncryptedEnvelope {
  std::string nonce_base64;
  std::string ciphertext_base64;
};

void InitializeCrypto();

SigningKeypair GenerateSigningKeypair();
std::string ComputeKeyFingerprintHex(const std::string& public_key_base64);
std::string RandomTokenBase64(int byte_count = 32);
std::string ComputeSha256Hex(const std::string& value);
std::string HashPassword(const std::string& password);
bool VerifyPasswordHash(
    const std::string& password,
    const std::string& password_hash);

std::string SignDetachedBase64(
    const std::string& message,
    const std::string& private_key_base64);

bool VerifyDetachedBase64(
    const std::string& message,
    const std::string& signature_base64,
    const std::string& public_key_base64);

EncryptedEnvelope EncryptEnvelopeBase64(
    const std::string& plaintext,
    const std::string& shared_secret_base64,
    const std::string& aad);

std::string DecryptEnvelopeBase64(
    const EncryptedEnvelope& envelope,
    const std::string& shared_secret_base64,
    const std::string& aad);

}  // namespace comet
