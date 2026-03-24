#include "comet/crypto_utils.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <iomanip>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <sodium.h>

namespace comet {

namespace {

std::once_flag g_crypto_init_once;
constexpr int kBase64Variant = sodium_base64_VARIANT_ORIGINAL;

void EnsureCrypto() {
  std::call_once(g_crypto_init_once, []() {
    if (sodium_init() < 0) {
      throw std::runtime_error("failed to initialize libsodium");
    }
  });
}

std::string EncodeBase64(const unsigned char* data, std::size_t size) {
  if (size == 0) {
    return {};
  }
  std::string out(sodium_base64_ENCODED_LEN(size, kBase64Variant), '\0');
  sodium_bin2base64(out.data(), out.size(), data, size, kBase64Variant);
  out.resize(std::strlen(out.c_str()));
  return out;
}

std::vector<unsigned char> DecodeBase64(const std::string& text, const std::string& label) {
  if (text.empty()) {
    return {};
  }
  std::vector<unsigned char> output(text.size(), 0);
  std::size_t actual_size = 0;
  if (sodium_base642bin(
          output.data(),
          output.size(),
          text.c_str(),
          text.size(),
          nullptr,
          &actual_size,
          nullptr,
          kBase64Variant) != 0) {
    throw std::runtime_error("failed to decode base64 " + label);
  }
  output.resize(actual_size);
  return output;
}

std::string ToHex(const unsigned char* data, std::size_t size) {
  std::ostringstream out;
  out << std::hex << std::setfill('0');
  for (std::size_t index = 0; index < size; ++index) {
    out << std::setw(2) << static_cast<int>(data[index]);
  }
  return out.str();
}

std::vector<unsigned char> DecodeFixedSizeBase64(
    const std::string& text,
    const std::string& label,
    std::size_t expected_size) {
  auto bytes = DecodeBase64(text, label);
  if (bytes.size() != expected_size) {
    throw std::runtime_error(
        "invalid " + label + " size: expected " + std::to_string(expected_size) +
        " bytes, got " + std::to_string(bytes.size()));
  }
  return bytes;
}

const unsigned char* DataOrNull(const std::string& value) {
  return value.empty() ? nullptr
                       : reinterpret_cast<const unsigned char*>(value.data());
}

const unsigned char* DataOrNull(const std::vector<unsigned char>& value) {
  return value.empty() ? nullptr : value.data();
}

std::array<unsigned char, crypto_hash_sha256_BYTES> DeriveEnvelopeKey(
    const std::string& shared_secret_base64) {
  const auto secret = DecodeBase64(shared_secret_base64, "shared secret");
  std::array<unsigned char, crypto_hash_sha256_BYTES> key{};
  crypto_hash_sha256(key.data(), DataOrNull(secret), secret.size());
  return key;
}

}  // namespace

void InitializeCrypto() {
  EnsureCrypto();
}

SigningKeypair GenerateSigningKeypair() {
  EnsureCrypto();
  std::array<unsigned char, crypto_sign_PUBLICKEYBYTES> public_key{};
  std::array<unsigned char, crypto_sign_SECRETKEYBYTES> private_key{};
  if (crypto_sign_keypair(public_key.data(), private_key.data()) != 0) {
    throw std::runtime_error("failed to generate signing keypair");
  }
  return SigningKeypair{
      EncodeBase64(public_key.data(), public_key.size()),
      EncodeBase64(private_key.data(), private_key.size()),
  };
}

std::string ComputeKeyFingerprintHex(const std::string& public_key_base64) {
  EnsureCrypto();
  const auto public_key =
      DecodeFixedSizeBase64(public_key_base64, "public key", crypto_sign_PUBLICKEYBYTES);
  std::array<unsigned char, crypto_hash_sha256_BYTES> digest{};
  crypto_hash_sha256(digest.data(), public_key.data(), public_key.size());
  return ToHex(digest.data(), digest.size());
}

std::string RandomTokenBase64(int byte_count) {
  EnsureCrypto();
  if (byte_count <= 0) {
    throw std::runtime_error("byte_count must be positive");
  }
  std::vector<unsigned char> bytes(static_cast<std::size_t>(byte_count));
  randombytes_buf(bytes.data(), bytes.size());
  return EncodeBase64(bytes.data(), bytes.size());
}

std::string SignDetachedBase64(
    const std::string& message,
    const std::string& private_key_base64) {
  EnsureCrypto();
  const auto private_key =
      DecodeFixedSizeBase64(private_key_base64, "private key", crypto_sign_SECRETKEYBYTES);
  std::array<unsigned char, crypto_sign_BYTES> signature{};
  unsigned long long signature_size = 0;
  if (crypto_sign_detached(
          signature.data(),
          &signature_size,
          DataOrNull(message),
          message.size(),
          private_key.data()) != 0) {
    throw std::runtime_error("failed to sign message");
  }
  return EncodeBase64(signature.data(), signature_size);
}

bool VerifyDetachedBase64(
    const std::string& message,
    const std::string& signature_base64,
    const std::string& public_key_base64) {
  EnsureCrypto();
  std::vector<unsigned char> public_key;
  std::vector<unsigned char> signature;
  try {
    public_key =
        DecodeFixedSizeBase64(public_key_base64, "public key", crypto_sign_PUBLICKEYBYTES);
    signature = DecodeFixedSizeBase64(signature_base64, "signature", crypto_sign_BYTES);
  } catch (const std::runtime_error&) {
    return false;
  }
  return crypto_sign_verify_detached(
             signature.data(),
             DataOrNull(message),
             message.size(),
             public_key.data()) == 0;
}

EncryptedEnvelope EncryptEnvelopeBase64(
    const std::string& plaintext,
    const std::string& shared_secret_base64,
    const std::string& aad) {
  EnsureCrypto();
  const auto key = DeriveEnvelopeKey(shared_secret_base64);
  std::array<unsigned char, crypto_aead_chacha20poly1305_ietf_NPUBBYTES> nonce{};
  randombytes_buf(nonce.data(), nonce.size());

  std::vector<unsigned char> ciphertext(
      plaintext.size() + crypto_aead_chacha20poly1305_ietf_ABYTES);
  unsigned long long ciphertext_size = 0;
  if (crypto_aead_chacha20poly1305_ietf_encrypt(
          ciphertext.data(),
          &ciphertext_size,
          DataOrNull(plaintext),
          plaintext.size(),
          DataOrNull(aad),
          aad.size(),
          nullptr,
          nonce.data(),
          key.data()) != 0) {
    throw std::runtime_error("failed to encrypt envelope payload");
  }
  ciphertext.resize(static_cast<std::size_t>(ciphertext_size));

  return EncryptedEnvelope{
      EncodeBase64(nonce.data(), nonce.size()),
      EncodeBase64(ciphertext.data(), ciphertext.size()),
  };
}

std::string DecryptEnvelopeBase64(
    const EncryptedEnvelope& envelope,
    const std::string& shared_secret_base64,
    const std::string& aad) {
  EnsureCrypto();
  const auto key = DeriveEnvelopeKey(shared_secret_base64);
  const auto nonce = DecodeBase64(envelope.nonce_base64, "envelope nonce");
  const auto ciphertext = DecodeBase64(envelope.ciphertext_base64, "envelope ciphertext");
  if (nonce.size() != crypto_aead_chacha20poly1305_ietf_NPUBBYTES ||
      ciphertext.size() < crypto_aead_chacha20poly1305_ietf_ABYTES) {
    throw std::runtime_error("invalid encrypted envelope");
  }
  std::vector<unsigned char> plaintext(
      ciphertext.size() - crypto_aead_chacha20poly1305_ietf_ABYTES);
  unsigned long long plaintext_size = 0;
  if (crypto_aead_chacha20poly1305_ietf_decrypt(
          plaintext.data(),
          &plaintext_size,
          nullptr,
          ciphertext.data(),
          ciphertext.size(),
          DataOrNull(aad),
          aad.size(),
          nonce.data(),
          key.data()) != 0) {
    throw std::runtime_error("failed to verify encrypted envelope");
  }
  plaintext.resize(static_cast<std::size_t>(plaintext_size));
  return std::string(plaintext.begin(), plaintext.end());
}

}  // namespace comet
