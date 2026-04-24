#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>
#include <openssl/bn.h>
#include <openssl/core_names.h>
#include <openssl/evp.h>
#include <openssl/params.h>
#include <openssl/sha.h>

#include "auth/webauthn_service.h"

namespace {

using ByteVector = std::vector<unsigned char>;
using nlohmann::json;

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

std::string EncodeBase64Url(const ByteVector& bytes) {
  static constexpr char kAlphabet[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  std::string out;
  int value = 0;
  int bits = -6;
  for (unsigned char byte : bytes) {
    value = (value << 8) | byte;
    bits += 8;
    while (bits >= 0) {
      out.push_back(kAlphabet[(value >> bits) & 0x3f]);
      bits -= 6;
    }
  }
  if (bits > -6) {
    out.push_back(kAlphabet[((value << 8) >> (bits + 8)) & 0x3f]);
  }
  return out;
}

std::string EncodeBase64Url(std::string_view text) {
  return EncodeBase64Url(ByteVector(text.begin(), text.end()));
}

ByteVector Sha256(const ByteVector& data) {
  ByteVector digest(SHA256_DIGEST_LENGTH);
  SHA256(data.data(), data.size(), digest.data());
  return digest;
}

ByteVector Sha256(std::string_view text) {
  return Sha256(ByteVector(text.begin(), text.end()));
}

void AppendUint(ByteVector& out, std::uint64_t value, unsigned char major) {
  if (value < 24) {
    out.push_back(static_cast<unsigned char>((major << 5U) | value));
    return;
  }
  if (value <= 0xff) {
    out.push_back(static_cast<unsigned char>((major << 5U) | 24U));
    out.push_back(static_cast<unsigned char>(value));
    return;
  }
  if (value <= 0xffff) {
    out.push_back(static_cast<unsigned char>((major << 5U) | 25U));
    out.push_back(static_cast<unsigned char>((value >> 8U) & 0xffU));
    out.push_back(static_cast<unsigned char>(value & 0xffU));
    return;
  }
  throw std::runtime_error("test CBOR integer is too large");
}

void AppendInt(ByteVector& out, std::int64_t value) {
  if (value >= 0) {
    AppendUint(out, static_cast<std::uint64_t>(value), 0);
  } else {
    AppendUint(out, static_cast<std::uint64_t>(-1 - value), 1);
  }
}

void AppendBytes(ByteVector& out, const ByteVector& bytes) {
  AppendUint(out, bytes.size(), 2);
  out.insert(out.end(), bytes.begin(), bytes.end());
}

void AppendText(ByteVector& out, std::string_view text) {
  AppendUint(out, text.size(), 3);
  out.insert(out.end(), text.begin(), text.end());
}

void AppendMapHeader(ByteVector& out, std::size_t size) {
  AppendUint(out, size, 5);
}

ByteVector BuildCoseP256PublicKey(const ByteVector& x, const ByteVector& y) {
  ByteVector cose;
  AppendMapHeader(cose, 5);
  AppendInt(cose, 1);
  AppendInt(cose, 2);
  AppendInt(cose, 3);
  AppendInt(cose, -7);
  AppendInt(cose, -1);
  AppendInt(cose, 1);
  AppendInt(cose, -2);
  AppendBytes(cose, x);
  AppendInt(cose, -3);
  AppendBytes(cose, y);
  return cose;
}

ByteVector BuildAttestationObject(const ByteVector& authenticator_data) {
  ByteVector empty_map;
  AppendMapHeader(empty_map, 0);

  ByteVector out;
  AppendMapHeader(out, 3);
  AppendText(out, "fmt");
  AppendText(out, "none");
  AppendText(out, "attStmt");
  out.insert(out.end(), empty_map.begin(), empty_map.end());
  AppendText(out, "authData");
  AppendBytes(out, authenticator_data);
  return out;
}

ByteVector BuildAuthenticatorData(
    const std::string& rp_id,
    unsigned char flags,
    std::uint32_t counter,
    const ByteVector& credential_id,
    const ByteVector& cose_public_key) {
  ByteVector out = Sha256(rp_id);
  out.push_back(flags);
  out.push_back(static_cast<unsigned char>((counter >> 24U) & 0xffU));
  out.push_back(static_cast<unsigned char>((counter >> 16U) & 0xffU));
  out.push_back(static_cast<unsigned char>((counter >> 8U) & 0xffU));
  out.push_back(static_cast<unsigned char>(counter & 0xffU));
  if (!credential_id.empty()) {
    out.insert(out.end(), 16, 0);
    out.push_back(static_cast<unsigned char>((credential_id.size() >> 8U) & 0xffU));
    out.push_back(static_cast<unsigned char>(credential_id.size() & 0xffU));
    out.insert(out.end(), credential_id.begin(), credential_id.end());
    out.insert(out.end(), cose_public_key.begin(), cose_public_key.end());
  }
  return out;
}

EVP_PKEY* GenerateP256Key() {
  EVP_PKEY_CTX* context = EVP_PKEY_CTX_new_from_name(nullptr, "EC", nullptr);
  if (context == nullptr || EVP_PKEY_keygen_init(context) != 1) {
    EVP_PKEY_CTX_free(context);
    throw std::runtime_error("failed to initialize P-256 keygen");
  }
  char group_name[] = "prime256v1";
  OSSL_PARAM params[] = {
      OSSL_PARAM_construct_utf8_string(
          OSSL_PKEY_PARAM_GROUP_NAME,
          group_name,
          0),
      OSSL_PARAM_construct_end(),
  };
  if (EVP_PKEY_CTX_set_params(context, params) != 1) {
    EVP_PKEY_CTX_free(context);
    throw std::runtime_error("failed to set P-256 keygen params");
  }
  EVP_PKEY* key = nullptr;
  if (EVP_PKEY_generate(context, &key) != 1 || key == nullptr) {
    EVP_PKEY_CTX_free(context);
    throw std::runtime_error("failed to generate P-256 key");
  }
  EVP_PKEY_CTX_free(context);
  return key;
}

ByteVector GetEcCoordinate(EVP_PKEY* key, const char* name) {
  BIGNUM* value = nullptr;
  if (EVP_PKEY_get_bn_param(key, name, &value) != 1 || value == nullptr) {
    throw std::runtime_error(std::string("failed to read coordinate: ") + name);
  }
  ByteVector out(32);
  if (BN_bn2binpad(value, out.data(), static_cast<int>(out.size())) !=
      static_cast<int>(out.size())) {
    BN_free(value);
    throw std::runtime_error(std::string("failed to encode coordinate: ") + name);
  }
  BN_free(value);
  return out;
}

ByteVector Sign(EVP_PKEY* key, const ByteVector& data) {
  EVP_MD_CTX* context = EVP_MD_CTX_new();
  if (context == nullptr ||
      EVP_DigestSignInit(context, nullptr, EVP_sha256(), nullptr, key) != 1 ||
      EVP_DigestSignUpdate(context, data.data(), data.size()) != 1) {
    EVP_MD_CTX_free(context);
    throw std::runtime_error("failed to initialize signing");
  }
  std::size_t size = 0;
  if (EVP_DigestSignFinal(context, nullptr, &size) != 1) {
    EVP_MD_CTX_free(context);
    throw std::runtime_error("failed to size signature");
  }
  ByteVector signature(size);
  if (EVP_DigestSignFinal(context, signature.data(), &size) != 1) {
    EVP_MD_CTX_free(context);
    throw std::runtime_error("failed to sign");
  }
  signature.resize(size);
  EVP_MD_CTX_free(context);
  return signature;
}

ByteVector JsonBytes(const json& value) {
  const std::string text = value.dump();
  return ByteVector(text.begin(), text.end());
}

void TestNativeRegistrationAndAuthentication() {
  const std::string rp_id = "localhost";
  const std::string origin = "http://localhost:18080";
  const std::string challenge = "native-test-challenge";
  const ByteVector credential_id{'c', 'r', 'e', 'd', '-', '1'};

  EVP_PKEY* key = GenerateP256Key();
  const ByteVector cose_public_key =
      BuildCoseP256PublicKey(
          GetEcCoordinate(key, OSSL_PKEY_PARAM_EC_PUB_X),
          GetEcCoordinate(key, OSSL_PKEY_PARAM_EC_PUB_Y));

  const ByteVector registration_auth_data =
      BuildAuthenticatorData(
          rp_id,
          0x01 | 0x04 | 0x40,
          0,
          credential_id,
          cose_public_key);
  const json registration_client_data = {
      {"type", "webauthn.create"},
      {"challenge", EncodeBase64Url(challenge)},
      {"origin", origin},
  };

  naim::controller::auth::WebAuthnService service;
  const json registration = service.Run(
      "verify-registration",
      json{
          {"expectedChallenge", challenge},
          {"expectedOrigin", origin},
          {"expectedRPID", rp_id},
          {"response",
           {
               {"id", EncodeBase64Url(credential_id)},
               {"response",
                {
                    {"clientDataJSON", EncodeBase64Url(JsonBytes(registration_client_data))},
                    {"attestationObject",
                     EncodeBase64Url(BuildAttestationObject(registration_auth_data))},
                    {"transports", json::array({"internal"})},
                }},
           }},
      });
  Expect(registration.value("verified", false), "registration should verify");
  const json registration_info = registration.at("registrationInfo");
  Expect(
      registration_info.value("credentialID", std::string{}) ==
          EncodeBase64Url(credential_id),
      "credential id should round-trip");

  const ByteVector authentication_auth_data =
      BuildAuthenticatorData(rp_id, 0x01 | 0x04, 1, {}, {});
  const json authentication_client_data = {
      {"type", "webauthn.get"},
      {"challenge", EncodeBase64Url(challenge)},
      {"origin", origin},
  };
  ByteVector signed_data = authentication_auth_data;
  const ByteVector client_hash = Sha256(JsonBytes(authentication_client_data));
  signed_data.insert(signed_data.end(), client_hash.begin(), client_hash.end());
  const ByteVector signature = Sign(key, signed_data);
  EVP_PKEY_free(key);

  const json authentication = service.Run(
      "verify-authentication",
      json{
          {"expectedChallenge", challenge},
          {"expectedOrigin", origin},
          {"expectedRPID", rp_id},
          {"credential",
           {
               {"id", EncodeBase64Url(credential_id)},
               {"publicKey", registration_info.at("credentialPublicKey")},
               {"counter", 0},
               {"transports", json::array({"internal"})},
           }},
          {"response",
           {
               {"id", EncodeBase64Url(credential_id)},
               {"response",
                {
                    {"authenticatorData", EncodeBase64Url(authentication_auth_data)},
                    {"clientDataJSON",
                     EncodeBase64Url(JsonBytes(authentication_client_data))},
                    {"signature", EncodeBase64Url(signature)},
                }},
           }},
      });
  Expect(authentication.value("verified", false), "authentication should verify");
  Expect(
      authentication.at("authenticationInfo").value("newCounter", 0) == 1,
      "authentication counter should be returned");

  std::cout << "ok: native-webauthn-registration-authentication" << '\n';
}

void TestOptionsAreBrowserJsonCompatible() {
  naim::controller::auth::WebAuthnService service;
  const json registration_options = service.Run(
      "generate-registration-options",
      json{
          {"rpName", "naim-node"},
          {"rpID", "localhost"},
          {"userName", "admin"},
          {"challenge", "abc"},
      });
  Expect(registration_options.contains("rp"), "registration options need rp");
  Expect(registration_options.contains("user"), "registration options need user");
  Expect(
      registration_options.value("challenge", std::string{}) == EncodeBase64Url("abc"),
      "registration challenge should be base64url JSON");

  const json authentication_options = service.Run(
      "generate-authentication-options",
      json{
          {"rpID", "localhost"},
          {"challenge", "abc"},
          {"allowCredentials", json::array()},
      });
  Expect(
      authentication_options.value("rpId", std::string{}) == "localhost",
      "authentication options need rpId");
  Expect(
      authentication_options.value("challenge", std::string{}) == EncodeBase64Url("abc"),
      "authentication challenge should be base64url JSON");

  std::cout << "ok: native-webauthn-options-json" << '\n';
}

}  // namespace

int main() {
  try {
    TestOptionsAreBrowserJsonCompatible();
    TestNativeRegistrationAndAuthentication();
    std::cout << "webauthn_service_tests passed" << '\n';
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "webauthn_service_tests failed: " << error.what() << '\n';
    return 1;
  }
}
