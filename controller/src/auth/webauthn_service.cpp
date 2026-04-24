#include "auth/webauthn_service.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <map>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

#include <openssl/core_names.h>
#include <openssl/evp.h>
#include <openssl/params.h>
#include <openssl/sha.h>

using nlohmann::json;

namespace {

using ByteVector = std::vector<unsigned char>;

struct CborValue {
  enum class Type {
    kUnsigned,
    kNegative,
    kBytes,
    kText,
    kArray,
    kMap,
    kBool,
    kNull,
  };

  Type type = Type::kNull;
  std::int64_t integer = 0;
  ByteVector bytes;
  std::string text;
  std::vector<CborValue> array;
  std::map<std::int64_t, CborValue> int_map;
  std::map<std::string, CborValue> text_map;
  bool boolean = false;
};

struct CborParser {
  const ByteVector& data;
  std::size_t offset = 0;

  explicit CborParser(const ByteVector& input) : data(input) {}

  unsigned char ReadByte() {
    if (offset >= data.size()) {
      throw std::runtime_error("truncated CBOR payload");
    }
    return data[offset++];
  }

  std::uint64_t ReadArgument(unsigned char additional) {
    if (additional < 24) {
      return additional;
    }
    if (additional == 24) {
      return ReadByte();
    }
    if (additional == 25) {
      std::uint64_t value = 0;
      for (int i = 0; i < 2; ++i) {
        value = (value << 8U) | ReadByte();
      }
      return value;
    }
    if (additional == 26) {
      std::uint64_t value = 0;
      for (int i = 0; i < 4; ++i) {
        value = (value << 8U) | ReadByte();
      }
      return value;
    }
    if (additional == 27) {
      std::uint64_t value = 0;
      for (int i = 0; i < 8; ++i) {
        value = (value << 8U) | ReadByte();
      }
      return value;
    }
    throw std::runtime_error("unsupported indefinite CBOR item");
  }

  ByteVector ReadBytes(std::size_t size) {
    if (offset + size > data.size()) {
      throw std::runtime_error("truncated CBOR bytes");
    }
    ByteVector out(data.begin() + static_cast<std::ptrdiff_t>(offset),
                   data.begin() + static_cast<std::ptrdiff_t>(offset + size));
    offset += size;
    return out;
  }

  CborValue Parse() {
    const unsigned char header = ReadByte();
    const unsigned char major = static_cast<unsigned char>(header >> 5U);
    const unsigned char additional = static_cast<unsigned char>(header & 0x1fU);

    if (major == 0) {
      CborValue value;
      value.type = CborValue::Type::kUnsigned;
      value.integer = static_cast<std::int64_t>(ReadArgument(additional));
      return value;
    }
    if (major == 1) {
      CborValue value;
      value.type = CborValue::Type::kNegative;
      value.integer = -1 - static_cast<std::int64_t>(ReadArgument(additional));
      return value;
    }
    if (major == 2) {
      CborValue value;
      value.type = CborValue::Type::kBytes;
      value.bytes = ReadBytes(static_cast<std::size_t>(ReadArgument(additional)));
      return value;
    }
    if (major == 3) {
      CborValue value;
      value.type = CborValue::Type::kText;
      const auto bytes = ReadBytes(static_cast<std::size_t>(ReadArgument(additional)));
      value.text.assign(bytes.begin(), bytes.end());
      return value;
    }
    if (major == 4) {
      CborValue value;
      value.type = CborValue::Type::kArray;
      const auto count = ReadArgument(additional);
      value.array.reserve(static_cast<std::size_t>(count));
      for (std::uint64_t index = 0; index < count; ++index) {
        value.array.push_back(Parse());
      }
      return value;
    }
    if (major == 5) {
      CborValue value;
      value.type = CborValue::Type::kMap;
      const auto count = ReadArgument(additional);
      for (std::uint64_t index = 0; index < count; ++index) {
        CborValue key = Parse();
        CborValue item = Parse();
        if (key.type == CborValue::Type::kUnsigned ||
            key.type == CborValue::Type::kNegative) {
          value.int_map.emplace(key.integer, std::move(item));
        } else if (key.type == CborValue::Type::kText) {
          value.text_map.emplace(key.text, std::move(item));
        }
      }
      return value;
    }
    if (major == 6) {
      (void)ReadArgument(additional);
      return Parse();
    }
    if (major == 7) {
      CborValue value;
      if (additional == 20 || additional == 21) {
        value.type = CborValue::Type::kBool;
        value.boolean = additional == 21;
        return value;
      }
      if (additional == 22) {
        value.type = CborValue::Type::kNull;
        return value;
      }
    }
    throw std::runtime_error("unsupported CBOR item");
  }
};

struct AuthenticatorData {
  ByteVector rp_id_hash;
  unsigned char flags = 0;
  std::uint32_t sign_count = 0;
  ByteVector credential_id;
  ByteVector credential_public_key_cose;
};

constexpr unsigned char kUserPresentFlag = 0x01;
constexpr unsigned char kUserVerifiedFlag = 0x04;
constexpr unsigned char kBackupEligibleFlag = 0x08;
constexpr unsigned char kBackupStateFlag = 0x10;
constexpr unsigned char kAttestedCredentialDataFlag = 0x40;

std::runtime_error WebAuthnError(const std::string& message) {
  return std::runtime_error("WebAuthn native verification failed: " + message);
}

int Base64UrlValue(char ch) {
  if (ch >= 'A' && ch <= 'Z') {
    return ch - 'A';
  }
  if (ch >= 'a' && ch <= 'z') {
    return 26 + ch - 'a';
  }
  if (ch >= '0' && ch <= '9') {
    return 52 + ch - '0';
  }
  if (ch == '-' || ch == '+') {
    return 62;
  }
  if (ch == '_' || ch == '/') {
    return 63;
  }
  return -1;
}

ByteVector DecodeBase64Url(const std::string& text) {
  ByteVector out;
  int value = 0;
  int bits = -8;
  for (char ch : text) {
    if (ch == '=') {
      break;
    }
    const int digit = Base64UrlValue(ch);
    if (digit < 0) {
      throw WebAuthnError("invalid base64url data");
    }
    value = (value << 6) | digit;
    bits += 6;
    if (bits >= 0) {
      out.push_back(static_cast<unsigned char>((value >> bits) & 0xff));
      bits -= 8;
    }
  }
  return out;
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

bool ConstantTimeEquals(const ByteVector& lhs, const ByteVector& rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  unsigned char diff = 0;
  for (std::size_t index = 0; index < lhs.size(); ++index) {
    diff |= static_cast<unsigned char>(lhs[index] ^ rhs[index]);
  }
  return diff == 0;
}

bool ChallengeMatches(const std::string& actual, const std::string& expected) {
  if (actual == expected) {
    return true;
  }
  if (actual == EncodeBase64Url(std::string_view(expected))) {
    return true;
  }
  try {
    return actual == EncodeBase64Url(DecodeBase64Url(expected));
  } catch (...) {
    return false;
  }
}

const json& RequireObjectField(const json& value, const char* key) {
  if (!value.contains(key) || !value.at(key).is_object()) {
    throw WebAuthnError(std::string("missing object field: ") + key);
  }
  return value.at(key);
}

std::string RequireStringField(const json& value, const char* key) {
  if (!value.contains(key) || !value.at(key).is_string()) {
    throw WebAuthnError(std::string("missing string field: ") + key);
  }
  return value.at(key).get<std::string>();
}

CborValue ParseCbor(const ByteVector& data) {
  CborParser parser(data);
  CborValue value = parser.Parse();
  if (parser.offset != data.size()) {
    throw WebAuthnError("unexpected trailing CBOR data");
  }
  return value;
}

const CborValue& RequireTextMapField(const CborValue& value, const std::string& key) {
  if (value.type != CborValue::Type::kMap) {
    throw WebAuthnError("CBOR value is not a map");
  }
  const auto it = value.text_map.find(key);
  if (it == value.text_map.end()) {
    throw WebAuthnError("missing CBOR map field: " + key);
  }
  return it->second;
}

const CborValue& RequireIntMapField(const CborValue& value, std::int64_t key) {
  if (value.type != CborValue::Type::kMap) {
    throw WebAuthnError("CBOR value is not a map");
  }
  const auto it = value.int_map.find(key);
  if (it == value.int_map.end()) {
    throw WebAuthnError("missing COSE key field");
  }
  return it->second;
}

std::int64_t RequireCborInteger(const CborValue& value) {
  if (value.type != CborValue::Type::kUnsigned &&
      value.type != CborValue::Type::kNegative) {
    throw WebAuthnError("CBOR value is not an integer");
  }
  return value.integer;
}

ByteVector RequireCborBytes(const CborValue& value) {
  if (value.type != CborValue::Type::kBytes) {
    throw WebAuthnError("CBOR value is not bytes");
  }
  return value.bytes;
}

AuthenticatorData ParseAuthenticatorData(
    const ByteVector& bytes,
    bool require_attested_credential) {
  if (bytes.size() < 37) {
    throw WebAuthnError("authenticator data is too short");
  }
  AuthenticatorData out;
  out.rp_id_hash.assign(bytes.begin(), bytes.begin() + 32);
  out.flags = bytes[32];
  out.sign_count =
      (static_cast<std::uint32_t>(bytes[33]) << 24U) |
      (static_cast<std::uint32_t>(bytes[34]) << 16U) |
      (static_cast<std::uint32_t>(bytes[35]) << 8U) |
      static_cast<std::uint32_t>(bytes[36]);

  if ((out.flags & kUserPresentFlag) == 0) {
    throw WebAuthnError("user presence flag is not set");
  }
  if ((out.flags & kUserVerifiedFlag) == 0) {
    throw WebAuthnError("user verification flag is not set");
  }
  if (!require_attested_credential) {
    return out;
  }
  if ((out.flags & kAttestedCredentialDataFlag) == 0) {
    throw WebAuthnError("attested credential data is missing");
  }
  std::size_t offset = 37;
  if (bytes.size() < offset + 18) {
    throw WebAuthnError("attested credential data is too short");
  }
  offset += 16;
  const std::uint16_t credential_id_size =
      (static_cast<std::uint16_t>(bytes[offset]) << 8U) |
      static_cast<std::uint16_t>(bytes[offset + 1]);
  offset += 2;
  if (bytes.size() < offset + credential_id_size) {
    throw WebAuthnError("credential id is truncated");
  }
  out.credential_id.assign(
      bytes.begin() + static_cast<std::ptrdiff_t>(offset),
      bytes.begin() + static_cast<std::ptrdiff_t>(offset + credential_id_size));
  offset += credential_id_size;
  const std::size_t cose_begin = offset;
  CborParser parser(bytes);
  parser.offset = offset;
  (void)parser.Parse();
  offset = parser.offset;
  out.credential_public_key_cose.assign(
      bytes.begin() + static_cast<std::ptrdiff_t>(cose_begin),
      bytes.begin() + static_cast<std::ptrdiff_t>(offset));
  return out;
}

void ValidateClientData(
    const ByteVector& client_data_json,
    const std::string& expected_type,
    const std::string& expected_challenge,
    const std::string& expected_origin) {
  const json client_data = json::parse(
      std::string(client_data_json.begin(), client_data_json.end()));
  if (client_data.value("type", std::string{}) != expected_type) {
    throw WebAuthnError("unexpected clientDataJSON type");
  }
  if (!ChallengeMatches(
          client_data.value("challenge", std::string{}),
          expected_challenge)) {
    throw WebAuthnError("unexpected challenge");
  }
  if (client_data.value("origin", std::string{}) != expected_origin) {
    throw WebAuthnError("unexpected origin");
  }
}

void ValidateRpIdHash(
    const AuthenticatorData& auth_data,
    const std::string& expected_rp_id) {
  if (!ConstantTimeEquals(auth_data.rp_id_hash, Sha256(expected_rp_id))) {
    throw WebAuthnError("unexpected rpIdHash");
  }
}

json GenerateRegistrationOptions(const json& payload) {
  const std::string rp_name = RequireStringField(payload, "rpName");
  const std::string rp_id = RequireStringField(payload, "rpID");
  const std::string user_name = RequireStringField(payload, "userName");
  const std::string challenge = RequireStringField(payload, "challenge");
  const std::string user_display_name =
      payload.value("userDisplayName", user_name);
  const std::string user_id =
      payload.value("userID", EncodeBase64Url(std::string_view(user_name)));

  json exclude_credentials = json::array();
  for (const auto& credential : payload.value("excludeCredentials", json::array())) {
    if (!credential.is_object()) {
      continue;
    }
    json item = credential;
    item["type"] = item.value("type", "public-key");
    exclude_credentials.push_back(std::move(item));
  }

  return json{
      {"rp", {{"name", rp_name}, {"id", rp_id}}},
      {"user", {{"id", user_id}, {"name", user_name}, {"displayName", user_display_name}}},
      {"challenge", EncodeBase64Url(std::string_view(challenge))},
      {"pubKeyCredParams",
       json::array({
           json{{"type", "public-key"}, {"alg", -7}},
       })},
      {"timeout", payload.value("timeout", 60000)},
      {"attestation", "none"},
      {"excludeCredentials", exclude_credentials},
      {"authenticatorSelection",
       {{"residentKey", "preferred"}, {"userVerification", "preferred"}}},
  };
}

json GenerateAuthenticationOptions(const json& payload) {
  json allow_credentials = json::array();
  for (const auto& credential : payload.value("allowCredentials", json::array())) {
    if (!credential.is_object()) {
      continue;
    }
    json item = credential;
    item["type"] = item.value("type", "public-key");
    allow_credentials.push_back(std::move(item));
  }
  return json{
      {"rpId", RequireStringField(payload, "rpID")},
      {"challenge", EncodeBase64Url(std::string_view(RequireStringField(payload, "challenge")))},
      {"timeout", payload.value("timeout", 60000)},
      {"userVerification", "preferred"},
      {"allowCredentials", allow_credentials},
  };
}

json VerifyRegistration(const json& payload) {
  const json& response = RequireObjectField(payload, "response");
  const json& credential_response = RequireObjectField(response, "response");
  const ByteVector client_data_json =
      DecodeBase64Url(RequireStringField(credential_response, "clientDataJSON"));
  ValidateClientData(
      client_data_json,
      "webauthn.create",
      RequireStringField(payload, "expectedChallenge"),
      RequireStringField(payload, "expectedOrigin"));

  const ByteVector attestation_object =
      DecodeBase64Url(RequireStringField(credential_response, "attestationObject"));
  const CborValue attestation = ParseCbor(attestation_object);
  const ByteVector auth_data_bytes =
      RequireCborBytes(RequireTextMapField(attestation, "authData"));
  const AuthenticatorData auth_data =
      ParseAuthenticatorData(auth_data_bytes, true);
  ValidateRpIdHash(auth_data, RequireStringField(payload, "expectedRPID"));
  if (response.contains("id") && response.at("id").is_string() &&
      response.at("id").get<std::string>() != EncodeBase64Url(auth_data.credential_id)) {
    throw WebAuthnError("registration credential id mismatch");
  }

  json transports = json::array();
  if (credential_response.contains("transports") &&
      credential_response.at("transports").is_array()) {
    transports = credential_response.at("transports");
  }

  return json{
      {"verified", true},
      {"registrationInfo",
       {
           {"credentialID", EncodeBase64Url(auth_data.credential_id)},
           {"credentialPublicKey", EncodeBase64Url(auth_data.credential_public_key_cose)},
           {"counter", auth_data.sign_count},
           {"credentialDeviceType",
            (auth_data.flags & kBackupEligibleFlag) != 0 ? "multiDevice" : "singleDevice"},
           {"credentialBackedUp", (auth_data.flags & kBackupStateFlag) != 0},
           {"transports", transports},
       }},
  };
}

EVP_PKEY* BuildP256PublicKey(const ByteVector& x, const ByteVector& y) {
  if (x.size() != 32 || y.size() != 32) {
    throw WebAuthnError("unsupported P-256 coordinate size");
  }
  ByteVector public_key{0x04};
  public_key.insert(public_key.end(), x.begin(), x.end());
  public_key.insert(public_key.end(), y.begin(), y.end());

  EVP_PKEY_CTX* context = EVP_PKEY_CTX_new_from_name(nullptr, "EC", nullptr);
  if (context == nullptr || EVP_PKEY_fromdata_init(context) != 1) {
    EVP_PKEY_CTX_free(context);
    throw WebAuthnError("failed to initialize P-256 public key decoder");
  }
  char group_name[] = "prime256v1";
  OSSL_PARAM params[] = {
      OSSL_PARAM_construct_utf8_string(
          OSSL_PKEY_PARAM_GROUP_NAME,
          group_name,
          0),
      OSSL_PARAM_construct_octet_string(
          OSSL_PKEY_PARAM_PUB_KEY,
          public_key.data(),
          public_key.size()),
      OSSL_PARAM_construct_end(),
  };
  EVP_PKEY* key = nullptr;
  if (EVP_PKEY_fromdata(context, &key, EVP_PKEY_PUBLIC_KEY, params) != 1 ||
      key == nullptr) {
    EVP_PKEY_CTX_free(context);
    throw WebAuthnError("failed to build P-256 public key");
  }
  EVP_PKEY_CTX_free(context);
  return key;
}

EVP_PKEY* ParseCoseP256PublicKey(const ByteVector& cose_key) {
  const CborValue cose = ParseCbor(cose_key);
  const auto kty = RequireCborInteger(RequireIntMapField(cose, 1));
  const auto alg = RequireCborInteger(RequireIntMapField(cose, 3));
  const auto crv = RequireCborInteger(RequireIntMapField(cose, -1));
  if (kty != 2 || alg != -7 || crv != 1) {
    throw WebAuthnError("only COSE ES256 P-256 credentials are supported");
  }
  return BuildP256PublicKey(
      RequireCborBytes(RequireIntMapField(cose, -2)),
      RequireCborBytes(RequireIntMapField(cose, -3)));
}

bool VerifyEs256Signature(
    const ByteVector& cose_key,
    const ByteVector& signed_data,
    const ByteVector& signature) {
  EVP_PKEY* key = ParseCoseP256PublicKey(cose_key);
  EVP_MD_CTX* context = EVP_MD_CTX_new();
  if (context == nullptr) {
    EVP_PKEY_free(key);
    throw WebAuthnError("failed to allocate signature verifier");
  }
  const bool ok =
      EVP_DigestVerifyInit(context, nullptr, EVP_sha256(), nullptr, key) == 1 &&
      EVP_DigestVerifyUpdate(context, signed_data.data(), signed_data.size()) == 1 &&
      EVP_DigestVerifyFinal(context, signature.data(), signature.size()) == 1;
  EVP_MD_CTX_free(context);
  EVP_PKEY_free(key);
  return ok;
}

json VerifyAuthentication(const json& payload) {
  const json& response = RequireObjectField(payload, "response");
  const json& credential_payload = RequireObjectField(payload, "credential");
  const json& credential_response = RequireObjectField(response, "response");

  const std::string expected_credential_id =
      RequireStringField(credential_payload, "id");
  const std::string response_credential_id = RequireStringField(response, "id");
  if (response_credential_id != expected_credential_id) {
    throw WebAuthnError("unexpected credential id");
  }

  const ByteVector client_data_json =
      DecodeBase64Url(RequireStringField(credential_response, "clientDataJSON"));
  ValidateClientData(
      client_data_json,
      "webauthn.get",
      RequireStringField(payload, "expectedChallenge"),
      RequireStringField(payload, "expectedOrigin"));

  const ByteVector authenticator_data_bytes =
      DecodeBase64Url(RequireStringField(credential_response, "authenticatorData"));
  const AuthenticatorData auth_data =
      ParseAuthenticatorData(authenticator_data_bytes, false);
  ValidateRpIdHash(auth_data, RequireStringField(payload, "expectedRPID"));

  ByteVector signed_data = authenticator_data_bytes;
  const ByteVector client_hash = Sha256(client_data_json);
  signed_data.insert(signed_data.end(), client_hash.begin(), client_hash.end());

  const ByteVector public_key =
      DecodeBase64Url(RequireStringField(credential_payload, "publicKey"));
  const ByteVector signature =
      DecodeBase64Url(RequireStringField(credential_response, "signature"));
  if (!VerifyEs256Signature(public_key, signed_data, signature)) {
    return json{{"verified", false}, {"authenticationInfo", nullptr}};
  }

  const auto stored_counter =
      static_cast<std::uint32_t>(credential_payload.value("counter", 0));
  if (auth_data.sign_count != 0 && stored_counter != 0 &&
      auth_data.sign_count <= stored_counter) {
    throw WebAuthnError("signature counter did not increase");
  }

  return json{
      {"verified", true},
      {"authenticationInfo",
       {
           {"newCounter", auth_data.sign_count},
           {"credentialID", expected_credential_id},
       }},
  };
}

}  // namespace

namespace naim::controller::auth {

nlohmann::json WebAuthnService::Run(
    const std::string& action,
    const nlohmann::json& payload) const {
  if (action == "generate-registration-options") {
    return GenerateRegistrationOptions(payload);
  }
  if (action == "verify-registration") {
    return VerifyRegistration(payload);
  }
  if (action == "generate-authentication-options") {
    return GenerateAuthenticationOptions(payload);
  }
  if (action == "verify-authentication") {
    return VerifyAuthentication(payload);
  }
  throw std::invalid_argument("unsupported WebAuthn action: " + action);
}

}  // namespace naim::controller::auth
