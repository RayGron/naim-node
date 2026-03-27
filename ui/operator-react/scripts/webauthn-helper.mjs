import fs from "node:fs/promises";

import {
  generateAuthenticationOptions,
  generateRegistrationOptions,
  verifyAuthenticationResponse,
  verifyRegistrationResponse,
} from "@simplewebauthn/server";

function toUint8Array(base64url) {
  return new Uint8Array(Buffer.from(base64url, "base64url"));
}

function toBase64Url(bytes) {
  return Buffer.from(bytes).toString("base64url");
}

async function main() {
  const [, , action, inputPath] = process.argv;
  if (!action || !inputPath) {
    throw new Error("usage: webauthn-helper.mjs <action> <input.json>");
  }

  const input = JSON.parse(await fs.readFile(inputPath, "utf8"));
  let output;

  if (action === "generate-registration-options") {
    output = await generateRegistrationOptions({
      rpName: input.rpName,
      rpID: input.rpID,
      userName: input.userName,
      userID: input.userID ? toUint8Array(input.userID) : undefined,
      challenge: input.challenge,
      userDisplayName: input.userDisplayName || input.userName,
      timeout: input.timeout || 60000,
      attestationType: "none",
      excludeCredentials: Array.isArray(input.excludeCredentials)
        ? input.excludeCredentials
        : [],
      authenticatorSelection: {
        residentKey: "preferred",
        userVerification: "preferred",
      },
    });
  } else if (action === "verify-registration") {
    const verification = await verifyRegistrationResponse({
      response: input.response,
      expectedChallenge: input.expectedChallenge,
      expectedOrigin: input.expectedOrigin,
      expectedRPID: input.expectedRPID,
      requireUserVerification: true,
    });
    output = {
      verified: verification.verified,
      registrationInfo: verification.verified
        ? {
            credentialID: verification.registrationInfo.credential.id,
            credentialPublicKey: toBase64Url(
              verification.registrationInfo.credential.publicKey,
            ),
            counter: verification.registrationInfo.credential.counter,
            credentialDeviceType:
              verification.registrationInfo.credentialDeviceType,
            credentialBackedUp:
              verification.registrationInfo.credentialBackedUp,
            transports: input.response?.response?.transports || [],
          }
        : null,
    };
  } else if (action === "generate-authentication-options") {
    output = await generateAuthenticationOptions({
      rpID: input.rpID,
      challenge: input.challenge,
      timeout: input.timeout || 60000,
      userVerification: "preferred",
      allowCredentials: Array.isArray(input.allowCredentials)
        ? input.allowCredentials
        : undefined,
    });
  } else if (action === "verify-authentication") {
    const verification = await verifyAuthenticationResponse({
      response: input.response,
      expectedChallenge: input.expectedChallenge,
      expectedOrigin: input.expectedOrigin,
      expectedRPID: input.expectedRPID,
      credential: {
        id: input.credential.id,
        publicKey: toUint8Array(input.credential.publicKey),
        counter: input.credential.counter,
        transports: input.credential.transports || [],
      },
      requireUserVerification: true,
    });
    output = {
      verified: verification.verified,
      authenticationInfo: verification.authenticationInfo || null,
    };
  } else {
    throw new Error(`unsupported action: ${action}`);
  }

  process.stdout.write(JSON.stringify(output));
}

main().catch((error) => {
  process.stderr.write(`${error.message}\n`);
  process.exit(1);
});
