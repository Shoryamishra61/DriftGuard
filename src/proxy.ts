import { NextRequest, NextResponse } from "next/server";

function unauthorized() {
  return new NextResponse("Authentication required", {
    status: 401,
    headers: {
      "Cache-Control": "no-store",
      "WWW-Authenticate": 'Basic realm="DriftGuard Dashboard", charset="UTF-8"',
      "X-Content-Type-Options": "nosniff",
    },
  });
}

async function digest(value: string) {
  return new Uint8Array(
    await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value)),
  );
}

async function secretMatches(candidate: string, expected: string) {
  const [candidateHash, expectedHash] = await Promise.all([
    digest(candidate),
    digest(expected),
  ]);
  let difference = candidateHash.length ^ expectedHash.length;
  const comparedLength = Math.max(candidateHash.length, expectedHash.length);
  for (let index = 0; index < comparedLength; index += 1) {
    difference |=
      candidateHash[index % candidateHash.length] ^
      expectedHash[index % expectedHash.length];
  }
  return difference === 0;
}

function decodeCredentials(authorization: string | null) {
  if (!authorization?.startsWith("Basic ")) return null;
  try {
    const decoded = atob(authorization.slice(6));
    const separator = decoded.indexOf(":");
    if (separator < 1) return null;
    return {
      username: decoded.slice(0, separator),
      password: decoded.slice(separator + 1),
    };
  } catch {
    return null;
  }
}

export async function proxy(request: NextRequest) {
  const publicReadOnly = process.env.DRIFTGUARD_PUBLIC_READ_ONLY === "true";
  const expectedUsername = process.env.DRIFTGUARD_DASHBOARD_USERNAME;
  const expectedPassword = process.env.DRIFTGUARD_DASHBOARD_PASSWORD;
  if (!publicReadOnly && (!expectedUsername || !expectedPassword)) {
    return new NextResponse("Dashboard authentication is not configured", {
      status: 503,
      headers: { "Cache-Control": "no-store" },
    });
  }

  if (!publicReadOnly) {
    const credentials = decodeCredentials(request.headers.get("authorization"));
    const [usernameMatches, passwordMatches] = await Promise.all([
      secretMatches(credentials?.username ?? "", expectedUsername!),
      secretMatches(credentials?.password ?? "", expectedPassword!),
    ]);
    if (!credentials || !usernameMatches || !passwordMatches) {
      return unauthorized();
    }
  }

  const response = NextResponse.next();
  response.headers.set("Cache-Control", "no-store");
  response.headers.set("Referrer-Policy", "no-referrer");
  response.headers.set("X-Content-Type-Options", "nosniff");
  response.headers.set("X-Frame-Options", "DENY");
  response.headers.set("X-DriftGuard-Access", publicReadOnly ? "public-read-only" : "authenticated");
  return response;
}

export const config = {
  matcher: ["/((?!api/health|api/live|_next/static|_next/image|favicon.ico|robots.txt).*)"],
};
