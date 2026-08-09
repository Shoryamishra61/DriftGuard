import { NextRequest, NextResponse } from "next/server";

export const dynamic = "force-dynamic";

const GET_ROUTES = new Set([
  "diagnostics/pulse",
  "metrics/trends",
  "alerts",
  "alert-rules",
  "vectors/projection",
]);

function routeIsAllowed(route: string, method: string) {
  if (method === "GET") return GET_ROUTES.has(route);
  if (method === "POST") return route === "alert-rules" || route === "logs";
  return method === "PUT" && /^alert-rules\/[1-9]\d*$/.test(route);
}

function requestIsSameOrigin(request: NextRequest) {
  const origin = request.headers.get("origin");
  const fetchSite = request.headers.get("sec-fetch-site")?.toLowerCase();
  const dashboardMarker = request.headers.get("x-driftguard-dashboard-request");
  if (!origin || dashboardMarker !== "1") return false;
  if (fetchSite) return fetchSite === "same-origin";

  try {
    return new URL(origin).protocol === "https:" || new URL(origin).hostname === "localhost";
  } catch {
    return false;
  }
}

type RouteContext = { params: Promise<{ path: string[] }> };

async function proxy(request: NextRequest, context: RouteContext) {
  const { path } = await context.params;
  const route = path.join("/");
  if (!routeIsAllowed(route, request.method)) {
    return NextResponse.json({ detail: "Route not available" }, { status: 404 });
  }

  if (process.env.DRIFTGUARD_PUBLIC_READ_ONLY === "true" && request.method !== "GET") {
    return NextResponse.json(
      { detail: "The public judging dashboard is read-only" },
      { status: 403, headers: { "Cache-Control": "no-store" } },
    );
  }

  if (request.method !== "GET" && !requestIsSameOrigin(request)) {
    return NextResponse.json({ detail: "Cross-origin mutation rejected" }, { status: 403 });
  }

  const apiKey = process.env.DRIFTGUARD_DASHBOARD_API_KEY;
  const adminToken = process.env.DRIFTGUARD_ADMIN_TOKEN;
  if (!apiKey || !adminToken) {
    return NextResponse.json(
      { detail: "Dashboard API credential is not configured" },
      { status: 503 },
    );
  }

  const baseUrl = (process.env.API_INTERNAL_URL ?? "http://api:8000").replace(/\/$/, "");
  const target = new URL(`${baseUrl}/api/v1/${route}`);
  request.nextUrl.searchParams.forEach((value, key) => target.searchParams.append(key, value));

  const headers = new Headers({
    Accept: "application/json",
    "X-DriftGuard-Admin-Token": adminToken,
    "X-API-Key": apiKey,
  });
  let body: string | undefined;
  if (request.method !== "GET") {
    const contentType = request.headers.get("content-type")?.toLowerCase() ?? "";
    if (!contentType.startsWith("application/json")) {
      return NextResponse.json({ detail: "JSON body required" }, { status: 415 });
    }
    const declaredLength = Number(request.headers.get("content-length") ?? "0");
    if (Number.isFinite(declaredLength) && declaredLength > 16_384) {
      return NextResponse.json({ detail: "Request body too large" }, { status: 413 });
    }
    body = await request.text();
    if (new TextEncoder().encode(body).byteLength > 16_384) {
      return NextResponse.json({ detail: "Request body too large" }, { status: 413 });
    }
    headers.set("Content-Type", "application/json");
  }

  try {
    const upstream = await fetch(target, {
      method: request.method,
      headers,
      body,
      cache: "no-store",
      signal: AbortSignal.timeout(1800),
    });
    const responseBody = await upstream.text();
    return new NextResponse(responseBody, {
      status: upstream.status,
      headers: {
        "Cache-Control": "no-store",
        "Content-Type": upstream.headers.get("content-type") ?? "application/json",
        "X-Content-Type-Options": "nosniff",
      },
    });
  } catch {
    return NextResponse.json({ detail: "DriftGuard API is unreachable" }, { status: 503 });
  }
}

export const GET = proxy;
export const POST = proxy;
export const PUT = proxy;
