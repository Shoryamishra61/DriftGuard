import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

const REQUIRED_SECRETS = [
  "DRIFTGUARD_DASHBOARD_API_KEY",
  "DRIFTGUARD_DASHBOARD_USERNAME",
  "DRIFTGUARD_DASHBOARD_PASSWORD",
  "DRIFTGUARD_ADMIN_TOKEN",
] as const;

export async function GET() {
  const missingConfiguration = REQUIRED_SECRETS.some((name) => !process.env[name]);
  if (missingConfiguration) {
    return NextResponse.json(
      { status: "not_ready", service: "dashboard" },
      { status: 503, headers: { "Cache-Control": "no-store" } },
    );
  }

  const apiUrl = (process.env.API_INTERNAL_URL ?? "http://api:8000").replace(/\/$/, "");
  try {
    const response = await fetch(`${apiUrl}/api/v1/dashboard/session`, {
      cache: "no-store",
      headers: {
        "X-API-Key": process.env.DRIFTGUARD_DASHBOARD_API_KEY!,
        "X-DriftGuard-Admin-Token": process.env.DRIFTGUARD_ADMIN_TOKEN!,
      },
      signal: AbortSignal.timeout(1_800),
    });
    if (!response.ok) throw new Error("API is not ready");
  } catch {
    return NextResponse.json(
      { status: "not_ready", service: "dashboard" },
      { status: 503, headers: { "Cache-Control": "no-store" } },
    );
  }

  return NextResponse.json(
    { status: "ready", service: "dashboard" },
    { headers: { "Cache-Control": "no-store" } },
  );
}
