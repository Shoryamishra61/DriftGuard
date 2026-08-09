import { NextResponse } from "next/server";

export function GET() {
  return NextResponse.json(
    { status: "alive", service: "dashboard" },
    { headers: { "Cache-Control": "no-store" } },
  );
}
