import { DriftDashboard } from "@/components/drift-dashboard";

export const dynamic = "force-dynamic";

export default function Home() {
  return (
    <DriftDashboard
      publicReadOnly={process.env.DRIFTGUARD_PUBLIC_READ_ONLY === "true"}
    />
  );
}
