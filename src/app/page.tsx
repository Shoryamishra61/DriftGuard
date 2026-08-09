import { DriftDashboard } from "@/components/drift-dashboard";

export default function Home() {
  return (
    <DriftDashboard
      publicReadOnly={process.env.DRIFTGUARD_PUBLIC_READ_ONLY === "true"}
    />
  );
}
