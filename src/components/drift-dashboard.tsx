"use client";

import {
  type CSSProperties,
  type FormEvent,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";

type TrendWindow = "24h" | "7d" | "30d";

type ServicePulse = {
  status: "healthy" | "degraded" | "unavailable";
  latency_ms?: number;
  queue_depth?: number;
  vector_count?: number | null;
  configured?: boolean;
  active?: boolean;
  timestamp?: string | null;
  worker_status?: string | null;
  pool?: {
    size: number;
    checked_in: number;
    checked_out: number;
    overflow: number;
  };
};

type PulseResponse = {
  status: string;
  timestamp: string;
  services: Record<string, ServicePulse>;
};

type PulseSample = {
  latency_ms: number;
  timestamp: number;
};

type TrendPoint = {
  timestamp: string;
  average_drift: number;
  p95_latency_ms: number | null;
  evaluations: number;
  anomalies: number;
};

type TrendsResponse = {
  window: TrendWindow;
  points: TrendPoint[];
  thresholds: Array<{
    rule_name: string;
    action_type: "NOTIFY" | "DIGEST" | "MUTE";
    threshold: number;
  }>;
  summary: {
    weighted_average_drift: number | null;
    evaluated_run_count: number;
    active_alert_count: number;
    p95_evaluation_latency_ms: number | null;
    average_end_to_end_latency_ms: number | null;
    p95_end_to_end_latency_ms: number | null;
  };
};

type AlertRecord = {
  id: string;
  evaluated_at: string;
  notified_at: string | null;
  status: string;
  route_status: "PENDING" | "DELIVERED" | "SUPPRESSED" | "FAILED";
  run_id: string;
  action_type: string;
  rule_name: string;
  drift_distance: number | null;
  prompt_text: string;
  output_text: string;
  matched_baseline_id?: string | null;
  matched_baseline_text?: string | null;
};

type AlertRule = {
  id: number;
  rule_name: string;
  threshold: number;
  action_type: "NOTIFY" | "DIGEST" | "MUTE";
  notification_target: string;
  is_active: boolean;
};

type ProjectionPoint = {
  id: string;
  point_type: "baseline" | "evaluation";
  x: number;
  y: number;
  run_id: string | null;
  baseline_set: string | null;
  drift_distance: number | null;
  matched_baseline_id: string | null;
};

type ProjectionResponse = {
  points: ProjectionPoint[];
  count: number;
  limit: number;
  has_more: boolean;
};

type TourStep = {
  target?: string;
  eyebrow: string;
  title: string;
  description: string;
  demo?: boolean;
};

type TourPosition = {
  top: number;
  left: number;
  width: number;
  height: number;
  cardTop: number;
  cardLeft: number;
};

type DemoState = "idle" | "running" | "complete" | "failed";

const TOUR_STORAGE_KEY = "driftguard:guided-judging-tour:v1";
const TOUR_RULE_NAME = "guided-judge-tour";
const TOUR_STEPS: TourStep[] = [
  {
    eyebrow: "WELCOME TO DRIFTGUARD",
    title: "A live judging flow, not a slide deck.",
    description:
      "This guided tour explains every production surface and verifies persisted semantic-drift evidence from the deployed system. Public judging access cannot change data or contact an external provider.",
  },
  {
    target: "hero",
    eyebrow: "01 · SYSTEM PATH",
    title: "Telemetry stays fast and durable.",
    description:
      "The API commits each run with its transactional outbox event before Valkey, MiniLM, Qdrant, and routing continue asynchronously.",
  },
  {
    target: "metrics",
    eyebrow: "02 · EXECUTIVE SIGNAL",
    title: "Four numbers summarize reliability.",
    description:
      "Average semantic distance, evaluated volume, active incidents, and end-to-end latency are calculated by the API for the selected time window.",
  },
  {
    target: "drift-chart",
    eyebrow: "03 · SEMANTIC TREND",
    title: "See model behavior move over time.",
    description:
      "The trend compares each production answer with its nearest project-owned baseline. Rule thresholds remain visible as operational boundaries.",
  },
  {
    target: "pulse",
    eyebrow: "04 · INFRASTRUCTURE PULSE",
    title: "The monitor monitors itself.",
    description:
      "These are live Zerops checks for PostgreSQL, Valkey, Qdrant, and the worker—including latency, queue depth, vector count, and heartbeat.",
  },
  {
    target: "vectors",
    eyebrow: "05 · VECTOR TOPOLOGY",
    title: "Baselines and evaluations stay tenant-safe.",
    description:
      "The browser receives only a bounded 2D projection. Every Qdrant search is filtered by project, point type, active baseline set, and pinned model revision.",
  },
  {
    target: "incidents",
    eyebrow: "06 · LIVE PROOF",
    title: "Verifying a real drift incident now.",
    description:
      "The tour reads a completed evaluation, its nearest baseline, drift distance, and durable routing outcome from the live deployment.",
    demo: true,
  },
  {
    target: "rules",
    eyebrow: "07 · ROUTING POLICY",
    title: "Operators control what happens next.",
    description:
      "Rules are project-scoped. NOTIFY sends immediately, DIGEST consolidates a UTC day, and MUTE records the incident without contacting an external destination.",
  },
  {
    eyebrow: "TOUR COMPLETE",
    title: "You just inspected the production path.",
    description:
      "The judging flow verified the API, PostgreSQL evidence, Valkey and worker health, Qdrant matching, and durable alert state without granting public mutation access. Replay it any time from the header.",
  },
];

const endpoint = (path: string) => `/api/driftguard/${path}`;

async function fetchJson<T>(path: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(endpoint(path), { cache: "no-store", signal });
  if (!response.ok) throw new Error(`Request failed (${response.status})`);
  return (await response.json()) as T;
}

async function mutateJson<T>(path: string, method: "POST" | "PUT", body: unknown) {
  const response = await fetch(endpoint(path), {
    method,
    headers: {
      "Content-Type": "application/json",
      "X-DriftGuard-Dashboard-Request": "1",
    },
    body: JSON.stringify(body),
    cache: "no-store",
  });
  if (!response.ok) throw new Error(`Save failed (${response.status})`);
  return (await response.json()) as T;
}

function formatMetric(value: number | null | undefined, digits = 2) {
  return value == null ? "—" : value.toFixed(digits);
}

function statusTone(status?: string) {
  if (status === "healthy") return "healthy";
  if (status === "degraded") return "degraded";
  return "unhealthy";
}

export function DriftDashboard({ publicReadOnly = false }: { publicReadOnly?: boolean }) {
  const [trendWindow, setTrendWindow] = useState<TrendWindow>("24h");
  const [alertQuery, setAlertQuery] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [pulse, setPulse] = useState<PulseResponse | null>(null);
  const [pulseHistory, setPulseHistory] = useState<Record<string, PulseSample[]>>({});
  const [trends, setTrends] = useState<TrendsResponse | null>(null);
  const [projection, setProjection] = useState<ProjectionResponse | null>(null);
  const [alerts, setAlerts] = useState<AlertRecord[]>([]);
  const [rules, setRules] = useState<AlertRule[]>([]);
  const [pulseError, setPulseError] = useState<string | null>(null);
  const [dataError, setDataError] = useState<string | null>(null);
  const [projectionError, setProjectionError] = useState<string | null>(null);
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);
  const [tourOpen, setTourOpen] = useState(false);
  const [tourIndex, setTourIndex] = useState(0);
  const [tourAutoplay, setTourAutoplay] = useState(true);
  const [tourPosition, setTourPosition] = useState<TourPosition | null>(null);
  const [demoState, setDemoState] = useState<DemoState>("idle");
  const [demoMessage, setDemoMessage] = useState("Ready to run the live proof.");
  const demoTriggered = useRef(false);

  const loadPulse = useCallback(async (signal?: AbortSignal) => {
    try {
      const nextPulse = await fetchJson<PulseResponse>("diagnostics/pulse", signal);
      setPulse(nextPulse);
      const sampledAt = Date.now();
      setPulseHistory((previous) => {
        const next = { ...previous };
        Object.entries(nextPulse.services).forEach(([name, service]) => {
          if (typeof service.latency_ms !== "number" || !Number.isFinite(service.latency_ms)) return;
          next[name] = [...(previous[name] ?? []), { latency_ms: service.latency_ms, timestamp: sampledAt }].slice(-30);
        });
        return next;
      });
      setPulseError(null);
      setLastRefresh(new Date());
    } catch (error) {
      if ((error as Error).name !== "AbortError") setPulseError((error as Error).message);
    }
  }, []);

  const loadTelemetry = useCallback(async (signal?: AbortSignal) => {
    const telemetryRequest = Promise.all([
        fetchJson<TrendsResponse>(`metrics/trends?window=${trendWindow}`, signal),
        fetchJson<{ items: AlertRecord[] }>(
          `alerts?limit=50${searchQuery ? `&q=${encodeURIComponent(searchQuery)}` : ""}`,
          signal,
        ),
        fetchJson<AlertRule[]>("alert-rules", signal),
    ]);
    const projectionRequest = fetchJson<ProjectionResponse>(
      "vectors/projection?limit=500",
      signal,
    );
    const [telemetryResult, projectionResult] = await Promise.allSettled([
      telemetryRequest,
      projectionRequest,
    ]);
    let refreshed = false;
    if (telemetryResult.status === "fulfilled") {
      const [nextTrends, nextAlerts, nextRules] = telemetryResult.value;
      setTrends(nextTrends);
      setAlerts(nextAlerts.items);
      setRules(nextRules);
      setDataError(null);
      refreshed = true;
    } else if ((telemetryResult.reason as Error).name !== "AbortError") {
      setDataError((telemetryResult.reason as Error).message);
    }

    if (projectionResult.status === "fulfilled") {
      setProjection(projectionResult.value);
      setProjectionError(null);
      refreshed = true;
    } else if ((projectionResult.reason as Error).name !== "AbortError") {
      setProjectionError((projectionResult.reason as Error).message);
    }

    if (refreshed) {
      setLastRefresh(new Date());
    }
  }, [searchQuery, trendWindow]);

  const runJudgingDemo = useCallback(async () => {
    setDemoState("running");
    setDemoMessage(
      publicReadOnly
        ? "Reading persisted evidence from the live deployment…"
        : "Preparing a safe MUTE-only routing rule…",
    );
    try {
      if (publicReadOnly) {
        const response = await fetchJson<{ items: AlertRecord[] }>("alerts?limit=100");
        const verified = response.items.find(
          (alert) => alert.drift_distance != null && alert.matched_baseline_text,
        );
        if (!verified) throw new Error("No completed semantic evidence is currently available.");
        setAlerts(response.items);
        await Promise.all([loadTelemetry(), loadPulse()]);
        setDemoState("complete");
        setDemoMessage(
          `Verified drift ${formatMetric(verified.drift_distance, 3)} · ${verified.status} / ${verified.route_status}`,
        );
        return;
      }

      const currentRules = await fetchJson<AlertRule[]>("alert-rules");
      const existing = currentRules.find((rule) => rule.rule_name === TOUR_RULE_NAME);
      const rulePayload = {
        rule_name: TOUR_RULE_NAME,
        threshold: 0,
        action_type: "MUTE" as const,
        notification_target: "Automated judging tour — no outbound delivery",
        is_active: true,
      };
      if (existing) {
        const needsRepair =
          existing.threshold !== 0 ||
          existing.action_type !== "MUTE" ||
          !existing.is_active ||
          existing.notification_target !== rulePayload.notification_target;
        if (needsRepair) {
          await mutateJson<AlertRule>(`alert-rules/${existing.id}`, "PUT", rulePayload);
        }
      } else {
        await mutateJson<AlertRule>("alert-rules", "POST", rulePayload);
      }

      setDemoMessage("Telemetry accepted. Waiting for MiniLM and Qdrant…");
      const accepted = await mutateJson<{ run_id: string }>("logs", "POST", {
        session_id: `guided-tour-${Date.now()}`,
        prompt_text: "Judge tour: verify the current DriftGuard deployment.",
        output_text:
          "A violet lighthouse calculates sandwiches while the production status remains completely unknown.",
        metadata: {
          source: "guided-judging-tour",
          purpose: "safe-live-product-proof",
        },
      });

      let verified: AlertRecord | undefined;
      let verifiedItems: AlertRecord[] = [];
      for (let attempt = 0; attempt < 30; attempt += 1) {
        await new Promise((resolve) => window.setTimeout(resolve, 800));
        const response = await fetchJson<{ items: AlertRecord[] }>(
          `alerts?limit=100&q=${encodeURIComponent(TOUR_RULE_NAME)}`,
        );
        verifiedItems = response.items;
        verified = response.items.find((alert) => alert.run_id === accepted.run_id);
        if (verified) break;
      }
      if (!verified) throw new Error("The worker did not finish within the tour window.");

      setAlertQuery(TOUR_RULE_NAME);
      setSearchQuery(TOUR_RULE_NAME);
      setAlerts(verifiedItems);
      await Promise.all([loadTelemetry(), loadPulse()]);
      setDemoState("complete");
      setDemoMessage(
        `Verified drift ${formatMetric(verified.drift_distance, 3)} · ${verified.status} / ${verified.route_status}`,
      );
    } catch (error) {
      setDemoState("failed");
      setDemoMessage((error as Error).message || "The live proof could not complete.");
    }
  }, [loadPulse, loadTelemetry, publicReadOnly]);

  useEffect(() => {
    const timer = window.setTimeout(() => setSearchQuery(alertQuery.trim()), 300);
    return () => window.clearTimeout(timer);
  }, [alertQuery]);

  useEffect(() => {
    const controller = new AbortController();
    const initialTimer = window.setTimeout(() => {
      void loadPulse(controller.signal);
      void loadTelemetry(controller.signal);
    }, 0);
    const pulseTimer = window.setInterval(() => void loadPulse(), 2_000);
    const dataTimer = window.setInterval(() => void loadTelemetry(), 15_000);
    return () => {
      controller.abort();
      window.clearTimeout(initialTimer);
      window.clearInterval(pulseTimer);
      window.clearInterval(dataTimer);
    };
  }, [loadPulse, loadTelemetry]);

  useEffect(() => {
    try {
      if (window.localStorage.getItem(TOUR_STORAGE_KEY) === "complete") return;
    } catch {
      // Storage can be unavailable in hardened browser contexts; the tour still works.
    }
    const timer = window.setTimeout(() => setTourOpen(true), 700);
    return () => window.clearTimeout(timer);
  }, []);

  useEffect(() => {
    if (!tourOpen) return;
    const step = TOUR_STEPS[tourIndex];
    const target = step.target
      ? document.querySelector<HTMLElement>(`[data-tour="${step.target}"]`)
      : null;
    if (target) target.scrollIntoView({ behavior: "smooth", block: "center" });

    const updatePosition = () => {
      if (!target) {
        setTourPosition(null);
        return;
      }
      const rect = target.getBoundingClientRect();
      const margin = 10;
      const cardWidth = Math.min(390, window.innerWidth - 32);
      const cardHeight = 310;
      const below = rect.bottom + 18;
      const cardTop =
        below + cardHeight <= window.innerHeight
          ? below
          : Math.max(16, rect.top - cardHeight - 18);
      const cardLeft = Math.min(
        Math.max(16, rect.left + rect.width / 2 - cardWidth / 2),
        window.innerWidth - cardWidth - 16,
      );
      setTourPosition({
        top: Math.max(8, rect.top - margin),
        left: Math.max(8, rect.left - margin),
        width: Math.min(window.innerWidth - 16, rect.width + margin * 2),
        height: Math.min(window.innerHeight - 16, rect.height + margin * 2),
        cardTop,
        cardLeft,
      });
    };

    const settle = window.setTimeout(updatePosition, target ? 480 : 0);
    window.addEventListener("resize", updatePosition);
    window.addEventListener("scroll", updatePosition, true);
    return () => {
      window.clearTimeout(settle);
      window.removeEventListener("resize", updatePosition);
      window.removeEventListener("scroll", updatePosition, true);
    };
  }, [tourIndex, tourOpen]);

  useEffect(() => {
    const step = TOUR_STEPS[tourIndex];
    if (!tourOpen || !step.demo || demoTriggered.current) return;
    demoTriggered.current = true;
    void runJudgingDemo();
  }, [runJudgingDemo, tourIndex, tourOpen]);

  useEffect(() => {
    if (!tourOpen || !tourAutoplay || tourIndex === 0) return;
    const step = TOUR_STEPS[tourIndex];
    if (step.demo && demoState !== "complete") return;
    if (tourIndex === TOUR_STEPS.length - 1) return;
    const timer = window.setTimeout(
      () => setTourIndex((current) => Math.min(current + 1, TOUR_STEPS.length - 1)),
      step.demo ? 3_500 : 5_500,
    );
    return () => window.clearTimeout(timer);
  }, [demoState, tourAutoplay, tourIndex, tourOpen]);

  const closeTour = useCallback(() => {
    try {
      window.localStorage.setItem(TOUR_STORAGE_KEY, "complete");
    } catch {
      // Closing the tour must never depend on browser storage availability.
    }
    setTourOpen(false);
    setTourPosition(null);
  }, []);

  const replayTour = useCallback(() => {
    demoTriggered.current = false;
    setDemoState("idle");
    setDemoMessage("Ready to run the live proof.");
    setTourIndex(0);
    setTourAutoplay(true);
    setTourOpen(true);
  }, []);

  const overallHealthy = useMemo(
    () => pulse && Object.values(pulse.services).every((service) => service.status === "healthy"),
    [pulse],
  );

  const summary = trends?.summary;
  const filteredAlerts = alerts;
  const windowLabel = { "24h": "last 24 hours", "7d": "last 7 days", "30d": "last 30 days" }[trendWindow];
  const services = [
    ["postgres", "PostgreSQL", pulse?.services.postgres],
    ["valkey", "Valkey queue", pulse?.services.valkey],
    ["qdrant", "Qdrant vectors", pulse?.services.qdrant],
    ["worker", "Evaluation worker", pulse?.services.worker],
  ] as const;

  return (
    <main className="shell">
      <header className="topbar">
        <div className="brandLockup">
          <span className="brandMark" aria-hidden="true">DG</span>
          <div>
            <p className="eyebrow">LLM RELIABILITY CONTROL PLANE</p>
            <h1>DriftGuard</h1>
          </div>
        </div>
        <div className="topbarActions">
          {publicReadOnly && <span className="accessBadge">PUBLIC · READ ONLY</span>}
          <button className="tourReplay" onClick={replayTour} type="button">
            Guided demo
          </button>
          <div className="liveState" aria-live="polite">
            <span className={`liveDot ${overallHealthy ? "online" : "offline"}`} />
            <span>{overallHealthy ? "All systems nominal" : pulse ? "System degraded" : "Connecting"}</span>
            <small>{lastRefresh ? `Updated ${lastRefresh.toLocaleTimeString()}` : "Awaiting first pulse"}</small>
          </div>
        </div>
      </header>

      <section className="hero" data-tour="hero">
        <div>
          <p className="kicker">SEMANTIC SIGNAL, WITHOUT THE LATENCY TAX</p>
          <h2>Catch silent model regressions before your users do.</h2>
          <p className="heroCopy">
            Production outputs are scored asynchronously against project-isolated baselines while ingestion stays fast and transactionally durable.
          </p>
        </div>
        <div className="architectureStrip" aria-label="Processing path">
          {['INGEST', 'OUTBOX', 'EMBED', 'COMPARE', 'ROUTE'].map((step, index) => (
            <div className="architectureStep" key={step}>
              <span>{String(index + 1).padStart(2, '0')}</span>
              <strong>{step}</strong>
            </div>
          ))}
        </div>
      </section>

      {(pulseError || dataError || projectionError) && (
        <div className="degradedBanner" role="status">
          Live telemetry is partially unavailable. {pulseError ?? dataError ?? projectionError}
        </div>
      )}

      <section className="metricGrid" aria-label="Drift summary" data-tour="metrics">
        <MetricCard label="Average drift" value={formatMetric(summary?.weighted_average_drift, 3)} unit="cosine" tone="cyan" />
        <MetricCard label="Evaluated runs" value={summary?.evaluated_run_count?.toLocaleString() ?? "—"} unit={windowLabel} tone="blue" />
        <MetricCard label="Active alerts" value={summary?.active_alert_count?.toLocaleString() ?? "—"} unit="open" tone="orange" />
        <MetricCard label="P95 end-to-end" value={formatMetric(summary?.p95_end_to_end_latency_ms, 0)} unit="milliseconds" tone="violet" />
      </section>

      <section className="contentGrid">
        <article className="panel chartPanel" data-tour="drift-chart">
          <PanelHeading eyebrow="SEMANTIC DISTANCE" title={`Drift over the ${windowLabel}`} detail="Cosine distance from the nearest project baseline" />
          <div className="panelToolbar" aria-label="Trend window">
            {(["24h", "7d", "30d"] as const).map((option) => (
              <button
                className={trendWindow === option ? "selected" : ""}
                key={option}
                onClick={() => setTrendWindow(option)}
                type="button"
              >
                {option}
              </button>
            ))}
          </div>
          <div className="chartArea">
            {trends?.points.length ? (
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={trends.points} margin={{ top: 12, right: 8, left: -20, bottom: 0 }}>
                  <CartesianGrid stroke="rgba(89, 79, 67, .12)" vertical={false} />
                  <XAxis
                    dataKey="timestamp"
                    tick={{ fill: "#7b756c", fontSize: 11 }}
                    tickFormatter={(value: string) => trendWindow === "24h"
                      ? new Date(value).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
                      : new Date(value).toLocaleDateString([], { month: "short", day: "numeric" })}
                    tickLine={false}
                    axisLine={false}
                    minTickGap={42}
                  />
                  <YAxis domain={[0, "auto"]} tick={{ fill: "#7b756c", fontSize: 11 }} tickLine={false} axisLine={false} />
                  <Tooltip contentStyle={{ background: "#fffdf8", border: "1px solid #ded6ca", borderRadius: 14, color: "#28241f" }} />
                  {trends.thresholds.map((threshold, index) => (
                    <ReferenceLine
                      key={`${threshold.rule_name}-${threshold.action_type}-${threshold.threshold}`}
                      y={threshold.threshold}
                      stroke={index % 2 ? "#b85c4b" : "#b77a32"}
                      strokeDasharray="5 5"
                      label={{ value: threshold.rule_name, fill: index % 2 ? "#b85c4b" : "#b77a32", fontSize: 10 }}
                    />
                  ))}
                  <Line type="monotone" dataKey="average_drift" stroke="#5d826d" strokeWidth={3} dot={false} activeDot={{ r: 5, fill: "#5d826d" }} />
                </LineChart>
              </ResponsiveContainer>
            ) : (
              <EmptyState label="Awaiting evaluated production telemetry" />
            )}
          </div>
        </article>

        <article className="panel pulsePanel" data-tour="pulse">
          <PanelHeading eyebrow="INFRASTRUCTURE PULSE" title="Private network health" detail="Live checks every two seconds" />
          <div className="serviceList">
            {services.map(([key, label, service]) => (
              <div className="serviceRow" key={key}>
                <span className={`serviceIndicator ${statusTone(service?.status)}`} />
                <div className="serviceIdentity">
                  <strong>{label}</strong>
                  <span>{service?.status.toUpperCase() ?? "UNKNOWN"}</span>
                </div>
                <div className="serviceMetric">
                  <strong>{formatMetric(service?.latency_ms, 1)}</strong>
                  <span>ms</span>
                </div>
                <ServiceDetail name={key} pulse={service} />
                <ServiceSparkline samples={pulseHistory[key] ?? []} status={service?.status} />
              </div>
            ))}
          </div>
        </article>
      </section>

      <section className="panel scatterPanel">
        <PanelHeading eyebrow="LATENCY CORRELATION" title="Drift versus evaluation latency" detail="Each point is a time bucket; size reflects evaluated volume" />
        <div className="chartArea scatterArea">
          {trends?.points.some((point) => point.p95_latency_ms !== null) ? (
            <ResponsiveContainer width="100%" height="100%">
              <ScatterChart margin={{ top: 12, right: 24, left: 2, bottom: 8 }}>
                <CartesianGrid stroke="rgba(89, 79, 67, .12)" />
                <XAxis
                  type="number"
                  dataKey="average_drift"
                  name="Average drift"
                  tick={{ fill: "#7b756c", fontSize: 11 }}
                  tickLine={false}
                />
                <YAxis
                  type="number"
                  dataKey="p95_latency_ms"
                  name="P95 latency"
                  unit=" ms"
                  tick={{ fill: "#7b756c", fontSize: 11 }}
                  tickLine={false}
                />
                <ZAxis type="number" dataKey="evaluations" range={[45, 340]} name="Evaluations" />
                <Tooltip
                  cursor={{ strokeDasharray: "4 4" }}
                  contentStyle={{ background: "#fffdf8", border: "1px solid #ded6ca", borderRadius: 14, color: "#28241f" }}
                />
                <Scatter
                  data={trends.points.filter((point) => point.p95_latency_ms !== null)}
                  fill="#6685a4"
                />
              </ScatterChart>
            </ResponsiveContainer>
          ) : (
            <EmptyState label="Awaiting latency-correlated evaluations" />
          )}
        </div>
      </section>

      <section className="panel scatterPanel" data-tour="vectors">
        <PanelHeading
          eyebrow="VECTOR TOPOLOGY"
          title="Project-isolated semantic clusters"
          detail={`${projection?.count ?? 0} projected vectors${projection?.has_more ? ` of more than ${projection.limit}` : ""}; raw embeddings never reach the browser`}
        />
        <div className="projectionLegend" aria-label="Vector point legend">
          <span><i className="baselinePoint" />Active and historical baselines</span>
          <span><i className="evaluationPoint" />Production evaluations</span>
        </div>
        <div className="chartArea scatterArea">
          {projection?.points.length ? (
            <ResponsiveContainer width="100%" height="100%">
              <ScatterChart margin={{ top: 12, right: 24, left: 2, bottom: 8 }}>
                <CartesianGrid stroke="rgba(89, 79, 67, .12)" />
                <XAxis
                  dataKey="x"
                  name="Projection X"
                  type="number"
                  tick={{ fill: "#7b756c", fontSize: 11 }}
                  tickLine={false}
                />
                <YAxis
                  dataKey="y"
                  name="Projection Y"
                  type="number"
                  tick={{ fill: "#7b756c", fontSize: 11 }}
                  tickLine={false}
                />
                <Tooltip
                  cursor={{ strokeDasharray: "4 4" }}
                  contentStyle={{ background: "#fffdf8", border: "1px solid #ded6ca", borderRadius: 14, color: "#28241f" }}
                />
                <Scatter
                  data={projection.points.filter((point) => point.point_type === "baseline")}
                  fill="#5d826d"
                  name="Baseline"
                />
                <Scatter
                  data={projection.points.filter((point) => point.point_type === "evaluation")}
                  fill="#bc674f"
                  name="Evaluation"
                />
              </ScatterChart>
            </ResponsiveContainer>
          ) : (
            <EmptyState label="Seed a baseline and evaluate telemetry to map semantic clusters" />
          )}
        </div>
      </section>

      <section className="contentGrid lowerGrid">
        <article className="panel alertPanel" data-tour="incidents">
          <PanelHeading eyebrow="INCIDENT STREAM" title="Recent drift alerts" detail={`${alerts.length} verified records`} />
          <div className="feedToolbar">
            <label htmlFor="alert-search">Search incidents</label>
            <input
              id="alert-search"
              onChange={(event) => setAlertQuery(event.target.value)}
              placeholder="Prompt, output, rule, or status…"
              type="search"
              value={alertQuery}
            />
          </div>
          <div className="alertList">
            {filteredAlerts.length ? filteredAlerts.map((alert) => (
              <div className="alertRow" key={alert.id}>
                <div className={`severityPill ${alert.action_type.toLowerCase()}`}>{alert.action_type}</div>
                <div className="alertCopy">
                  <strong>{alert.rule_name}</strong>
                  <p>{alert.prompt_text}</p>
                  <span>Drift {formatMetric(alert.drift_distance, 3)} · {alert.status} / {alert.route_status}</span>
                  <details>
                    <summary>Inspect semantic evidence</summary>
                    <dl>
                      <div><dt>Production output</dt><dd>{alert.output_text}</dd></div>
                      <div><dt>Nearest baseline</dt><dd>{alert.matched_baseline_text ?? "Baseline text unavailable"}</dd></div>
                      <div><dt>Baseline ID</dt><dd>{alert.matched_baseline_id ?? "No baseline matched"}</dd></div>
                    </dl>
                  </details>
                </div>
                <time title={alert.notified_at ? `Delivered ${new Date(alert.notified_at).toLocaleString()}` : `Route ${alert.route_status.toLowerCase()}`}>
                  {new Date(alert.evaluated_at).toLocaleTimeString()}
                </time>
              </div>
            )) : <EmptyState label={alertQuery.trim() ? "No incidents match this search" : "No alert records have been persisted"} />}
          </div>
        </article>

        <article className="panel rulesPanel" data-tour="rules">
          <PanelHeading eyebrow="ROUTING POLICY" title="Alert behavior" detail="Project-scoped threshold actions" />
          <div className="ruleList">
            {publicReadOnly && (
              <p className="readOnlyNotice">
                Judge view: policies are visible, while creation and edits remain disabled.
              </p>
            )}
            {!publicReadOnly && <RuleEditor onSaved={() => void loadTelemetry()} />}
            {rules.map((rule) => (
              <RuleEditor
                key={rule.id}
                onSaved={() => void loadTelemetry()}
                readOnly={publicReadOnly}
                rule={rule}
              />
            ))}
          </div>
        </article>
      </section>

      <footer>
        <span>DRIFTGUARD / ZEROPS PRIVATE VXLAN</span>
        <span>384-D COSINE · TRANSACTIONAL OUTBOX · AT-LEAST-ONCE DELIVERY</span>
      </footer>
      {tourOpen && (
        <GuidedTour
          autoplay={tourAutoplay}
          demoMessage={demoMessage}
          demoState={demoState}
          index={tourIndex}
          onAutoplayChange={setTourAutoplay}
          onBack={() => setTourIndex((current) => Math.max(0, current - 1))}
          onClose={closeTour}
          onNext={() => {
            if (tourIndex === TOUR_STEPS.length - 1) closeTour();
            else setTourIndex((current) => Math.min(current + 1, TOUR_STEPS.length - 1));
          }}
          onRetry={() => {
            demoTriggered.current = true;
            void runJudgingDemo();
          }}
          position={tourPosition}
        />
      )}
    </main>
  );
}

function MetricCard({ label, value, unit, tone }: { label: string; value: string; unit: string; tone: string }) {
  return (
    <article className={`metricCard ${tone}`}>
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{unit}</small>
    </article>
  );
}

function PanelHeading({ eyebrow, title, detail }: { eyebrow: string; title: string; detail: string }) {
  return (
    <header className="panelHeading">
      <div><span>{eyebrow}</span><h3>{title}</h3></div>
      <p>{detail}</p>
    </header>
  );
}

function EmptyState({ label }: { label: string }) {
  return <div className="emptyState"><span className="emptyPulse" />{label}</div>;
}

function GuidedTour({
  autoplay,
  demoMessage,
  demoState,
  index,
  onAutoplayChange,
  onBack,
  onClose,
  onNext,
  onRetry,
  position,
}: {
  autoplay: boolean;
  demoMessage: string;
  demoState: DemoState;
  index: number;
  onAutoplayChange: (value: boolean) => void;
  onBack: () => void;
  onClose: () => void;
  onNext: () => void;
  onRetry: () => void;
  position: TourPosition | null;
}) {
  const step = TOUR_STEPS[index];
  const isFirst = index === 0;
  const isLast = index === TOUR_STEPS.length - 1;
  const waitingForDemo = Boolean(step.demo && demoState === "running");
  const spotlightStyle = position
    ? ({
        top: position.top,
        left: position.left,
        width: position.width,
        height: position.height,
      } satisfies CSSProperties)
    : undefined;
  const cardStyle = position
    ? ({ top: position.cardTop, left: position.cardLeft } satisfies CSSProperties)
    : undefined;

  return (
    <div className="tourLayer" aria-live="polite">
      <div className={`tourScrim ${position ? "spotlightMode" : ""}`} />
      {position && <div className="tourSpotlight" style={spotlightStyle} />}
      <aside
        aria-label="DriftGuard guided judging tour"
        aria-modal="true"
        className={`tourCard ${position ? "anchored" : "centered"}`}
        role="dialog"
        style={cardStyle}
      >
        <div className="tourTopline">
          <span>{step.eyebrow}</span>
          <button aria-label="Close guided tour" onClick={onClose} type="button">×</button>
        </div>
        <div className="tourProgress" aria-label={`Step ${index + 1} of ${TOUR_STEPS.length}`}>
          {TOUR_STEPS.map((item, stepIndex) => (
            <i className={stepIndex <= index ? "complete" : ""} key={item.eyebrow} />
          ))}
        </div>
        <h2>{step.title}</h2>
        <p>{step.description}</p>
        {step.demo && (
          <div className={`tourDemoState ${demoState}`} role="status">
            <span aria-hidden="true">{demoState === "complete" ? "✓" : demoState === "failed" ? "!" : "•"}</span>
            <div><strong>{demoState === "complete" ? "Live proof verified" : demoState === "failed" ? "Proof needs attention" : "Production flow running"}</strong><small>{demoMessage}</small></div>
            {demoState === "failed" && <button onClick={onRetry} type="button">Retry</button>}
          </div>
        )}
        <div className="tourControls">
          {!isFirst && !isLast ? (
            <label><input checked={autoplay} onChange={(event) => onAutoplayChange(event.target.checked)} type="checkbox" />Autoplay</label>
          ) : <span />}
          <div>
            {!isFirst && <button className="tourSecondary" onClick={onBack} type="button">Back</button>}
            <button className="tourPrimary" disabled={waitingForDemo} onClick={onNext} type="button">
              {isFirst ? "Start guided demo" : isLast ? "Finish" : waitingForDemo ? "Running live proof…" : "Next"}
            </button>
          </div>
        </div>
        <small className="tourCounter">{String(index + 1).padStart(2, "0")} / {String(TOUR_STEPS.length).padStart(2, "0")}</small>
      </aside>
    </div>
  );
}

function RuleEditor({
  rule,
  onSaved,
  readOnly = false,
}: {
  rule?: AlertRule;
  onSaved: () => void;
  readOnly?: boolean;
}) {
  const [name, setName] = useState(rule?.rule_name ?? "");
  const [threshold, setThreshold] = useState(rule?.threshold ?? 0.3);
  const [action, setAction] = useState<AlertRule["action_type"]>(rule?.action_type ?? "NOTIFY");
  const [target, setTarget] = useState(rule?.notification_target ?? "");
  const [active, setActive] = useState(rule?.is_active ?? true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function save(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (readOnly) return;
    setSaving(true);
    setError(null);
    const payload = {
      rule_name: name.trim(),
      threshold,
      action_type: action,
      notification_target: action === "MUTE" && !target.trim() ? "suppressed" : target.trim(),
      is_active: active,
    };
    try {
      const saved = await mutateJson<AlertRule>(
        rule ? `alert-rules/${rule.id}` : "alert-rules",
        rule ? "PUT" : "POST",
        payload,
      );
      if (rule) {
        setName(saved.rule_name);
        setThreshold(saved.threshold);
        setAction(saved.action_type);
        setTarget(saved.notification_target);
        setActive(saved.is_active);
      } else {
        setName("");
        setThreshold(0.3);
        setAction("NOTIFY");
        setTarget("");
        setActive(true);
      }
      onSaved();
    } catch (saveError) {
      setError((saveError as Error).message);
    } finally {
      setSaving(false);
    }
  }

  return (
    <form className={`ruleEditor ${rule ? "existing" : "new"}`} onSubmit={save}>
      <div className="ruleEditorHeading">
        <strong>{rule ? `Rule #${rule.id}` : "Create routing rule"}</strong>
        {error && <span role="status">{error}</span>}
      </div>
      <label>
        <span>Name</span>
        <input disabled={readOnly} maxLength={100} onChange={(event) => setName(event.target.value)} required value={name} />
      </label>
      <label>
        <span>Threshold</span>
        <input
          max="2"
          min="0"
          disabled={readOnly}
          onChange={(event) => setThreshold(event.target.valueAsNumber)}
          required
          step="0.01"
          type="number"
          value={threshold}
        />
      </label>
      <label>
        <span>Route</span>
        <select disabled={readOnly} onChange={(event) => setAction(event.target.value as AlertRule["action_type"])} value={action}>
          <option value="NOTIFY">Notify now</option>
          <option value="DIGEST">Daily digest</option>
          <option value="MUTE">Mute</option>
        </select>
      </label>
      <label className="targetField">
        <span>
          {action === "MUTE"
            ? "Suppression note"
            : action === "DIGEST"
              ? "HTTPS webhook or mailto: recipient"
              : "Slack, Discord, PagerDuty, or allowlisted HTTPS"}
        </span>
        <input
          disabled={readOnly}
          maxLength={255}
          onChange={(event) => setTarget(event.target.value)}
          placeholder={action === "MUTE" ? "Optional" : action === "DIGEST" ? "mailto:alerts@example.com" : "https://hooks.slack.com/services/..."}
          required={action !== "MUTE"}
          type={action === "MUTE" ? "text" : "url"}
          value={target}
        />
      </label>
      <label className="activeToggle">
        <input checked={active} disabled={readOnly} onChange={(event) => setActive(event.target.checked)} type="checkbox" />
        <span>Active</span>
      </label>
      <button disabled={saving || readOnly} type="submit">
        {readOnly ? "Read only" : saving ? "Saving…" : rule ? "Save" : "Add rule"}
      </button>
    </form>
  );
}

function ServiceDetail({ name, pulse }: { name: string; pulse?: ServicePulse }) {
  if (name === "valkey") return <div className="serviceDetail"><strong>{pulse?.queue_depth ?? "—"}</strong><span>queued</span></div>;
  if (name === "postgres") return <div className="serviceDetail"><strong>{pulse?.pool?.checked_out ?? "—"}/{pulse?.pool?.checked_in ?? "—"}</strong><span>active / idle</span></div>;
  if (name === "qdrant") return <div className="serviceDetail"><strong>{pulse?.vector_count?.toLocaleString() ?? "—"}</strong><span>vectors</span></div>;
  return <div className="serviceDetail"><strong>{pulse?.active && pulse.timestamp ? "LIVE" : "—"}</strong><span>heartbeat</span></div>;
}

function ServiceSparkline({ samples, status }: { samples: PulseSample[]; status?: string }) {
  if (samples.length < 2) return <div className="serviceSparkline" aria-label="Collecting latency history" />;
  return (
    <div className="serviceSparkline" aria-label="Recent latency trend">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={samples}>
          <XAxis dataKey="timestamp" hide type="number" domain={["dataMin", "dataMax"]} />
          <YAxis dataKey="latency_ms" hide domain={[0, "auto"]} />
          <Line
            dataKey="latency_ms"
            dot={false}
            isAnimationActive={false}
            stroke={status === "healthy" ? "#5d826d" : status === "degraded" ? "#b77a32" : "#b85c4b"}
            strokeWidth={1.5}
            type="monotone"
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
