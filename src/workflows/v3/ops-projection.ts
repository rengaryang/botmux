/**
 * v3 dashboard projection: a run dir (journal.ndjson + dag.json) → a read-only
 * `RunView` the dashboard renders as a DAG graph with per-node live/replay
 * terminals.
 *
 * Mirrors `src/workflows/ops-projection.ts` (the v0.2 read-only projection):
 * runId allowlist + path-inside-dir defense before touching the filesystem,
 * and defensive reads (missing/partial files degrade gracefully, never throw)
 * so a half-written run still renders.
 *
 * Node status comes from `materialize(journal)` (the canonical fold); live
 * terminal info comes from the `nodeSessionReady` event (written mid-run, kept
 * even if the node later fails); edges/goal come from the persisted dag.json.
 */
import { readFileSync, existsSync, readdirSync } from 'node:fs';
import { join, resolve, sep } from 'node:path';
import { homedir } from 'node:os';
import { readJournal } from './journal.js';
import { openV3WorkerAttempts } from './attempt-ledger.js';
import { materialize, type V3RunStatus } from './state.js';
import type { StoredEvent } from './event-contract.js';
import type { V3NodeStatus } from './orchestrator.js';
import { parseFrozenBotSnapshots } from './bot-resolve.js';

/** Same allowlist shape as v0.2 ops-projection — validate BEFORE path-joining a
 *  caller-supplied runId, so it can't escape runsDir via traversal. */
const RUN_ID_RE = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
export function isValidRunId(runId: string): boolean {
  return RUN_ID_RE.test(runId);
}

/** Allowlist for the gate/wait id (`<instanceId|nodeId>-gate`, e.g. `A#001-gate`).
 *  Same defense as {@link isValidRunId}: a card callback's `waitId` is joined into
 *  `runDir/waits/<waitId>.json`, so it must be validated BEFORE the join or a
 *  `../..` waitId escapes the run dir. Includes `#` for instance-scoped ids; the
 *  nonce check alone is NOT sufficient (it is non-secret and reproducible). */
const WAIT_ID_RE = /^[A-Za-z0-9][A-Za-z0-9._#-]{0,127}$/;
export function isValidWaitId(waitId: string): boolean {
  return WAIT_ID_RE.test(waitId);
}

/** Default run root, aligned with cli-run.ts / grill-state.ts. */
export function defaultRunsDir(): string {
  return join(homedir(), '.botmux', 'v3-runs');
}

export interface WebTerminalView {
  sessionId: string;
  webPort?: number;
  // NO `token` — the dashboard view is READ-ONLY; the write token is never
  // exposed through the read API (codex security review 2026-06-02).  A live
  // view connects read-only to `http://<host>:<webPort>/`.
  /** `live` while the node's work worker is in flight; `closed` once the node
   *  reached a terminal verdict (then replay via the pty-log endpoint). */
  status: 'live' | 'closed';
}

export interface RunNodeView {
  id: string;
  status: V3NodeStatus;
  /** Upstream node ids (graph edges) — from the persisted dag.json. */
  depends: string[];
  goal?: string;
  attemptId?: string;
  /** Present once the node's worker reported `nodeSessionReady`. */
  webTerminal?: WebTerminalView;
  /** Whether a raw PTY log exists for replay.  The absolute path is NOT exposed
   *  to the frontend (codex review) — a replay endpoint locates it server-side
   *  via `ptyLogPathFor(runsDir, runId, nodeId)`. */
  hasPtyLog: boolean;
  /** Whether the node produced a manifest (i.e. succeeded with a recorded
   *  manifest).  The raw fs path is NOT exposed — same rationale as `hasPtyLog`:
   *  `GET /api/v3/runs/:id` is link-shareable public-read, so a public reader
   *  must never see absolute `/root/.botmux/...` paths (codex security review
   *  2026-06-02).  A download, if ever needed, goes through a cookie-auth
   *  endpoint that locates the file server-side via runId/nodeId. */
  hasManifest: boolean;
  /** For blocked/failed nodes: the coarse error class + the node's
   *  self-reported `manifest.error.code` (e.g. AUTH_REQUIRED).  The free-text
   *  `message` is deliberately NOT projected — it can quote validator problems
   *  containing absolute paths, and this view is link-shareable public-read. */
  errorClass?: string;
  errorCode?: string;
  /** True for composite loop nodes (from dag.json). */
  isLoop?: boolean;
  /** Composite loop progress (loop nodes only, once started).  `lastDecision`
   *  is the coarse enum; the free-text decision `detail` is deliberately NOT
   *  projected (it can quote agent-written result strings) — same public-read
   *  rationale as `message`. */
  loopState?: {
    iteration: number;
    maxIterations?: number;
    granted: number;
    lastDecision?: 'exit' | 'continue' | 'exhausted';
    /** Per-iteration verdict history (enum ONLY — same no-detail rationale).
     *  Lets the dashboard draw an honest round timeline instead of guessing
     *  past verdicts from `lastDecision`. */
    decisions: Array<{ iteration: number; decision: 'exit' | 'continue' | 'exhausted' }>;
    /** Body template shape (authored ids + body-internal depends) — the
     *  dashboard lays every round's mini-dag on this skeleton, so a round
     *  whose later nodes have not dispatched yet still shows its full shape
     *  (undispatched slots render as pending ghosts). */
    bodyTemplate: Array<{ id: string; depends: string[] }>;
  };
  /** For loop BODY INSTANCE nodes (`repairLoop.i001.code`): the structured
   *  membership ref from the dispatch event.  The id stays opaque — group by
   *  THIS, never parse the id string. */
  loop?: { loopId: string; iteration: number; bodyNodeId: string };
  /** Read-only workflow reliability projection. No absolute paths or free-text
   *  worker output are exposed; this is safe for link-shareable run views. */
  reliability: RunNodeReliabilityView;
  execution?: {
    selector: string;
    profileId?: string;
    cli: string;
    model?: string;
    workingDir: string;
    timeoutSec: number;
    costTier: 'low' | 'medium' | 'high' | 'unknown';
    riskLevel: 'low' | 'medium' | 'high';
    gated: boolean;
  };
}

export interface RunView {
  runId: string;
  runStatus: V3RunStatus;
  failedNodeId?: string;
  blockedNodeId?: string;
  nodes: RunNodeView[];
  reliability: RunReliabilityView;
}

export interface RunNodeReliabilityView {
  dispatchedAttempts: number;
  completedAttempts: number;
  openAttempts: number;
  timeoutFailures: number;
  orphanRecoveries: number;
  drainObservations: number;
  signals: { sigint: number; sigkill: number };
  orphanRecoveryExhausted: boolean;
  lastDrain?: {
    status: 'closed' | 'pending' | 'unknown';
    reason?: string;
    signal?: 'SIGINT' | 'SIGKILL';
    workerPid?: number;
    workerProcStart?: string;
  };
}

export interface RunReliabilityView {
  dispatchedAttempts: number;
  completedAttempts: number;
  openAttempts: number;
  timeoutFailures: number;
  orphanRecoveries: number;
  orphanRecoveryRate: number;
  drainObservations: number;
  signals: { sigint: number; sigkill: number };
  orphanRecoveryExhausted: boolean;
  diagnostics: string[];
}

interface DagNodeLite {
  id: string;
  depends: string[];
  goal?: string;
  selector?: string;
  timeoutSec?: number;
  gated?: boolean;
  type?: string;
  isLoop?: boolean;
  maxIterations?: number;
  /** Loop nodes only: body template — body-INTERNAL depends + goal per body
   *  node id.  Used to give body instances their real intra-round edges (the
   *  dashboard draws each round as a mini-dag, not a flat chip row). */
  body?: Map<string, { depends: string[]; goal?: string }>;
}

/** Flatten a persisted depends entry to its source nodeId for display.
 *  runDirs persist the NORMALIZED dag (edge-activation design §1.1), so
 *  entries are `{ from, when? }` objects in new runs and plain strings in
 *  pre-edge-activation runDirs — the projection accepts both. */
function dependsToIds(v: unknown): string[] {
  if (!Array.isArray(v)) return [];
  return v
    .map((d) => (typeof d === 'string' ? d : String((d as { from?: unknown } | null)?.from ?? '')))
    .filter((s) => s.length > 0);
}

function readDagNodes(runDir: string): DagNodeLite[] {
  const p = join(runDir, 'dag.json');
  if (!existsSync(p)) return [];
  try {
    const dag = JSON.parse(readFileSync(p, 'utf-8')) as { nodes?: unknown };
    if (!dag || !Array.isArray(dag.nodes)) return [];
    return dag.nodes.map((raw): DagNodeLite => {
      const n = raw as { id?: unknown; depends?: unknown; goal?: unknown; type?: unknown; bot?: unknown; executionProfile?: unknown; timeoutSec?: unknown; humanGate?: unknown; maxIterations?: unknown; body?: unknown };
      let body: DagNodeLite['body'];
      const bodyNodes = (n.body as { nodes?: unknown } | undefined)?.nodes;
      if (n.type === 'loop' && Array.isArray(bodyNodes)) {
        body = new Map(
          bodyNodes.map((b) => {
            const bn = b as { id?: unknown; depends?: unknown; goal?: unknown };
            return [
              String(bn.id),
              {
                depends: dependsToIds(bn.depends),
                goal: typeof bn.goal === 'string' ? bn.goal : undefined,
              },
            ];
          }),
        );
      }
      return {
        id: String(n.id),
        depends: dependsToIds(n.depends),
        goal: typeof n.goal === 'string' ? n.goal : undefined,
        selector: typeof n.executionProfile === 'string' ? n.executionProfile : typeof n.bot === 'string' ? n.bot : '',
        timeoutSec: typeof n.timeoutSec === 'number' ? n.timeoutSec : undefined,
        gated: Boolean(n.humanGate),
        type: typeof n.type === 'string' ? n.type : undefined,
        isLoop: n.type === 'loop' || undefined,
        maxIterations: typeof n.maxIterations === 'number' ? n.maxIterations : undefined,
        body,
      };
    });
  } catch {
    return [];
  }
}

function emptyNodeReliability(): RunNodeReliabilityView {
  return {
    dispatchedAttempts: 0,
    completedAttempts: 0,
    openAttempts: 0,
    timeoutFailures: 0,
    orphanRecoveries: 0,
    drainObservations: 0,
    signals: { sigint: 0, sigkill: 0 },
    orphanRecoveryExhausted: false,
  };
}

function nodeReliability(
  map: Map<string, RunNodeReliabilityView>,
  nodeId: string,
): RunNodeReliabilityView {
  const existing = map.get(nodeId);
  if (existing) return existing;
  const fresh = emptyNodeReliability();
  map.set(nodeId, fresh);
  return fresh;
}

function summarizeReliability(events: readonly StoredEvent[]): {
  run: RunReliabilityView;
  nodes: Map<string, RunNodeReliabilityView>;
} {
  const nodes = new Map<string, RunNodeReliabilityView>();
  const dispatched = new Set<string>();
  const completed = new Set<string>();
  const dispatchedByNode = new Map<string, Set<string>>();
  const completedByNode = new Map<string, Set<string>>();
  let drainObservations = 0;
  const signals = { sigint: 0, sigkill: 0 };

  for (const event of events) {
    if (event.type === 'nodeDispatched') {
      dispatched.add(event.attemptId);
      const node = nodeReliability(nodes, event.nodeId);
      const set = dispatchedByNode.get(event.nodeId) ?? new Set<string>();
      if (!set.has(event.attemptId)) {
        set.add(event.attemptId);
        dispatchedByNode.set(event.nodeId, set);
        node.dispatchedAttempts += 1;
      }
      continue;
    }
    if (
      event.type === 'nodeSucceeded' ||
      event.type === 'nodeFailed' ||
      event.type === 'nodeBlocked' ||
      event.type === 'nodeAttemptDrained'
    ) {
      completed.add(event.attemptId);
      const node = nodeReliability(nodes, event.nodeId);
      const set = completedByNode.get(event.nodeId) ?? new Set<string>();
      if (!set.has(event.attemptId)) {
        set.add(event.attemptId);
        completedByNode.set(event.nodeId, set);
        node.completedAttempts += 1;
      }
      if (event.type === 'nodeFailed' && event.errorClass === 'timeout') node.timeoutFailures += 1;
      if (event.type === 'nodeAttemptDrained' && event.reason === 'orphanRecovery') node.orphanRecoveries += 1;
      if (
        event.type === 'nodeBlocked' &&
        event.errorCode === 'ORPHAN_RECOVERY_EXHAUSTED'
      ) node.orphanRecoveryExhausted = true;
      continue;
    }
    if (event.type === 'nodeCancelled' && event.reason === 'runCancelled' && event.attemptId) {
      completed.add(event.attemptId);
      const node = nodeReliability(nodes, event.nodeId);
      const set = completedByNode.get(event.nodeId) ?? new Set<string>();
      if (!set.has(event.attemptId)) {
        set.add(event.attemptId);
        completedByNode.set(event.nodeId, set);
        node.completedAttempts += 1;
      }
      continue;
    }
    if (event.type === 'nodeAttemptDrainObserved') {
      drainObservations += 1;
      const node = nodeReliability(nodes, event.nodeId);
      node.drainObservations += 1;
      node.lastDrain = {
        status: event.status,
        ...(event.leaseReason ? { reason: event.leaseReason } : {}),
        ...(event.signal ? { signal: event.signal } : {}),
        ...(event.workerPid !== undefined ? { workerPid: event.workerPid } : {}),
        ...(event.workerProcStart ? { workerProcStart: event.workerProcStart } : {}),
      };
      if (event.signal === 'SIGINT') {
        signals.sigint += 1;
        node.signals.sigint += 1;
      } else if (event.signal === 'SIGKILL') {
        signals.sigkill += 1;
        node.signals.sigkill += 1;
      }
    }
  }

  let openAttempts: ReturnType<typeof openV3WorkerAttempts> = [];
  try {
    openAttempts = openV3WorkerAttempts(events);
    for (const attempt of openAttempts) {
      nodeReliability(nodes, attempt.nodeId).openAttempts += 1;
    }
  } catch {
    // The runtime path fails closed on ledger corruption. The read-only
    // projection keeps reporting the durable counters it can still compute.
  }

  const nodeValues = [...nodes.values()];
  const dispatchedAttempts = dispatched.size;
  const orphanRecoveries = nodeValues.reduce((sum, item) => sum + item.orphanRecoveries, 0);
  const timeoutFailures = nodeValues.reduce((sum, item) => sum + item.timeoutFailures, 0);
  const orphanRecoveryExhausted = nodeValues.some((item) => item.orphanRecoveryExhausted);
  const diagnostics: string[] = [];
  const goalCliTimeouts = events.filter(
    (event) => event.type === 'runCancelRequested' && event.reason === 'goal-cli-timeout',
  ).length;
  if (goalCliTimeouts > 0) {
    diagnostics.push(
      'GOAL_CLI_TIMEOUT: outer goal-run timeout requested bounded cancellation; the external caller must re-drive the same runId or raise its own 900s budget.',
    );
  }
  if (openAttempts.length > 0) {
    diagnostics.push(
      `OPEN_ATTEMPTS: ${openAttempts.length} attempt(s) still lack durable close proof; re-drive the same runId to drain or recover, not a new run.`,
    );
  }
  if (timeoutFailures > 0) {
    diagnostics.push(
      `NODE_TIMEOUT: ${timeoutFailures} attempt(s) exceeded their bounded timeout; increase node timeoutSec or the goal-run --timeout only when the work genuinely needs more time.`,
    );
  }
  if (orphanRecoveries > 0) {
    diagnostics.push(
      `ORPHAN_RECOVERY: ${orphanRecoveries} attempt(s) were drained after driver loss; replacement attempts are journal-numbered to avoid duplicate dispatch directories.`,
    );
  }
  if (signals.sigkill > 0) {
    diagnostics.push(
      `SIGKILL_USED: ${signals.sigkill} external drain signal(s) escalated past SIGINT grace; inspect worker shutdown responsiveness.`,
    );
  }
  if (orphanRecoveryExhausted) {
    diagnostics.push(
      'ORPHAN_RECOVERY_EXHAUSTED: automatic orphan recovery reached its cap; inspect daemon/CLI stability before manually retrying the blocked attempt.',
    );
  }

  return {
    run: {
      dispatchedAttempts,
      completedAttempts: completed.size,
      openAttempts: openAttempts.length,
      timeoutFailures,
      orphanRecoveries,
      orphanRecoveryRate: dispatchedAttempts === 0 ? 0 : orphanRecoveries / dispatchedAttempts,
      drainObservations,
      signals,
      orphanRecoveryExhausted,
      diagnostics,
    },
    nodes,
  };
}

export function summarizeRunReliability(events: readonly StoredEvent[]): RunReliabilityView {
  return summarizeReliability(events).run;
}

export function summarizeRunNodeReliability(events: readonly StoredEvent[]): Map<string, RunNodeReliabilityView> {
  return summarizeReliability(events).nodes;
}

/**
 * Project an already-resolved run dir into a `RunView`.  Read-only + defensive:
 * a missing journal / dag still yields a (possibly sparse) view rather than
 * throwing — the dashboard polls this while a run is mid-flight.
 */
export function projectRun(runId: string, runDir: string): RunView {
  const journalPath = join(runDir, 'journal.ndjson');
  // readJournal is fail-loud on mid-file corruption (hardening #11) for the
  // RUNTIME paths, but projectRun is the read-only dashboard projection and is
  // contractually defensive (see above): degrade to a sparse view instead of
  // throwing, so one corrupt journal can't 500 the whole runs list or its own
  // detail page. The runtime (decision-making) callers still get the throw.
  let events: ReturnType<typeof readJournal> = [];
  if (existsSync(journalPath)) {
    try {
      events = readJournal(journalPath);
    } catch {
      events = [];
    }
  }
  const snap = materialize(events);
  const dagNodes = readDagNodes(runDir);
  let executionSnapshots = new Map<string, import('./contract.js').BotSnapshot>();
  try { executionSnapshots = parseFrozenBotSnapshots(JSON.parse(readFileSync(join(runDir, 'bots.snapshot.json'), 'utf8'))); } catch { /* pre-envelope or partial authoring */ }
  const reliabilitySummary = summarizeReliability(events);
  const reliability = reliabilitySummary.run;

  const sessions = new Map<string, { sessionId: string; webPort?: number; ptyLogPath?: string }>();
  const manifests = new Map<string, string>();
  const errors = new Map<string, { errorClass: string; errorCode?: string }>();
  const loopRefs = new Map<string, { loopId: string; iteration: number; bodyNodeId: string }>();
  const loopDecisions = new Map<string, Array<{ iteration: number; decision: 'exit' | 'continue' | 'exhausted' }>>();
  const reliabilityByNode = reliabilitySummary.nodes;
  for (const e of events) {
    if (e.type === 'nodeSessionReady') {
      sessions.set(e.nodeId, { ...e.sessionInfo, ptyLogPath: e.ptyLogPath });
    } else if (e.type === 'nodeSucceeded') {
      manifests.set(e.nodeId, e.manifestPath);
      errors.delete(e.nodeId); // a later successful attempt clears the error
    } else if (e.type === 'nodeFailed' || e.type === 'nodeBlocked') {
      errors.set(e.nodeId, { errorClass: e.errorClass, errorCode: e.errorCode });
    } else if (e.type === 'nodeDispatched' && e.loop) {
      loopRefs.set(e.nodeId, e.loop);
    } else if (e.type === 'loopIterationDecision') {
      // enum only — e.detail can quote agent-written strings, never project it.
      (loopDecisions.get(e.loopId) ?? loopDecisions.set(e.loopId, []).get(e.loopId)!)
        .push({ iteration: e.iteration, decision: e.decision });
    }
  }

  // Prefer the dag's node order (covers not-yet-dispatched nodes); fall back to
  // whatever the journal has seen if dag.json is missing.  Loop body INSTANCES
  // exist only in the journal (dag.json holds the body template) — append them
  // after the dag order, in first-dispatch order.
  const dagById = new Map(dagNodes.map((n) => [n.id, n]));
  const instanceIds = [...loopRefs.keys()].filter((id) => !dagById.has(id));
  const ids = dagNodes.length
    ? [...dagNodes.map((n) => n.id), ...instanceIds]
    : [...snap.nodes.keys()];

  // (loopId, iteration, bodyNodeId) → instance id, so an instance's template
  // depends map to its SAME-round sibling instances (structured refs only —
  // never derived by parsing the opaque instance id).
  const instByKey = new Map<string, string>();
  for (const [id, ref] of loopRefs) instByKey.set(`${ref.loopId} ${ref.iteration} ${ref.bodyNodeId}`, id);
  const instanceMeta = (id: string): { depends: string[]; goal?: string } | undefined => {
    const ref = loopRefs.get(id);
    if (!ref) return undefined;
    const tpl = dagById.get(ref.loopId)?.body?.get(ref.bodyNodeId);
    if (!tpl) return undefined;
    return {
      depends: tpl.depends
        .map((d) => instByKey.get(`${ref.loopId} ${ref.iteration} ${d}`))
        .filter((x): x is string => Boolean(x)),
      goal: tpl.goal,
    };
  };

  const nodes: RunNodeView[] = ids.map((id) => {
    const status = (snap.nodes.get(id)?.status ?? 'pending') as V3NodeStatus;
    const sess = sessions.get(id);
    const inst = instanceMeta(id);
    const view: RunNodeView = {
      id,
      status,
      depends: inst?.depends ?? dagById.get(id)?.depends ?? [],
      goal: inst?.goal ?? dagById.get(id)?.goal,
      attemptId: snap.attempts.get(id),
      hasPtyLog: Boolean(sess?.ptyLogPath),
      hasManifest: manifests.has(id),
      reliability: reliabilityByNode.get(id) ?? emptyNodeReliability(),
    };
    if (sess) {
      view.webTerminal = {
        sessionId: sess.sessionId,
        webPort: sess.webPort,
        status: status === 'running' ? 'live' : 'closed',
      };
    }
    const err = errors.get(id);
    if (err && (status === 'blocked' || status === 'failed')) {
      view.errorClass = err.errorClass;
      view.errorCode = err.errorCode;
    }
    const dagNode = dagById.get(id);
    const execution = dagNode ? executionSnapshots.get(dagNode.selector ?? '') : undefined;
    if (execution && dagNode?.type === 'goal') {
      view.execution = {
        selector: dagNode.selector ?? '',
        ...(execution.executionProfileId ? { profileId: execution.executionProfileId } : {}),
        cli: execution.cliId,
        ...(execution.model ? { model: execution.model } : {}),
        workingDir: execution.workingDir,
        timeoutSec: Math.min(dagNode.timeoutSec ?? execution.timeoutDefaultSec ?? 1800, execution.timeoutMaxSec ?? dagNode.timeoutSec ?? 1800),
        costTier: execution.costTier ?? 'unknown',
        riskLevel: dagNode.gated ? 'high' : /部署|删除|写配置|deploy|delete/iu.test(dagNode.goal ?? '') ? 'medium' : 'low',
        gated: Boolean(dagNode.gated),
      };
    }
    if (dagNode?.isLoop) view.isLoop = true;
    const ls = snap.loops.get(id);
    if (ls) {
      view.isLoop = true;
      view.loopState = {
        iteration: ls.iteration,
        maxIterations: dagNode?.maxIterations,
        granted: ls.granted,
        lastDecision: ls.lastDecision,
        decisions: loopDecisions.get(id) ?? [],
        bodyTemplate: [...(dagNode?.body ?? new Map())].map(([bid, t]) => ({ id: bid, depends: t.depends })),
      };
    }
    const ref = loopRefs.get(id);
    if (ref) view.loop = ref;
    return view;
  });

  return {
    runId,
    runStatus: snap.runStatus,
    failedNodeId: snap.failedNodeId,
    blockedNodeId: snap.blockedNodeId,
    nodes,
    reliability,
  };
}

/**
 * Validate a caller-supplied `runId`, resolve it under `runsDir` (re-checking
 * the join stays inside runsDir — defense in depth), and project.  Returns
 * `undefined` for an invalid id / traversal attempt / missing run.
 */
export function projectRunById(runsDir: string, runId: string): RunView | undefined {
  if (!isValidRunId(runId)) return undefined;
  const root = resolve(runsDir);
  const runDir = resolve(root, runId);
  if (runDir !== root && !runDir.startsWith(root + sep)) return undefined;
  if (!existsSync(runDir)) return undefined;
  return projectRun(runId, runDir);
}

/**
 * Server-side resolver for a node's raw PTY log path (for the replay endpoint).
 * The absolute path is NEVER in the public RunView — callers locate it here by
 * runId/nodeId, and it's re-validated to be inside the run dir (defense in
 * depth) before any read.  Returns undefined for invalid id / traversal / no log.
 */
export function ptyLogPathFor(runsDir: string, runId: string, nodeId: string): string | undefined {
  if (!isValidRunId(runId)) return undefined;
  const root = resolve(runsDir);
  const runDir = resolve(root, runId);
  if (runDir !== root && !runDir.startsWith(root + sep)) return undefined;
  const journalPath = join(runDir, 'journal.ndjson');
  if (!existsSync(journalPath)) return undefined;

  // Read-only endpoint: a corrupt journal (readJournal fail-loud) degrades to
  // "no pty log" (→ 404) rather than a 500 (hardening #11 read-only carve-out).
  let events: ReturnType<typeof readJournal>;
  try {
    events = readJournal(journalPath);
  } catch {
    return undefined;
  }
  let recorded: string | undefined;
  for (const e of events) {
    if (e.type === 'nodeSessionReady' && e.nodeId === nodeId && e.ptyLogPath) recorded = e.ptyLogPath;
  }
  if (!recorded) return undefined;

  const abs = resolve(recorded);
  if (abs !== runDir && !abs.startsWith(runDir + sep)) return undefined;
  return existsSync(abs) ? abs : undefined;
}

export interface RunSummary {
  runId: string;
  runStatus: V3RunStatus;
  nodeCount: number;
}

/** List runs under `runsDir` (dirs that have a journal.ndjson), newest-first by
 *  name (runIds carry a `<slug>-<yymmdd-hhmm>` stamp so name sort ≈ time sort). */
export function listRuns(runsDir: string): RunSummary[] {
  if (!existsSync(runsDir)) return [];
  const out: RunSummary[] = [];
  for (const entry of readdirSync(runsDir, { withFileTypes: true })) {
    if (!entry.isDirectory() || !isValidRunId(entry.name)) continue;
    if (!existsSync(join(runsDir, entry.name, 'journal.ndjson'))) continue;
    const view = projectRun(entry.name, join(runsDir, entry.name));
    out.push({ runId: view.runId, runStatus: view.runStatus, nodeCount: view.nodes.length });
  }
  return out.sort((a, b) => b.runId.localeCompare(a.runId));
}
