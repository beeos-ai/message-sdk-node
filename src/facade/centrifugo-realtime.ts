import type {
  RealtimeConnectInput,
  RealtimeConnectionState,
  RealtimeRecoveryStatus,
  RealtimeSession,
  RealtimeTransportPort,
} from "./types.js";

/** Identity-bound bearer source. Service keys are intentionally unsupported. */
export interface RealtimeAuthProvider {
  getAccessToken(): Promise<string>;
}

/**
 * The deliberately small Centrifuge boundary used by the SDK.
 *
 * It has no subscribe, publish, channel, or subscription-token operation.
 * The Message Service injects a signed server-side subscription into the
 * connection token, so applications cannot influence the audience set.
 */
export interface CentrifugeClient {
  connect(): Promise<void> | void;
  close(): Promise<void> | void;
  updateToken(token: string): Promise<void> | void;
}

export interface CentrifugeClientOptions {
  readonly url: string;
  readonly token: string;
  onPublication(publication: unknown): void;
  onState(state: RealtimeConnectionState): void;
  /** Internal recovery result only; it contains no channel or token. */
  onRecovery?(status: RealtimeRecoveryStatus): void;
  onRefreshRequired(): Promise<void>;
  onError(error: unknown): void;
}

/** Platform owners adapt the Centrifuge JS/RN implementation at this seam. */
export interface CentrifugeClientFactory {
  create(options: CentrifugeClientOptions): CentrifugeClient;
}

export interface CentrifugoRealtimeTransportOptions {
  /** Message Service origin, e.g. https://msg.beeos.ai. */
  apiBaseUrl: string;
  authProvider: RealtimeAuthProvider;
  centrifuge: CentrifugeClientFactory;
  /** Injectable for browser, React Native, and Node test/runtime hosts. */
  fetchImpl?: typeof fetch;
}

interface IssuedSession {
  token: string;
  realtimeUrl: string;
  /** Opaque server recovery cursor. It never becomes a client channel. */
  syncCursor: string;
}

interface ActiveConnection {
  input: RealtimeConnectInput;
  client: CentrifugeClient;
  session: IssuedSession;
  handle: RealtimeSession;
  refreshPromise?: Promise<void>;
  closed: boolean;
}

/**
 * SDK-owned implementation of the v2 realtime transport.
 *
 * It obtains a server-bound session, creates exactly one physical Centrifuge
 * client for that facade connection, and refreshes only the token for that
 * same client. Raw channel names, subscription tokens, and Centrifuge APIs
 * never cross this boundary.
 */
export class CentrifugoRealtimeTransport implements RealtimeTransportPort {
  private readonly apiBaseUrl: string;
  private readonly fetchImpl: typeof fetch;
  private active?: ActiveConnection;
  private connectPromise?: Promise<RealtimeSession>;

  constructor(private readonly options: CentrifugoRealtimeTransportOptions) {
    this.apiBaseUrl = normalizeApiBaseUrl(options.apiBaseUrl);
    this.fetchImpl = options.fetchImpl ?? fetch;
  }

  async connect(input: RealtimeConnectInput): Promise<RealtimeSession> {
    if (this.active && !this.active.closed) return this.active.handle;
    if (this.connectPromise) return this.connectPromise;
    const run = this.open(input);
    this.connectPromise = run.finally(() => {
      this.connectPromise = undefined;
    });
    return this.connectPromise;
  }

  private async open(input: RealtimeConnectInput): Promise<RealtimeSession> {

    const session = await this.issueSession("/api/v2/realtime/session");
    let active: ActiveConnection | undefined;
    const client = this.options.centrifuge.create({
      url: session.realtimeUrl,
      token: session.token,
      onPublication: (publication) => {
        if (!active?.closed) active?.input.onEvent(publication);
      },
      onState: (state) => {
        if (!active || active.closed) return;
        // Centrifuge owns transient reconnect on the same physical WSS. A
        // disconnected state is therefore not a retirement signal. Only a
        // terminal failed state or explicit close allows a replacement client.
        if (state === "failed") {
          active.closed = true;
          if (this.active === active) this.active = undefined;
        }
        active.input.onState(state);
      },
      onRecovery: (status) => {
        if (!active || active.closed) return;
        active.input.onRecovery?.(status);
      },
      onRefreshRequired: async () => {
        if (!active || active.closed) throw new Error("message-sdk realtime session is closed");
        await this.refresh(active);
      },
      onError: () => {
        if (!active || active.closed) return;
        active.closed = true;
        if (this.active === active) this.active = undefined;
        void Promise.resolve(active.client.close()).catch(() => undefined);
        active.input.onState("failed");
      },
    });

    const handle: RealtimeSession = {
      get syncCursor() {
        return active?.session.syncCursor;
      },
      close: async () => {
        if (!active || active.closed) return;
        active.closed = true;
        if (this.active === active) this.active = undefined;
        await active.client.close();
      },
    };
    active = { input, client, session, handle, closed: false };
    this.active = active;

    try {
      await client.connect();
    } catch (error) {
      active.closed = true;
      if (this.active === active) this.active = undefined;
      input.onState("failed");
      await Promise.resolve(client.close()).catch(() => undefined);
      throw error;
    }
    return handle;
  }

  private async refresh(active: ActiveConnection): Promise<void> {
    if (active.refreshPromise) return active.refreshPromise;
    active.refreshPromise = (async () => {
      const refreshed = await this.issueSession("/api/v2/realtime/session/refresh");
      // A refresh must preserve the transport endpoint. Swapping an endpoint
      // underneath a connected user would silently create a second route.
      if (refreshed.realtimeUrl !== active.session.realtimeUrl) {
        active.input.onState("failed");
        throw new Error("message-sdk realtime refresh changed the WSS endpoint");
      }
      await active.client.updateToken(refreshed.token);
      active.session = refreshed;
    })().finally(() => {
      if (active) active.refreshPromise = undefined;
    });
    return active.refreshPromise;
  }

  private async issueSession(path: "/api/v2/realtime/session" | "/api/v2/realtime/session/refresh"): Promise<IssuedSession> {
    const token = await this.options.authProvider.getAccessToken();
    if (!token || !token.trim()) throw new Error("message-sdk realtime auth provider returned an empty bearer token");
    const response = await this.fetchImpl(`${this.apiBaseUrl}${path}`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}` },
    });
    if (!response.ok) {
      // Do not attach an error body: it may contain sensitive transport data.
      throw new Error(`message-sdk realtime session request failed with HTTP ${response.status}`);
    }
    return decodeIssuedSession(await response.json());
  }
}

export function createCentrifugoRealtimeTransport(
  options: CentrifugoRealtimeTransportOptions,
): CentrifugoRealtimeTransport {
  return new CentrifugoRealtimeTransport(options);
}

function decodeIssuedSession(raw: unknown): IssuedSession {
  if (!isRecord(raw)) throw new Error("message-sdk realtime session response must be an object");
  // A healthy server never returns audience material. Fail closed if a proxy
  // accidentally reintroduces it into this client boundary.
  for (const forbidden of ["channel", "channels", "private_channels", "subscription", "subscription_token", "audience_key"]) {
    if (forbidden in raw) throw new Error(`message-sdk realtime session response leaked ${forbidden}`);
  }
  const token = readString(raw, "token");
  const realtimeUrl = readString(raw, "realtime_url");
  const syncCursor = readString(raw, "sync_cursor");
  let parsed: URL;
  try {
    parsed = new URL(realtimeUrl);
  } catch {
    throw new Error("message-sdk realtime session returned an invalid realtime_url");
  }
  if (parsed.protocol !== "wss:") {
    throw new Error("message-sdk realtime session requires a wss:// realtime_url");
  }
  return { token, realtimeUrl, syncCursor };
}

function normalizeApiBaseUrl(value: string): string {
  if (!value) throw new Error("message-sdk realtime apiBaseUrl is required");
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error("message-sdk realtime apiBaseUrl must be an absolute HTTPS URL");
  }
  if (parsed.protocol !== "https:") {
    throw new Error("message-sdk realtime apiBaseUrl must use https://");
  }
  return value.replace(/\/+$/, "");
}

function readString(raw: Record<string, unknown>, key: string): string {
  const value = raw[key];
  if (typeof value !== "string" || !value) {
    throw new Error(`message-sdk realtime session response requires ${key}`);
  }
  return value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
