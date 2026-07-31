import type {
  RealtimeConnectInput,
  RealtimeConnectionState,
  RealtimeSession,
  RealtimeSessionPort,
} from "../facade/contracts.js";

interface RealtimeCredentials {
  readonly token: string;
  readonly realtimeUrl: string;
}

interface RealtimeCredentialsProvider {
  getCredentials(): Promise<RealtimeCredentials>;
}

interface InternalCentrifugeClient {
  connect(): Promise<void> | void;
  close(): Promise<void> | void;
  updateToken(token: string): Promise<void> | void;
}

interface InternalCentrifugeFactory {
  create(options: {
    url: string;
    token: string;
    onEvent(event: unknown): void;
    onState(state: RealtimeConnectionState): void;
    onRefresh(): Promise<void>;
    onError(error: unknown): void;
  }): InternalCentrifugeClient;
}

export interface CentrifugoSessionAdapterOptions {
  readonly credentials: RealtimeCredentialsProvider;
  readonly factory: InternalCentrifugeFactory;
}

/** Owns one server-bound personal WSS. Raw subscribe and publish are absent. */
export class CentrifugoSessionAdapter implements RealtimeSessionPort {
  private active?: {
    readonly client: InternalCentrifugeClient;
    readonly session: RealtimeSession;
    url: string;
    closed: boolean;
    refresh?: Promise<void>;
  };
  private opening?: Promise<RealtimeSession>;

  constructor(private readonly options: CentrifugoSessionAdapterOptions) {}

  connect(input: RealtimeConnectInput): Promise<RealtimeSession> {
    if (this.active && !this.active.closed) return Promise.resolve(this.active.session);
    if (this.opening) return this.opening;
    this.opening = this.open(input).finally(() => { this.opening = undefined; });
    return this.opening;
  }

  private async open(input: RealtimeConnectInput): Promise<RealtimeSession> {
    input.onState("connecting");
    const credentials = await this.options.credentials.getCredentials();
    validateCredentials(credentials);
    let active: CentrifugoSessionAdapter["active"];
    let connectedStateObserved = false;
    const client = this.options.factory.create({
      url: credentials.realtimeUrl,
      token: credentials.token,
      onEvent: (event) => { if (active && !active.closed) input.onEvent(event); },
      onState: (state) => {
        if (state === "connected") connectedStateObserved = true;
        if (active && !active.closed) input.onState(state);
      },
      onRefresh: async () => {
        if (!active || active.closed) throw new Error("realtime session is closed");
        if (active.refresh) return active.refresh;
        active.refresh = this.refresh(active).finally(() => {
          if (active) active.refresh = undefined;
        });
        return active.refresh;
      },
      onError: (error) => {
        if (!active || active.closed) return;
        active.closed = true;
        void Promise.resolve(active.client.close()).catch(() => undefined);
        input.onState("failed");
        void error;
      },
    });
    const session: RealtimeSession = {
      close: async () => {
        if (!active || active.closed) return;
        active.closed = true;
        if (this.active === active) this.active = undefined;
        await client.close();
      },
    };
    active = { client, session, url: credentials.realtimeUrl, closed: false };
    this.active = active;
    try {
      await client.connect();
      if (!connectedStateObserved) input.onState("connected");
      return session;
    } catch (error) {
      active.closed = true;
      if (this.active === active) this.active = undefined;
      input.onState("failed");
      throw error;
    }
  }

  private async refresh(active: NonNullable<CentrifugoSessionAdapter["active"]>): Promise<void> {
    const credentials = await this.options.credentials.getCredentials();
    validateCredentials(credentials);
    if (credentials.realtimeUrl !== active.url) {
      throw new Error("realtime credential refresh changed the WSS endpoint");
    }
    await active.client.updateToken(credentials.token);
  }
}

function validateCredentials(credentials: RealtimeCredentials): void {
  if (!credentials.token.trim()) throw new Error("realtime credentials returned an empty token");
  const url = new URL(credentials.realtimeUrl);
  if (url.protocol !== "wss:" || url.username || url.password || url.hash) {
    throw new Error("realtime credentials require a safe wss URL");
  }
}
