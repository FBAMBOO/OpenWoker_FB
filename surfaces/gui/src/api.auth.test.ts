import { afterEach, expect, it, vi } from "vitest";
import { apiRequest, ApiRequestError, downloadApiResource, getHealth, Session } from "./api";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

it("authenticates REST and session WebSocket calls with the launch token", async () => {
  vi.stubGlobal("__COWORKER_API_TOKEN__", "launch-token");
  const request = vi.fn(async (_url: string, init?: RequestInit) => {
    expect(new Headers(init?.headers).get("X-OpenWorker-Token")).toBe("launch-token");
    return { json: async () => ({ status: "ok" }) } as Response;
  });
  vi.stubGlobal("fetch", request);

  class FakeWebSocket {
    static readonly CONNECTING = 0;
    static readonly OPEN = 1;
    readyState = FakeWebSocket.CONNECTING;
    onmessage: ((event: MessageEvent) => void) | null = null;
    onopen: (() => void) | null = null;
    onclose: (() => void) | null = null;
    send = vi.fn();

    constructor(
      public readonly url: string,
      public readonly protocols?: string | string[],
    ) {}
  }
  vi.stubGlobal("WebSocket", FakeWebSocket);

  await getHealth();
  expect(request).toHaveBeenCalledOnce();

  const session = new Session("s1", "/workspace", "code", { onEvent: vi.fn() });
  const socket = (session as unknown as { ws: FakeWebSocket }).ws;
  expect(socket.protocols).toEqual(["openworker", "launch-token"]);
});

it("downloads API resources with authentication and an object URL", async () => {
  vi.stubGlobal("__COWORKER_API_TOKEN__", "launch-token");
  const request = vi.fn(async (_url: string, init?: RequestInit) => {
    expect(new Headers(init?.headers).get("X-OpenWorker-Token")).toBe("launch-token");
    return { ok: true, blob: async () => new Blob(["evidence"]) } as Response;
  });
  vi.stubGlobal("fetch", request);
  const createObjectURL = vi.fn(() => "blob:download-test");
  const revokeObjectURL = vi.fn();
  vi.stubGlobal("URL", { createObjectURL, revokeObjectURL });
  const click = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => undefined);

  await downloadApiResource("/v1/orchestration/blobs/digest", "release/report.txt");
  await new Promise((resolve) => window.setTimeout(resolve, 0));

  expect(request).toHaveBeenCalledOnce();
  expect(createObjectURL).toHaveBeenCalledOnce();
  expect(click).toHaveBeenCalledOnce();
  expect(revokeObjectURL).toHaveBeenCalledWith("blob:download-test");
});

it("treats a 503 orchestration readiness snapshot as reachable health data", async () => {
  const snapshot = {
    status: "not_ready",
    default_workspace: null,
    model: "openai:test",
    orchestration: {
      ready: false,
      state: "unhealthy",
      loop_alive: true,
      leader: { held: false, heartbeat_alive: false },
      outbox: { loop_alive: true, pending: 2, dead_letters: 1, stale: true },
    },
  };
  vi.stubGlobal("fetch", vi.fn(async () => ({
    ok: false,
    status: 503,
    json: async () => snapshot,
  }) as Response));

  await expect(getHealth()).resolves.toEqual(snapshot);
});

it("surfaces the canonical orchestration error message without stringifying the envelope", async () => {
  const payload = {
    error: {
      code: "ETAG_MISMATCH",
      message: "The contract changed; reload its current version.",
      retryable: true,
      details: {},
      correlation_id: "corr-1",
    },
  };
  vi.stubGlobal("fetch", vi.fn(async () => ({
    ok: false,
    status: 409,
    text: async () => JSON.stringify(payload),
  })));

  const caught = await apiRequest("/v1/orchestration/task-drafts/task-1/contract").catch((error) => error);
  expect(caught).toBeInstanceOf(ApiRequestError);
  expect((caught as ApiRequestError).message).toBe("The contract changed; reload its current version.");
  expect((caught as ApiRequestError).payload).toEqual(payload);
});
