// SKILLS-SPEC §4.6 GUI — the composer's "/" force-run popup: opens only for a leading
// slash, lists only the session's effective (enabled) menu, filters while typing, and the
// picked skill rides onSend as its own field — never as message text.
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { Composer } from "./Composer";

const MENU = {
  skills: [
    { name: "weekly-report", description: "Monday status report", scope: "global", enabled: true },
    { name: "greet", description: "says hello", scope: "project", enabled: true },
    { name: "muted-one", description: "muted here", scope: "global", enabled: false },
  ],
};

function stubFetch() {
  const calls: { url: string; method: string }[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      calls.push({ url, method: (init?.method || "GET").toUpperCase() });
      if (url.includes("/skills")) return { ok: true, json: async () => MENU } as Response;
      return { ok: true, json: async () => ({}) } as Response;
    }),
  );
  return calls;
}

const props = (extra: Partial<Parameters<typeof Composer>[0]> = {}) => ({
  mode: "interactive",
  model: "gpt-5.6-sol",
  running: false,
  connected: true,
  sessionId: "s1",
  onSend: vi.fn(),
  onInterrupt: vi.fn(),
  onModeChange: vi.fn(),
  onModelChange: vi.fn(),
  ...extra,
});

const box = () => screen.getByPlaceholderText(/Ask the coworker/);

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("Composer / skills popup", () => {
  it("disables attachments and slash skills for Subscription Agent runtimes", async () => {
    const calls = stubFetch();
    const p = props({
      model: "codex-subscription:gpt-5.6-sol@max",
      prefill: {
        text: "inspect this",
        attachments: [{ kind: "text", name: "context.txt", text: "context" }],
        nonce: 1,
      },
    });
    render(<Composer {...p} />);

    expect(screen.getByTestId("subscription-runtime-limitations").textContent).toMatch(
      /attachments and OpenWorker slash skills are unavailable/i,
    );
    expect((screen.getByRole("button", { name: "Attach" }) as HTMLButtonElement).disabled).toBe(true);
    await waitFor(() => expect(screen.queryByText("context.txt")).toBeNull());

    fireEvent.change(box(), { target: { value: "/" } });
    expect(screen.queryByTestId("skill-popup")).toBeNull();
    expect(calls.some((call) => call.url.includes("/skills"))).toBe(false);

    fireEvent.change(box(), { target: { value: "plain request" } });
    fireEvent.keyDown(box(), { key: "Enter" });
    await waitFor(() => expect(p.onSend).toHaveBeenCalledWith("plain request", [], undefined));
  });

  it("shows the Kimi ACP security boundary only for the Kimi subscription runtime", () => {
    stubFetch();
    const p = props({ model: "kimi-code-subscription:kimi-code/k3@max" });
    const { rerender } = render(<Composer {...p} />);
    const warning = screen.getByTestId("kimi-subscription-warning");
    expect(warning.getAttribute("role")).toBe("alert");
    expect(warning.textContent).toMatch(/foreground personal use only/i);
    expect(warning.textContent).toMatch(/no protected system\/developer instruction layer/i);
    expect(warning.textContent).toMatch(/production orchestration is blocked/i);
    expect(warning.textContent).toMatch(/native shell is disabled/i);

    rerender(<Composer {...p} model="openai:gpt-5.6-sol" />);
    expect(screen.queryByTestId("kimi-subscription-warning")).toBeNull();
    expect(screen.queryByTestId("subscription-runtime-limitations")).toBeNull();
  });

  it("removes stale API attachment and forced-skill state when switching to a subscription runtime", async () => {
    stubFetch();
    const p = props({
      prefill: {
        text: "draft",
        attachments: [{ kind: "text", name: "api-context.txt", text: "context" }],
        nonce: 1,
      },
    });
    const { rerender } = render(<Composer {...p} />);
    expect(await screen.findByText("api-context.txt")).toBeTruthy();
    fireEvent.change(box(), { target: { value: "/gr" } });
    fireEvent.click(await screen.findByRole("option", { name: /greet/ }));
    fireEvent.change(box(), { target: { value: "/greet keep this request" } });

    rerender(<Composer {...p} model="claude-code-subscription:claude-opus-5@high" />);
    await waitFor(() => {
      expect(screen.queryByText("api-context.txt")).toBeNull();
      expect((box() as HTMLTextAreaElement).value).toBe("keep this request");
    });
    fireEvent.keyDown(box(), { key: "Enter" });
    await waitFor(() => expect(p.onSend).toHaveBeenCalledWith("keep this request", [], undefined));
  });

  it("opens on a leading '/' and lists only enabled skills from the effective menu", async () => {
    stubFetch();
    render(<Composer {...props()} />);
    fireEvent.change(box(), { target: { value: "/" } });
    await screen.findByTestId("skill-popup");
    expect(await screen.findByText("/weekly-report")).toBeTruthy();
    expect(screen.getByText("/greet")).toBeTruthy();
    expect(screen.queryByText("/muted-one")).toBeNull(); // muted → not offered
    expect(screen.getByText("project")).toBeTruthy(); // scope badge
  });

  it("filters as you type", async () => {
    stubFetch();
    render(<Composer {...props()} />);
    fireEvent.change(box(), { target: { value: "/" } });
    await screen.findByText("/weekly-report");
    fireEvent.change(box(), { target: { value: "/wee" } });
    expect(screen.getByText("/weekly-report")).toBeTruthy();
    expect(screen.queryByText("/greet")).toBeNull();
  });

  it("does NOT open for a mid-text slash", async () => {
    stubFetch();
    render(<Composer {...props()} />);
    fireEvent.change(box(), { target: { value: "rate 5/10 please" } });
    expect(screen.queryByTestId("skill-popup")).toBeNull();
  });

  it("selecting inserts /name inline; the send strips the prefix and carries the skill field", async () => {
    stubFetch();
    const p = props();
    render(<Composer {...p} />);
    fireEvent.change(box(), { target: { value: "/gr" } });
    fireEvent.click(await screen.findByRole("option", { name: /greet/ }));
    expect((box() as HTMLTextAreaElement).value).toBe("/greet "); // inline, no chip
    fireEvent.change(box(), { target: { value: "/greet say hi to the team" } });
    fireEvent.keyDown(box(), { key: "Enter" });
    await waitFor(() => expect(p.onSend).toHaveBeenCalled());
    expect(p.onSend).toHaveBeenCalledWith("say hi to the team", [], "greet");
  });

  it("a skill-only send works and Enter inside the popup never sends the query text", async () => {
    stubFetch();
    const p = props();
    render(<Composer {...p} />);
    fireEvent.change(box(), { target: { value: "/wee" } });
    await screen.findByText("/weekly-report");
    fireEvent.keyDown(box(), { key: "Enter" }); // selects, does not send
    expect(p.onSend).not.toHaveBeenCalled();
    expect((box() as HTMLTextAreaElement).value).toBe("/weekly-report ");
    fireEvent.keyDown(box(), { key: "Enter" }); // now sends, skill-only
    await waitFor(() => expect(p.onSend).toHaveBeenCalledWith("", [], "weekly-report"));
  });

  it("editing the /name prefix away un-picks the skill", async () => {
    stubFetch();
    const p = props();
    render(<Composer {...p} />);
    fireEvent.change(box(), { target: { value: "/gr" } });
    fireEvent.click(await screen.findByRole("option", { name: /greet/ }));
    fireEvent.change(box(), { target: { value: "hello plain" } }); // prefix gone
    fireEvent.keyDown(box(), { key: "Enter" });
    await waitFor(() => expect(p.onSend).toHaveBeenCalledWith("hello plain", [], undefined));
  });

  it("Escape closes the popup and no popup ever opens without a sessionId", async () => {
    stubFetch();
    render(<Composer {...props()} />);
    fireEvent.change(box(), { target: { value: "/gr" } });
    await screen.findByTestId("skill-popup");
    fireEvent.keyDown(box(), { key: "Escape" });
    expect(screen.queryByTestId("skill-popup")).toBeNull();
    cleanup();
    stubFetch();
    render(<Composer {...props({ sessionId: undefined })} />);
    fireEvent.change(box(), { target: { value: "/" } });
    expect(screen.queryByTestId("skill-popup")).toBeNull();
  });
});

describe("Composer — the doorway prefill (SKILLS-SPEC §5.2)", () => {
  it("a prefill arriving together with a session switch survives the draft clear", async () => {
    stubFetch();
    const { rerender } = render(<Composer {...props({ resetKey: "s1" })} />);
    // The doorway does both in one render: new session (resetKey) + prefill. The clear
    // effect must run BEFORE the prefill effect or the prefill is wiped (regression).
    rerender(
      <Composer
        {...props({
          resetKey: "s2",
          prefill: { text: "Build a new skill for me: release procedure", nonce: 1 },
        })}
      />,
    );
    await waitFor(() => {
      expect((box() as HTMLTextAreaElement).value).toBe(
        "Build a new skill for me: release procedure",
      );
    });
  });
});
