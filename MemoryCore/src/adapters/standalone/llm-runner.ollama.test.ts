/**
 * Native Ollama transport (`/api/chat`) for StandaloneLLMRunner.
 *
 * Why this needs its own coverage: the whole point of the native endpoint is
 * that it honours `think: false`, which Ollama's OpenAI-compatible
 * `/v1/chat/completions` silently drops. If that flag ever stops reaching
 * the wire, Qwen3 reasoning eats the entire output budget and L1 extraction
 * silently returns zero memories — exactly the failure this transport
 * exists to prevent.
 */

import { describe, it, expect, vi, afterEach } from "vitest";
import { StandaloneLLMRunner } from "./llm-runner.js";

const originalFetch = globalThis.fetch;

afterEach(() => {
  globalThis.fetch = originalFetch;
  vi.restoreAllMocks();
});

interface FetchInitLike {
  body?: string;
}

function mockFetch(handler: (body: Record<string, unknown>, url: string) => unknown) {
  const spy = vi.fn(async (input: unknown, init: FetchInitLike) => {
    const url = String(input);
    const body = JSON.parse(init.body ?? "{}") as Record<string, unknown>;
    const payload = handler(body, url);
    return {
      ok: true,
      status: 200,
      json: async () => payload,
      text: async () => JSON.stringify(payload),
    } as unknown as Response;
  });
  globalThis.fetch = spy as unknown as typeof fetch;
  return spy;
}

/** Read the JSON body of the Nth fetch call. */
function bodyOf(spy: ReturnType<typeof mockFetch>, index: number): Record<string, unknown> {
  const call = spy.mock.calls[index];
  const init = call?.[1] as FetchInitLike | undefined;
  return JSON.parse(init?.body ?? "{}") as Record<string, unknown>;
}

/** Read the URL of the Nth fetch call. */
function urlOf(spy: ReturnType<typeof mockFetch>, index: number): string {
  return String(spy.mock.calls[index]?.[0]);
}

const baseConfig = {
  baseUrl: "http://127.0.0.1:11434/v1",
  apiKey: "",
  model: "qwen3.5:9b-mlx",
  maxTokens: 4096,
  timeoutMs: 30_000,
};

describe("StandaloneLLMRunner — native Ollama path", () => {
  it("posts to /api/chat and forwards think=false", async () => {
    const spy = mockFetch(() => ({
      message: { content: '[{"scenes":[{"memories":[]}]}]' },
      done: true,
      eval_count: 42,
    }));

    const runner = new StandaloneLLMRunner({
      config: { ...baseConfig, provider: "ollama", ollama: { think: false } },
    });

    const out = await runner.run({
      prompt: "user text",
      systemPrompt: "system text",
      taskId: "l1-extraction",
    });

    expect(out).toContain("scenes");
    expect(spy).toHaveBeenCalledTimes(1);
    // The native endpoint lives at the Ollama root, not under /v1.
    expect(urlOf(spy, 0)).toBe("http://127.0.0.1:11434/api/chat");
    const body = bodyOf(spy, 0);
    expect(body.think).toBe(false);
    expect(body.stream).toBe(false);
    expect(body.model).toBe("qwen3.5:9b-mlx");
    expect(body.messages).toEqual([
      { role: "system", content: "system text" },
      { role: "user", content: "user text" },
    ]);
    expect(body.options.num_predict).toBe(4096);
  });

  it("per-call provider override selects the native path", async () => {
    const spy = mockFetch(() => ({
      message: { content: "ok" },
      done: true,
      eval_count: 1,
    }));

    const runner = new StandaloneLLMRunner({ config: baseConfig });
    await runner.run({
      prompt: "p",
      taskId: "l1-extraction",
      provider: "ollama",
      ollama: { think: false, numCtx: 32768 },
    });

    expect(urlOf(spy, 0)).toBe("http://127.0.0.1:11434/api/chat");
    const body = bodyOf(spy, 0);
    expect(body.think).toBe(false);
    expect((body.options as Record<string, unknown>).num_ctx).toBe(32768);
  });

  it("per-call ollama options override the runner-level defaults", async () => {
    const spy = mockFetch(() => ({
      message: { content: "ok" },
      done: true,
      eval_count: 1,
    }));

    const runner = new StandaloneLLMRunner({
      config: { ...baseConfig, provider: "ollama", ollama: { think: true, numCtx: 8192 } },
    });
    await runner.run({
      prompt: "p",
      taskId: "l1-extraction",
      ollama: { think: false },
    });

    const body = bodyOf(spy, 0);
    expect(body.think).toBe(false);
    // numCtx is not overridden per-call, so the runner-level value survives.
    expect((body.options as Record<string, unknown>).num_ctx).toBe(8192);
  });

  it("runs a tool loop: model asks for a tool, gets the result, then answers", async () => {
    let round = 0;
    const spy = mockFetch((body) => {
      round += 1;
      if (round === 1) {
        return {
          message: {
            content: "",
            tool_calls: [
              {
                id: "call_1",
                function: { name: "read_file", arguments: { path: "a.txt" } },
              },
            ],
          },
          done: true,
          eval_count: 10,
        };
      }
      // Second round: the tool result must be in the message history.
      expect(JSON.stringify(body.messages)).toContain("tool-content");
      return { message: { content: "final answer" }, done: true, eval_count: 5 };
    });

    const runner = new StandaloneLLMRunner({
      config: { ...baseConfig, provider: "ollama" },
    });

    const out = await runner.run({
      prompt: "p",
      taskId: "l2-scene",
      enableTools: true,
      tools: {
        read_file: {
          description: "Read a file",
          inputSchema: {
            type: "object",
            properties: { path: { type: "string" } },
            required: ["path"],
          },
          execute: async () => "tool-content",
        },
      },
    });

    expect(out).toBe("final answer");
    expect(spy).toHaveBeenCalledTimes(2);

    // First request must advertise the tool in Ollama's own format.
    const firstBody = bodyOf(spy, 0);
    const wire = firstBody.tools as Array<{ function: { name: string; parameters: unknown } }>;
    expect(wire).toHaveLength(1);
    expect(wire[0].function.name).toBe("read_file");
    expect(wire[0].function.parameters).toMatchObject({ type: "object" });

    // Second request must carry the assistant turn and the tool result.
    const secondBody = bodyOf(spy, 1);
    const msgs = secondBody.messages as Array<Record<string, unknown>>;
    const toolMsg = msgs.find((m) => m.role === "tool");
    expect(toolMsg?.content).toBe("tool-content");
    const assistantMsg = msgs.find((m) => m.role === "assistant");
    expect(assistantMsg?.tool_calls).toBeDefined();
  });

  it("passes a malformed tool-arguments string as an empty object", async () => {
    let round = 0;
    mockFetch(() => {
      round += 1;
      if (round === 1) {
        return {
          message: {
            content: "",
            tool_calls: [
              { id: "c1", function: { name: "read_file", arguments: "{not json" } },
            ],
          },
          done: true,
        };
      }
      return { message: { content: "ok" }, done: true };
    });

    const runner = new StandaloneLLMRunner({
      config: { ...baseConfig, provider: "ollama" },
    });

    const out = await runner.run({
      prompt: "p",
      taskId: "l2-scene",
      enableTools: true,
      tools: {
        read_file: {
          description: "Read a file",
          inputSchema: { type: "object", properties: {} },
          execute: async () => "recovered",
        },
      },
    });

    expect(out).toBe("ok");
  });

  it("surfaces a non-200 response as an error", async () => {
    globalThis.fetch = vi.fn(async () => ({
      ok: false,
      status: 500,
      json: async () => ({}),
      text: async () => "model not found",
    })) as unknown as typeof fetch;

    const runner = new StandaloneLLMRunner({
      config: { ...baseConfig, provider: "ollama" },
    });

    await expect(
      runner.run({ prompt: "p", taskId: "l1-extraction" }),
    ).rejects.toThrow(/500/);
  });
});
