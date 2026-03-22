import { beforeEach, expect, test } from "vitest";
import {
  type ApiAdapter,
  type ApiPayload,
  type ApiResponse,
  getCompanyIntelligence,
  getPredictiveInsights,
  getPriorityDashboard,
  getRfpPanel,
  setAdapter,
} from "./api_client";


const STUB_RESPONSE: ApiResponse = { company_id: "cmp-001", status: "ok" };

function makeStub(
  out: ApiResponse = STUB_RESPONSE
): { adapter: ApiAdapter; calls: Array<{ endpoint: string; payload: ApiPayload }> } {
  const calls: Array<{ endpoint: string; payload: ApiPayload }> = [];
  const adapter: ApiAdapter = (endpoint, payload) => {
    calls.push({ endpoint, payload });
    return Promise.resolve(out);
  };
  return { adapter, calls };
}

beforeEach(() => {
  setAdapter(makeStub().adapter);
});


test("all four client functions are exported and callable", () => {
  expect(typeof getPriorityDashboard).toBe("function");
  expect(typeof getCompanyIntelligence).toBe("function");
  expect(typeof getPredictiveInsights).toBe("function");
  expect(typeof getRfpPanel).toBe("function");
});


test("each function delegates to the shared adapter with the correct endpoint", async () => {
  const cases: Array<[string, (p: ApiPayload) => Promise<ApiResponse>]> = [
    ["/api/priority-dashboard", getPriorityDashboard],
    ["/api/company-intelligence", getCompanyIntelligence],
    ["/api/predictive-insights", getPredictiveInsights],
    ["/api/rfp-panel", getRfpPanel],
  ];

  const payload: ApiPayload = { company_id: "cmp-001" };

  for (const [expectedEndpoint, fn] of cases) {
    const { adapter, calls } = makeStub();
    setAdapter(adapter);
    await fn(payload);
    expect(calls).toHaveLength(1);
    expect(calls[0].endpoint).toBe(expectedEndpoint);
    expect(calls[0].payload).toEqual(payload);
  }
});


test("each function forwards the payload unchanged to the adapter", async () => {
  const payload: ApiPayload = {
    company_id: "cmp-test",
    extra: "field",
    nested: { key: 1 },
  };

  const fns: Array<(p: ApiPayload) => Promise<ApiResponse>> = [
    getPriorityDashboard,
    getCompanyIntelligence,
    getPredictiveInsights,
    getRfpPanel,
  ];

  for (const fn of fns) {
    const { adapter, calls } = makeStub();
    setAdapter(adapter);
    await fn(payload);
    expect(calls[0].payload).toEqual(payload);
  }
});


test("repeated identical calls produce identical results", async () => {
  const { adapter } = makeStub();
  setAdapter(adapter);

  const payload: ApiPayload = { company_id: "cmp-replay" };
  const [a, b, c] = await Promise.all([
    getPriorityDashboard(payload),
    getPriorityDashboard(payload),
    getPriorityDashboard(payload),
  ]);
  expect(a).toEqual(b);
  expect(b).toEqual(c);
});


test("stable failure behavior: adapter error propagates consistently across calls", async () => {
  setAdapter(() => Promise.reject(new Error("upstream failure")));

  const messages: string[] = [];
  for (let i = 0; i < 3; i++) {
    try {
      await getPriorityDashboard({ company_id: "cmp-001" });
    } catch (err) {
      messages.push((err as Error).message);
    }
  }

  expect(messages).toEqual([
    "upstream failure",
    "upstream failure",
    "upstream failure",
  ]);
});


test("no missing exports: setAdapter and all four functions are present in the module", async () => {
  const mod = await import("./api_client");
  const required = [
    "setAdapter",
    "getPriorityDashboard",
    "getCompanyIntelligence",
    "getPredictiveInsights",
    "getRfpPanel",
  ];
  for (const name of required) {
    expect(typeof (mod as Record<string, unknown>)[name]).toBe("function");
  }
});
