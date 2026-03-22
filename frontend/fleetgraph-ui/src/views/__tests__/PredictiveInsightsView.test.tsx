import React from "react";
import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { PredictiveInsightsView } from "../PredictiveInsightsView";


vi.mock("../../api/client", () => ({
  getPredictiveInsights: vi.fn(),
}));

import { getPredictiveInsights } from "../../api/client";


const mockGetPredictiveInsights = vi.mocked(getPredictiveInsights);
const PAYLOAD = { company_id: "cmp-001" };

function deferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (error: unknown) => void;
} {
  let resolveFn!: (value: T) => void;
  let rejectFn!: (error: unknown) => void;
  const promise = new Promise<T>((resolve, reject) => {
    resolveFn = resolve;
    rejectFn = reject;
  });
  return { promise, resolve: resolveFn, reject: rejectFn };
}

async function flush(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

async function renderView(payload: Record<string, unknown>): Promise<{
  container: HTMLDivElement;
  root: ReturnType<typeof createRoot>;
}> {
  const container = document.createElement("div");
  document.body.appendChild(container);
  const root = createRoot(container);

  await act(async () => {
    root.render(<PredictiveInsightsView payload={payload} />);
  });

  return { container, root };
}

beforeEach(() => {
  mockGetPredictiveInsights.mockReset();
});

afterEach(() => {
  document.body.innerHTML = "";
});


test("loading state renders", async () => {
  const gate = deferred<Record<string, unknown>>();
  mockGetPredictiveInsights.mockReturnValue(gate.promise);

  const { container, root } = await renderView(PAYLOAD);

  expect(container.innerHTML).toContain("Predictive Insights");
  expect(container.innerHTML).toContain("Loading predictive insights...");

  await act(async () => {
    gate.resolve({
      company_id: "cmp-001",
      predictive_summary: {
        predicted_demand_score: 0,
        confidence_level: "N/A",
        forecast_window: "N/A",
      },
      next_best_icp: {
        icp_name: "N/A",
        fit_score: 0,
        rationale: "N/A",
      },
      immediate_actions: [],
    });
    await flush();
  });

  root.unmount();
});


test("error state renders", async () => {
  mockGetPredictiveInsights.mockRejectedValue(new Error("boom"));

  const { container, root } = await renderView(PAYLOAD);

  await act(async () => {
    await flush();
  });

  expect(container.innerHTML).toContain("Failed to load predictive insights.");
  root.unmount();
});


test("empty state renders summary and empty message", async () => {
  mockGetPredictiveInsights.mockResolvedValue({
    company_id: "cmp-001",
    predictive_summary: {
      predicted_demand_score: 0,
      confidence_level: "N/A",
      forecast_window: "N/A",
    },
    immediate_actions: [],
  });

  const { container, root } = await renderView(PAYLOAD);

  await act(async () => {
    await flush();
  });

  const html = container.innerHTML;
  expect(html).toContain("No predictive insights available.");
  expect(html).toContain("Predicted Demand Score: 0");
  expect(html).toContain("Confidence Level: N/A");
  expect(html).toContain("Forecast Window: N/A");
  root.unmount();
});


test("summary renders exact required labels and values", async () => {
  mockGetPredictiveInsights.mockResolvedValue({
    company_id: "cmp-001",
    predictive_summary: {
      predicted_demand_score: 95,
      confidence_level: "HIGH",
      forecast_window: "30_DAYS",
    },
    next_best_icp: {
      icp_name: "OEM",
      fit_score: 95,
      rationale: "Expansion fit",
    },
    immediate_actions: [{ action_title: "Call fleet lead", action_priority: "HIGH" }],
  });

  const { container, root } = await renderView(PAYLOAD);

  await act(async () => {
    await flush();
  });

  const html = container.innerHTML;
  expect(html).toContain("Predicted Demand Score");
  expect(html).toContain("Confidence Level");
  expect(html).toContain("Forecast Window");
  expect(html).toContain("Predicted Demand Score: 95");
  expect(html).toContain("Confidence Level: HIGH");
  expect(html).toContain("Forecast Window: 30_DAYS");
  root.unmount();
});


test("next best icp renders exact labels", async () => {
  mockGetPredictiveInsights.mockResolvedValue({
    company_id: "cmp-001",
    predictive_summary: {
      predicted_demand_score: 91,
      confidence_level: "MEDIUM",
      forecast_window: "7_DAYS",
    },
    next_best_icp: {
      name: "DLR",
      score: 91,
      reason: "Service coverage",
    },
    immediate_actions: [{ title: "Call dealer", priority: "HIGH" }],
  });

  const { container, root } = await renderView(PAYLOAD);

  await act(async () => {
    await flush();
  });

  const html = container.innerHTML;
  expect(html).toContain("ICP Name");
  expect(html).toContain("Fit Score");
  expect(html).toContain("Rationale");
  expect(html).toContain("DLR");
  expect(html).toContain("91");
  expect(html).toContain("Service coverage");
  root.unmount();
});


test("immediate actions render exact labels", async () => {
  mockGetPredictiveInsights.mockResolvedValue({
    company_id: "cmp-001",
    predictive_summary: {
      predicted_demand_score: 95,
      confidence_level: "HIGH",
      forecast_window: "30_DAYS",
    },
    next_best_icp: {
      icp_name: "OEM",
      fit_score: 95,
      rationale: "Expansion fit",
    },
    immediate_actions: [
      { action_title: "Call fleet lead", action_priority: "HIGH" },
      { title: "Send deck", priority: "MEDIUM" },
    ],
  });

  const { container, root } = await renderView(PAYLOAD);

  await act(async () => {
    await flush();
  });

  const html = container.innerHTML;
  expect(html).toContain("Immediate Actions");
  expect(html).toContain("Action Title");
  expect(html).toContain("Priority");
  expect(html).toContain("Call fleet lead");
  expect(html).toContain("Send deck");
  root.unmount();
});


test("deterministic action sorting and repeated render are stable", async () => {
  const response = {
    company_id: "cmp-001",
    predictive_summary: {
      predicted_demand_score: 90,
      confidence_level: "HIGH",
      forecast_window: "30_DAYS",
    },
    best_icp: "ALPHA",
    immediate_actions: [
      { action_id: "a-2", action_title: "Beta outreach", action_priority: "HIGH" },
      { action_id: "a-1", action_title: "Alpha outreach", action_priority: "HIGH" },
      { action_id: "a-3", action_title: "Gamma email", action_priority: "MEDIUM" },
      { action_id: "a-4", action_title: "Omega nurture", action_priority: "UNKNOWN" },
    ],
  };

  mockGetPredictiveInsights.mockResolvedValue(response);
  const first = await renderView(PAYLOAD);

  await act(async () => {
    await flush();
  });

  const firstHtml = first.container.innerHTML;
  expect(mockGetPredictiveInsights).toHaveBeenCalledTimes(1);
  expect(mockGetPredictiveInsights).toHaveBeenCalledWith(PAYLOAD);
  expect(firstHtml).toContain("ICP Name: ALPHA");
  expect(firstHtml.indexOf("Alpha outreach")).toBeLessThan(firstHtml.indexOf("Beta outreach"));
  expect(firstHtml.indexOf("Beta outreach")).toBeLessThan(firstHtml.indexOf("Gamma email"));
  expect(firstHtml.indexOf("Gamma email")).toBeLessThan(firstHtml.indexOf("Omega nurture"));

  first.root.unmount();

  mockGetPredictiveInsights.mockClear();
  mockGetPredictiveInsights.mockResolvedValue(response);
  const second = await renderView(PAYLOAD);

  await act(async () => {
    await flush();
  });

  expect(second.container.innerHTML).toBe(firstHtml);
  second.root.unmount();
});


test("api client is mocked at frontend boundary only", async () => {
  mockGetPredictiveInsights.mockResolvedValue({
    company_id: "cmp-001",
    predictive_summary: {
      predicted_demand_score: 75,
      confidence_level: "MEDIUM",
      forecast_window: "7_DAYS",
    },
    next_best_icp: {
      icp_name: "OEM",
      fit_score: 75,
      rationale: "Coverage",
    },
    immediate_actions: [],
  });

  const payload = { company_id: "cmp-001", include: "all" };
  const { root } = await renderView(payload);

  await act(async () => {
    await flush();
  });

  expect(mockGetPredictiveInsights).toHaveBeenCalledTimes(1);
  expect(mockGetPredictiveInsights).toHaveBeenCalledWith(payload);

  root.unmount();
});