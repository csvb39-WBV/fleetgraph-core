import React from "react";
import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import type { PriorityDashboardResponse } from "../../api/types";
import { PriorityDashboardView } from "../PriorityDashboardView";


vi.mock("../../api/client", () => ({
  getPriorityDashboard: vi.fn(),
}));

import { getPriorityDashboard } from "../../api/client";


const mockGetPriorityDashboard = vi.mocked(getPriorityDashboard);

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

async function renderAndGetHtml(payload: Record<string, unknown>): Promise<{
  container: HTMLDivElement;
  root: ReturnType<typeof createRoot>;
}> {
  const container = document.createElement("div");
  document.body.appendChild(container);
  const root = createRoot(container);

  await act(async () => {
    root.render(<PriorityDashboardView payload={payload} />);
  });

  return { container, root };
}

beforeEach(() => {
  mockGetPriorityDashboard.mockReset();
});

afterEach(() => {
  document.body.innerHTML = "";
});


test("loading state renders", async () => {
  const gate = deferred<PriorityDashboardResponse>();
  mockGetPriorityDashboard.mockReturnValue(gate.promise);

  const { container, root } = await renderAndGetHtml({ company_id: "cmp-001" });

  expect(container.innerHTML).toContain("Loading priority dashboard...");

  await act(async () => {
    gate.resolve({
      company_id: "cmp-001",
      dashboard_summary: {
        total_records: 0,
        high_priority_count: 0,
        medium_priority_count: 0,
        low_priority_count: 0,
      },
      records: [],
    });
    await flush();
  });

  root.unmount();
});


test("error state renders on client failure", async () => {
  mockGetPriorityDashboard.mockRejectedValue(new Error("boom"));

  const { container, root } = await renderAndGetHtml({ company_id: "cmp-001" });

  await act(async () => {
    await flush();
  });

  expect(container.innerHTML).toContain("Failed to load priority dashboard.");
  root.unmount();
});


test("empty state renders for zero records", async () => {
  mockGetPriorityDashboard.mockResolvedValue({
    company_id: "cmp-001",
    dashboard_summary: {
      total_records: 0,
      high_priority_count: 0,
      medium_priority_count: 0,
      low_priority_count: 0,
    },
    records: [],
  });

  const { container, root } = await renderAndGetHtml({ company_id: "cmp-001" });

  await act(async () => {
    await flush();
  });

  const html = container.innerHTML;
  expect(html).toContain("No pipeline records available.");
  expect(html).toContain("Total Pipeline Records: 0");
  expect(html).toContain("High Priority Records: 0");
  expect(html).toContain("Active Companies: 0");
  root.unmount();
});


test("summary cards render exact required labels and values", async () => {
  mockGetPriorityDashboard.mockResolvedValue({
    company_id: "cmp-001",
    dashboard_summary: {
      total_records: 3,
      high_priority_count: 2,
      medium_priority_count: 1,
      low_priority_count: 0,
    },
    records: [
      {
        record_id: "r1",
        company_name: "Acme",
        opportunity_title: "Fleet Refresh",
        priority_score: 90,
        stage: "READY",
        top_icp: "DLR",
      },
      {
        record_id: "r2",
        company_name: "Bravo",
        opportunity_title: "Service Expansion",
        priority_score: 81,
        stage: "READY",
        top_icp: "OEM",
      },
      {
        record_id: "r3",
        company_name: "Acme",
        opportunity_title: "Maintenance Pilot",
        priority_score: 50,
        stage: "READY",
        top_icp: "UPFITTER",
      },
    ],
  });

  const { container, root } = await renderAndGetHtml({ company_id: "cmp-001" });

  await act(async () => {
    await flush();
  });

  const html = container.innerHTML;
  expect(html).toContain("Total Pipeline Records: 3");
  expect(html).toContain("High Priority Records: 2");
  expect(html).toContain("Active Companies: 2");

  root.unmount();
});


test("summary uses API returned counts for total and high-priority", async () => {
  mockGetPriorityDashboard.mockResolvedValue({
    company_id: "cmp-001",
    dashboard_summary: {
      total_records: 9,
      high_priority_count: 4,
      medium_priority_count: 2,
      low_priority_count: 3,
    },
    records: [
      {
        record_id: "r1",
        company_name: "Acme",
        opportunity_title: "Fleet Refresh",
        priority_score: 95,
        stage: "READY",
        top_icp: "DLR",
      },
      {
        record_id: "r2",
        company_name: "Bravo",
        opportunity_title: "Expansion",
        priority_score: 20,
        stage: "READY",
        top_icp: "OEM",
      },
    ],
  });

  const { container, root } = await renderAndGetHtml({ company_id: "cmp-001" });

  await act(async () => {
    await flush();
  });

  const html = container.innerHTML;
  expect(html).toContain("Total Pipeline Records: 9");
  expect(html).toContain("High Priority Records: 4");
  root.unmount();
});


test("pipeline table renders exact required fields", async () => {
  mockGetPriorityDashboard.mockResolvedValue({
    company_id: "cmp-001",
    dashboard_summary: {
      total_records: 1,
      high_priority_count: 1,
      medium_priority_count: 0,
      low_priority_count: 0,
    },
    records: [
      {
        record_id: "r1",
        company_name: "Acme",
        opportunity_title: "Fleet Refresh",
        priority_score: 90,
        stage: "READY",
        top_icp: "DLR",
      },
    ],
  });

  const { container, root } = await renderAndGetHtml({ company_id: "cmp-001" });

  await act(async () => {
    await flush();
  });

  const html = container.innerHTML;
  expect(html).toContain("Company Name");
  expect(html).toContain("Opportunity Title");
  expect(html).toContain("Priority Score");
  expect(html).toContain("Stage");
  expect(html).toContain("Top ICP");

  expect(html).toContain("Acme");
  expect(html).toContain("Fleet Refresh");
  expect(html).toContain("90");
  expect(html).toContain("READY");
  expect(html).toContain("DLR");

  root.unmount();
});


test("deterministic order holds and API client is mocked at frontend boundary", async () => {
  mockGetPriorityDashboard.mockResolvedValue({
    company_id: "cmp-001",
    dashboard_summary: {
      total_records: 4,
      high_priority_count: 2,
      medium_priority_count: 1,
      low_priority_count: 1,
    },
    records: [
      {
        record_id: "r2",
        company_name: "BetaCo",
        opportunity_title: "B2",
        priority_score: 90,
        stage: "READY",
        top_icp: "OEM",
      },
      {
        record_id: "r1",
        company_name: "AlphaCo",
        opportunity_title: "A1",
        priority_score: 90,
        stage: "READY",
        top_icp: "DLR",
      },
      {
        record_id: "r3",
        company_name: "AlphaCo",
        opportunity_title: "A2",
        priority_score: 90,
        stage: "READY",
        top_icp: "UPFITTER",
      },
      {
        record_id: "r4",
        company_name: "GammaCo",
        opportunity_title: "G1",
        priority_score: 70,
        stage: "READY",
        top_icp: "LEASING",
      },
    ],
  });

  const payload = { company_id: "cmp-001" };
  const firstRender = await renderAndGetHtml(payload);

  await act(async () => {
    await flush();
  });

  expect(mockGetPriorityDashboard).toHaveBeenCalledTimes(1);
  expect(mockGetPriorityDashboard).toHaveBeenCalledWith(payload);

  const firstHtml = firstRender.container.innerHTML;
  const iA1 = firstHtml.indexOf("A1");
  const iA2 = firstHtml.indexOf("A2");
  const iB2 = firstHtml.indexOf("B2");
  const iG1 = firstHtml.indexOf("G1");

  expect(iA1).toBeGreaterThan(-1);
  expect(iA2).toBeGreaterThan(-1);
  expect(iB2).toBeGreaterThan(-1);
  expect(iG1).toBeGreaterThan(-1);

  expect(iA1).toBeLessThan(iA2);
  expect(iA2).toBeLessThan(iB2);
  expect(iB2).toBeLessThan(iG1);

  firstRender.root.unmount();

  mockGetPriorityDashboard.mockClear();
  mockGetPriorityDashboard.mockResolvedValue({
    company_id: "cmp-001",
    dashboard_summary: {
      total_records: 4,
      high_priority_count: 2,
      medium_priority_count: 1,
      low_priority_count: 1,
    },
    records: [
      {
        record_id: "r2",
        company_name: "BetaCo",
        opportunity_title: "B2",
        priority_score: 90,
        stage: "READY",
        top_icp: "OEM",
      },
      {
        record_id: "r1",
        company_name: "AlphaCo",
        opportunity_title: "A1",
        priority_score: 90,
        stage: "READY",
        top_icp: "DLR",
      },
      {
        record_id: "r3",
        company_name: "AlphaCo",
        opportunity_title: "A2",
        priority_score: 90,
        stage: "READY",
        top_icp: "UPFITTER",
      },
      {
        record_id: "r4",
        company_name: "GammaCo",
        opportunity_title: "G1",
        priority_score: 70,
        stage: "READY",
        top_icp: "LEASING",
      },
    ],
  });

  const secondRender = await renderAndGetHtml(payload);
  await act(async () => {
    await flush();
  });

  expect(secondRender.container.innerHTML).toBe(firstHtml);
  secondRender.root.unmount();
});