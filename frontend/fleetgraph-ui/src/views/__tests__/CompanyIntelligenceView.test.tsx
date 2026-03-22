import React from "react";
import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { CompanyIntelligenceView } from "../CompanyIntelligenceView";


vi.mock("../../api/client", () => ({
  getCompanyIntelligence: vi.fn(),
}));

import { getCompanyIntelligence } from "../../api/client";


const mockGetCompanyIntelligence = vi.mocked(getCompanyIntelligence);

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
    root.render(<CompanyIntelligenceView payload={payload} />);
  });

  return { container, root };
}

beforeEach(() => {
  mockGetCompanyIntelligence.mockReset();
});

afterEach(() => {
  document.body.innerHTML = "";
});


test("loading state renders", async () => {
  const gate = deferred<Record<string, unknown>>();
  mockGetCompanyIntelligence.mockReturnValue(gate.promise);

  const { container, root } = await renderView(PAYLOAD);

  expect(container.innerHTML).toContain("Company Intelligence");
  expect(container.innerHTML).toContain("Loading company intelligence...");

  await act(async () => {
    gate.resolve({
      company_id: "cmp-001",
      company_intelligence: { highest_opportunity_score: 0, highest_priority: "NONE" },
      opportunities: [],
      prospects: [],
      pipeline_records: [],
    });
    await flush();
  });

  root.unmount();
});


test("error state renders", async () => {
  mockGetCompanyIntelligence.mockRejectedValue(new Error("boom"));

  const { container, root } = await renderView(PAYLOAD);

  await act(async () => {
    await flush();
  });

  expect(container.innerHTML).toContain("Failed to load company intelligence.");
  root.unmount();
});


test("empty state renders with summary", async () => {
  mockGetCompanyIntelligence.mockResolvedValue({
    company_id: "cmp-001",
    company_name: "Empty Corp",
    company_intelligence: {
      highest_opportunity_score: 0,
      highest_priority: "NONE",
    },
    opportunities: [],
    prospects: [],
    pipeline_records: [],
  });

  const { container, root } = await renderView(PAYLOAD);

  await act(async () => {
    await flush();
  });

  const html = container.innerHTML;
  expect(html).toContain("No company intelligence available.");
  expect(html).toContain("Company Name");
  expect(html).toContain("Empty Corp");
  expect(html).toContain("Highest Score");
  expect(html).toContain("Priority Summary");

  root.unmount();
});


test("summary renders exact labels", async () => {
  mockGetCompanyIntelligence.mockResolvedValue({
    company_id: "cmp-001",
    company_name: "Acme Corp",
    company_intelligence: {
      highest_opportunity_score: 87,
      highest_priority: "HIGH",
    },
    opportunities: [
      { icp: "OEM", opportunity_score: 87, reason: "Expansion", stage: "QUALIFIED" },
    ],
    prospects: [],
    pipeline_records: [],
  });

  const { container, root } = await renderView(PAYLOAD);

  await act(async () => {
    await flush();
  });

  const html = container.innerHTML;
  expect(html).toContain("Company Name");
  expect(html).toContain("Acme Corp");
  expect(html).toContain("Highest Score");
  expect(html).toContain("87");
  expect(html).toContain("Priority Summary");
  expect(html).toContain("HIGH");

  root.unmount();
});


test("opportunities render exact column headers", async () => {
  mockGetCompanyIntelligence.mockResolvedValue({
    company_id: "cmp-001",
    company_intelligence: {
      highest_opportunity_score: 75,
      highest_priority: "MEDIUM",
    },
    opportunities: [
      {
        icp: "OEM",
        opportunity_score: 75,
        reason: "Fleet Expansion",
        stage: "QUALIFIED",
      },
    ],
    prospects: [],
    pipeline_records: [],
  });

  const { container, root } = await renderView(PAYLOAD);

  await act(async () => {
    await flush();
  });

  const html = container.innerHTML;
  expect(html).toContain("Title");
  expect(html).toContain("Score");
  expect(html).toContain("Stage");
  expect(html).toContain("Fleet Expansion");
  expect(html).toContain("75");
  expect(html).toContain("QUALIFIED");

  root.unmount();
});


test("prospects render exact column headers", async () => {
  mockGetCompanyIntelligence.mockResolvedValue({
    company_id: "cmp-001",
    company_intelligence: {
      highest_opportunity_score: 80,
      highest_priority: "HIGH",
    },
    opportunities: [],
    prospects: [
      { company_id: "cmp-002", icp: "DLR", priority: "HIGH" },
    ],
    pipeline_records: [],
  });

  const { container, root } = await renderView(PAYLOAD);

  await act(async () => {
    await flush();
  });

  const html = container.innerHTML;
  expect(html).toContain("Name");
  expect(html).toContain("Role");
  expect(html).toContain("Priority");
  expect(html).toContain("cmp-002");
  expect(html).toContain("DLR");
  expect(html).toContain("HIGH");

  root.unmount();
});


test("related pipeline records render exact column headers", async () => {
  mockGetCompanyIntelligence.mockResolvedValue({
    company_id: "cmp-001",
    company_intelligence: {
      highest_opportunity_score: 88,
      highest_priority: "HIGH",
    },
    opportunities: [],
    prospects: [],
    pipeline_records: [
      {
        company_id: "cmp-003",
        icp: "OEM",
        opportunity_score: 88,
        reason: "Growth",
        stage: "READY",
      },
    ],
  });

  const { container, root } = await renderView(PAYLOAD);

  await act(async () => {
    await flush();
  });

  const html = container.innerHTML;
  expect(html).toContain("Company Name");
  expect(html).toContain("Opportunity Title");
  expect(html).toContain("Priority Score");
  expect(html).toContain("Stage");
  expect(html).toContain("cmp-003");
  expect(html).toContain("Growth");
  expect(html).toContain("88");
  expect(html).toContain("READY");

  root.unmount();
});


test("deterministic sorting is stable across all three sections", async () => {
  const response = {
    company_id: "cmp-001",
    company_intelligence: {
      highest_opportunity_score: 90,
      highest_priority: "HIGH",
    },
    opportunities: [
      { reason: "ZETA_OPP", opportunity_score: 70 },
      { reason: "ALPHA_OPP", opportunity_score: 90 },
      { reason: "BETA_OPP", opportunity_score: 90 },
    ],
    prospects: [
      { company_id: "p-beta", icp: "B", priority: "MEDIUM" },
      { company_id: "p-alpha", icp: "A", priority: "HIGH" },
      { company_id: "p-gamma", icp: "G", priority: "HIGH" },
    ],
    pipeline_records: [
      { company_id: "pr-beta", icp: "B", opportunity_score: 90, reason: "x" },
      { company_id: "pr-alpha", icp: "A", opportunity_score: 90, reason: "a" },
      { company_id: "pr-omega", icp: "O", opportunity_score: 20, reason: "o" },
    ],
  };

  mockGetCompanyIntelligence.mockResolvedValue(response);
  const first = await renderView(PAYLOAD);

  await act(async () => {
    await flush();
  });

  const firstHtml = first.container.innerHTML;

  // Opportunities: score desc → title asc → opportunity_id asc
  expect(firstHtml.indexOf("ALPHA_OPP")).toBeLessThan(firstHtml.indexOf("BETA_OPP"));
  expect(firstHtml.indexOf("BETA_OPP")).toBeLessThan(firstHtml.indexOf("ZETA_OPP"));

  // Prospects: priority rank (HIGH < MEDIUM) → name asc → prospect_id asc
  expect(firstHtml.indexOf("p-alpha")).toBeLessThan(firstHtml.indexOf("p-gamma"));
  expect(firstHtml.indexOf("p-gamma")).toBeLessThan(firstHtml.indexOf("p-beta"));

  // Pipeline records: priority_score desc → company_name asc → record_id asc
  expect(firstHtml.indexOf("pr-alpha")).toBeLessThan(firstHtml.indexOf("pr-beta"));
  expect(firstHtml.indexOf("pr-beta")).toBeLessThan(firstHtml.indexOf("pr-omega"));

  first.root.unmount();

  mockGetCompanyIntelligence.mockClear();
  mockGetCompanyIntelligence.mockResolvedValue(response);
  const second = await renderView(PAYLOAD);

  await act(async () => {
    await flush();
  });

  expect(second.container.innerHTML).toBe(firstHtml);

  second.root.unmount();
});


test("api boundary is called with provided payload", async () => {
  mockGetCompanyIntelligence.mockResolvedValue({
    company_id: "cmp-001",
    company_intelligence: {
      highest_opportunity_score: 0,
      highest_priority: "NONE",
    },
    opportunities: [],
    prospects: [],
    pipeline_records: [],
  });

  const payload = { company_id: "cmp-001", include: "all" };
  const { root } = await renderView(payload);

  await act(async () => {
    await flush();
  });

  expect(mockGetCompanyIntelligence).toHaveBeenCalledTimes(1);
  expect(mockGetCompanyIntelligence).toHaveBeenCalledWith(payload);

  root.unmount();
});

