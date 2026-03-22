import React from "react";
import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, beforeEach, expect, test, vi } from "vitest";

import { RfpPanelView } from "../RfpPanelView";


vi.mock("../../api/client", () => ({
  getRfpPanel: vi.fn(),
}));

import { getRfpPanel } from "../../api/client";


const mockGetRfpPanel = vi.mocked(getRfpPanel);
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
    root.render(<RfpPanelView payload={payload} />);
  });

  return { container, root };
}

beforeEach(() => {
  mockGetRfpPanel.mockReset();
});

afterEach(() => {
  document.body.innerHTML = "";
});


test("loading state renders", async () => {
  const gate = deferred<Record<string, unknown>>();
  mockGetRfpPanel.mockReturnValue(gate.promise);

  const { container, root } = await renderView(PAYLOAD);

  expect(container.innerHTML).toContain("RFP Panel");
  expect(container.innerHTML).toContain("Loading RFP panel...");

  await act(async () => {
    gate.resolve({
      company_id: "cmp-001",
      rfp_summary: {
        total_signals: 0,
        high_priority_signals: 0,
        panel_status: "N/A",
      },
      top_icp: {
        icp_name: "N/A",
        fit_score: 0,
        rationale: "N/A",
      },
      high_priority_signals: [],
    });
    await flush();
  });

  root.unmount();
});


test("error state renders", async () => {
  mockGetRfpPanel.mockRejectedValue(new Error("boom"));

  const { container, root } = await renderView(PAYLOAD);

  await act(async () => {
    await flush();
  });

  expect(container.innerHTML).toContain("Failed to load RFP panel.");
  root.unmount();
});


test("empty state renders summary and empty message", async () => {
  mockGetRfpPanel.mockResolvedValue({
    company_id: "cmp-001",
    rfp_summary: {
      total_signals: 0,
      high_priority_signals: 0,
      panel_status: "N/A",
    },
    high_priority_signals: [],
  });

  const { container, root } = await renderView(PAYLOAD);

  await act(async () => {
    await flush();
  });

  const html = container.innerHTML;
  expect(html).toContain("No RFP panel data available.");
  expect(html).toContain("Total Signals: 0");
  expect(html).toContain("High Priority Signals: 0");
  expect(html).toContain("Panel Status: N/A");

  root.unmount();
});


test("summary renders exact labels and values", async () => {
  mockGetRfpPanel.mockResolvedValue({
    company_id: "cmp-001",
    rfp_summary: {
      total_signals: 3,
      high_priority_signals: 2,
      panel_status: "ACTIVE",
    },
    top_icp: {
      icp_name: "OEM",
      fit_score: 95,
      rationale: "Expansion fit",
    },
    high_priority_signals: [{ signal_title: "Call fleet lead", signal_priority: "HIGH" }],
  });

  const { container, root } = await renderView(PAYLOAD);

  await act(async () => {
    await flush();
  });

  const html = container.innerHTML;
  expect(html).toContain("Total Signals");
  expect(html).toContain("High Priority Signals");
  expect(html).toContain("Panel Status");
  expect(html).toContain("Total Signals: 3");
  expect(html).toContain("High Priority Signals: 2");
  expect(html).toContain("Panel Status: ACTIVE");

  root.unmount();
});


test("top icp renders exact labels", async () => {
  mockGetRfpPanel.mockResolvedValue({
    company_id: "cmp-001",
    rfp_summary: {
      total_signals: 1,
      high_priority_signals: 1,
      panel_status: "ACTIVE",
    },
    top_icp: {
      name: "DLR",
      score: 91,
      reason: "Service coverage",
    },
    high_priority_signals: [{ title: "Call dealer", priority: "HIGH" }],
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


test("high priority signals render exact labels", async () => {
  mockGetRfpPanel.mockResolvedValue({
    company_id: "cmp-001",
    rfp_summary: {
      total_signals: 2,
      high_priority_signals: 2,
      panel_status: "ACTIVE",
    },
    top_icp: {
      icp_name: "OEM",
      fit_score: 95,
      rationale: "Expansion fit",
    },
    high_priority_signals: [
      { signal_title: "Call fleet lead", signal_priority: "HIGH" },
      { title: "Send deck", priority: "MEDIUM" },
    ],
  });

  const { container, root } = await renderView(PAYLOAD);

  await act(async () => {
    await flush();
  });

  const html = container.innerHTML;
  expect(html).toContain("Signal Title");
  expect(html).toContain("Priority");
  expect(html).toContain("Call fleet lead");
  expect(html).toContain("Send deck");

  root.unmount();
});


test("deterministic signal sorting and repeated render are stable", async () => {
  const response = {
    company_id: "cmp-001",
    rfp_summary: {
      total_signals: 4,
      high_priority_signals: 4,
      panel_status: "ACTIVE",
    },
    best_icp: "OEM",
    high_priority_signals: [
      { signal_id: "s2", signal_title: "Beta signal", signal_priority: "HIGH" },
      { signal_id: "s1", signal_title: "Alpha signal", signal_priority: "HIGH" },
      { signal_id: "s3", signal_title: "Gamma signal", signal_priority: "MEDIUM" },
      { signal_id: "s4", signal_title: "Omega signal", signal_priority: "UNKNOWN" },
    ],
  };

  mockGetRfpPanel.mockResolvedValue(response);
  const first = await renderView(PAYLOAD);

  await act(async () => {
    await flush();
  });

  const firstHtml = first.container.innerHTML;
  expect(mockGetRfpPanel).toHaveBeenCalledTimes(1);
  expect(mockGetRfpPanel).toHaveBeenCalledWith(PAYLOAD);
  expect(firstHtml).toContain("ICP Name: OEM");
  expect(firstHtml.indexOf("Alpha signal")).toBeLessThan(firstHtml.indexOf("Beta signal"));
  expect(firstHtml.indexOf("Beta signal")).toBeLessThan(firstHtml.indexOf("Gamma signal"));
  expect(firstHtml.indexOf("Gamma signal")).toBeLessThan(firstHtml.indexOf("Omega signal"));

  first.root.unmount();

  mockGetRfpPanel.mockClear();
  mockGetRfpPanel.mockResolvedValue(response);
  const second = await renderView(PAYLOAD);

  await act(async () => {
    await flush();
  });

  expect(second.container.innerHTML).toBe(firstHtml);
  second.root.unmount();
});


test("api client is mocked at frontend boundary only", async () => {
  mockGetRfpPanel.mockResolvedValue({
    company_id: "cmp-001",
    rfp_summary: {
      total_signals: 2,
      high_priority_signals: 1,
      panel_status: "ACTIVE",
    },
    top_icp: {
      icp_name: "OEM",
      fit_score: 75,
      rationale: "Coverage",
    },
    high_priority_signals: [],
  });

  const payload = { company_id: "cmp-001", include: "all" };
  const { root } = await renderView(payload);

  await act(async () => {
    await flush();
  });

  expect(mockGetRfpPanel).toHaveBeenCalledTimes(1);
  expect(mockGetRfpPanel).toHaveBeenCalledWith(payload);

  root.unmount();
});