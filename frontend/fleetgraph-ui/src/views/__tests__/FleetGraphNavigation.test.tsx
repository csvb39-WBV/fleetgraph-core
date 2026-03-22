import React from "react";
import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, expect, test, vi } from "vitest";

import { AppShell } from "../../app_shell";


const mockedViews = vi.hoisted(() => ({
  priorityDashboard: vi.fn(({ payload }: { payload: { company_id: string } }) => (
    <section aria-label="Priority Dashboard View">Priority Dashboard View::{payload.company_id}</section>
  )),
  companyIntelligence: vi.fn(() => (
    <section aria-label="Company Intelligence View">Company Intelligence View</section>
  )),
  predictiveInsights: vi.fn(() => (
    <section aria-label="Predictive Insights View">Predictive Insights View</section>
  )),
  rfpPanel: vi.fn(() => <section aria-label="RFP Panel View">RFP Panel View</section>),
}));


vi.mock("../../views/PriorityDashboardView", () => ({
  PriorityDashboardView: mockedViews.priorityDashboard,
}));

vi.mock("../../views/CompanyIntelligenceView", () => ({
  CompanyIntelligenceView: mockedViews.companyIntelligence,
}));

vi.mock("../../views/PredictiveInsightsView", () => ({
  PredictiveInsightsView: mockedViews.predictiveInsights,
}));

vi.mock("../../views/RfpPanelView", () => ({
  RfpPanelView: mockedViews.rfpPanel,
}));


async function renderShell(): Promise<{
  container: HTMLDivElement;
  root: ReturnType<typeof createRoot>;
}> {
  const container = document.createElement("div");
  document.body.appendChild(container);
  const root = createRoot(container);

  await act(async () => {
    root.render(<AppShell />);
  });

  return { container, root };
}

async function clickButton(container: HTMLDivElement, label: string): Promise<void> {
  const buttons = Array.from(container.querySelectorAll("button"));
  const target = buttons.find((button) => button.textContent === label);

  if (!target) {
    throw new Error(`Missing button: ${label}`);
  }

  await act(async () => {
    target.dispatchEvent(new MouseEvent("click", { bubbles: true }));
  });
}

function getRenderedViewMarkers(container: HTMLDivElement): string[] {
  return [
    "Priority Dashboard View",
    "Company Intelligence View",
    "Predictive Insights View",
    "RFP Panel View",
  ].filter((label) => container.innerHTML.includes(label));
}

afterEach(() => {
  mockedViews.priorityDashboard.mockClear();
  mockedViews.companyIntelligence.mockClear();
  mockedViews.predictiveInsights.mockClear();
  mockedViews.rfpPanel.mockClear();
  document.body.innerHTML = "";
});


test("default navigation renders priority dashboard workflow", async () => {
  const { container, root } = await renderShell();

  expect(container.innerHTML).toContain("FleetGraph Operator Console");
  expect(container.innerHTML).toContain("Left navigation region");
  expect(container.innerHTML).toContain("Priority Dashboard View::cmp-001");
  expect(container.innerHTML).not.toContain("Company Intelligence View");
  expect(container.innerHTML).not.toContain("Predictive Insights View");
  expect(container.innerHTML).not.toContain("RFP Panel View");
  expect(getRenderedViewMarkers(container)).toEqual(["Priority Dashboard View"]);

  expect(mockedViews.priorityDashboard).toHaveBeenCalledTimes(1);
  expect(mockedViews.priorityDashboard.mock.calls[0][0].payload).toEqual({ company_id: "cmp-001" });

  const activeButton = Array.from(container.querySelectorAll("button")).find(
    (button) => button.getAttribute("aria-pressed") === "true"
  );
  expect(activeButton?.textContent).toBe("Priority Dashboard");

  root.unmount();
});


test("navigation switches across all four views", async () => {
  const { container, root } = await renderShell();

  await clickButton(container, "Company Intelligence");
  expect(container.innerHTML).toContain("Company Intelligence View");
  expect(container.innerHTML).not.toContain("Priority Dashboard View::cmp-001");
  expect(container.innerHTML).not.toContain("Predictive Insights View");
  expect(container.innerHTML).not.toContain("RFP Panel View");
  expect(getRenderedViewMarkers(container)).toEqual(["Company Intelligence View"]);

  await clickButton(container, "Predictive Insights");
  expect(container.innerHTML).toContain("Predictive Insights View");
  expect(container.innerHTML).not.toContain("Company Intelligence View");

  await clickButton(container, "RFP Panel");
  expect(container.innerHTML).toContain("RFP Panel View");
  expect(container.innerHTML).not.toContain("Predictive Insights View");

  await clickButton(container, "Priority Dashboard");
  expect(container.innerHTML).toContain("Priority Dashboard View::cmp-001");
  expect(container.innerHTML).not.toContain("RFP Panel View");

  root.unmount();
});


test("navigation button pressed state tracks active view", async () => {
  const { container, root } = await renderShell();

  await clickButton(container, "Predictive Insights");

  const pressedStates = Array.from(container.querySelectorAll("button")).map((button) => ({
    label: button.textContent,
    pressed: button.getAttribute("aria-pressed"),
  }));

  expect(pressedStates).toEqual([
    { label: "Priority Dashboard", pressed: "false" },
    { label: "Company Intelligence", pressed: "false" },
    { label: "Predictive Insights", pressed: "true" },
    { label: "RFP Panel", pressed: "false" },
  ]);

  root.unmount();
});


test("repeated navigation sequence produces identical output", async () => {
  const first = await renderShell();

  await clickButton(first.container, "Company Intelligence");
  await clickButton(first.container, "RFP Panel");
  await clickButton(first.container, "Predictive Insights");
  const firstHtml = first.container.innerHTML;
  first.root.unmount();

  const second = await renderShell();
  await clickButton(second.container, "Company Intelligence");
  await clickButton(second.container, "RFP Panel");
  await clickButton(second.container, "Predictive Insights");

  expect(second.container.innerHTML).toBe(firstHtml);
  second.root.unmount();
});