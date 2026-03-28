import { expect, test } from "vitest";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";

import { AppShell } from "./app_shell";


function render(): string {
  return renderToStaticMarkup(<AppShell />);
}


test("shell renders successfully", () => {
  const html = render();
  expect(html.length).toBeGreaterThan(0);
});

test("header / title area is present", () => {
  const html = render();
  expect(html).toContain("FleetGraph Operator Console");
  expect(html).toContain("Watchlist Mode is the current operating priority.");
});

test("left navigation region is present", () => {
  const html = render();
  expect(html).toContain("Left navigation region");
});

test("main content region is present", () => {
  const html = render();
  expect(html).toContain("Main content region");
});

test("all required section placeholders are present", () => {
  const html = render();
  expect(html).toContain("Priority Dashboard");
  expect(html).toContain("Watchlist Console");
  expect(html).toContain("Company Intelligence");
  expect(html).toContain("Predictive Insights");
  expect(html).toContain("RFP Panel");
});

test("repeated render is deterministic", () => {
  const first = render();
  const second = render();
  const third = render();
  expect(first).toBe(second);
  expect(second).toBe(third);
});
