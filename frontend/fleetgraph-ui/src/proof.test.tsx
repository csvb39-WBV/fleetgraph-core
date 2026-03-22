import { expect, test } from "vitest";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";

function ProofComponent(): JSX.Element {
  return <div>FleetGraph test toolchain ready</div>;
}

test("proof component renders expected text", () => {
  const html = renderToStaticMarkup(<ProofComponent />);
  expect(html).toContain("FleetGraph test toolchain ready");
});

test("repeated render is deterministic", () => {
  const first = renderToStaticMarkup(<ProofComponent />);
  const second = renderToStaticMarkup(<ProofComponent />);
  expect(first).toBe(second);
});
