import React from "react";


export type DashboardState = "loading" | "error" | "empty" | "success";

type Props = {
  state: DashboardState;
};

const STATE_TEXT: Record<Exclude<DashboardState, "success">, string> = {
  loading: "Loading priority dashboard...",
  error: "Failed to load priority dashboard.",
  empty: "No pipeline records available.",
};


export function DashboardStatePanel({ state }: Props): JSX.Element | null {
  if (state === "success") {
    return null;
  }

  return <p>{STATE_TEXT[state]}</p>;
}


export default DashboardStatePanel;