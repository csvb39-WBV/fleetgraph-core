import React from "react";


export type PredictiveInsightsState = "loading" | "error" | "empty" | "success";

type Props = {
  state: PredictiveInsightsState;
};

const STATE_TEXT: Record<Exclude<PredictiveInsightsState, "success">, string> = {
  loading: "Loading predictive insights...",
  error: "Failed to load predictive insights.",
  empty: "No predictive insights available.",
};


export function PredictiveInsightsStatePanel({ state }: Props): JSX.Element | null {
  if (state === "success") {
    return null;
  }

  return <p>{STATE_TEXT[state]}</p>;
}


export default PredictiveInsightsStatePanel;