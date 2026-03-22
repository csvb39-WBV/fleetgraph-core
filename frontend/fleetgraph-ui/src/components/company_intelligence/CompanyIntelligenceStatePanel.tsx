import React from "react";


export type CompanyIntelligenceState = "loading" | "error" | "empty" | "success";

type Props = {
  state: CompanyIntelligenceState;
};

const STATE_TEXT: Record<Exclude<CompanyIntelligenceState, "success">, string> = {
  loading: "Loading company intelligence...",
  error: "Failed to load company intelligence.",
  empty: "No company intelligence available.",
};


export function CompanyIntelligenceStatePanel({ state }: Props): JSX.Element | null {
  if (state === "success") {
    return null;
  }

  return <p>{STATE_TEXT[state]}</p>;
}


export default CompanyIntelligenceStatePanel;
