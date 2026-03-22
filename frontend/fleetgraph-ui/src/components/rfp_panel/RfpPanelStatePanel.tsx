import React from "react";


export type RfpPanelState = "loading" | "error" | "empty" | "success";

type Props = {
  state: RfpPanelState;
};

const STATE_TEXT: Record<Exclude<RfpPanelState, "success">, string> = {
  loading: "Loading RFP panel...",
  error: "Failed to load RFP panel.",
  empty: "No RFP panel data available.",
};


export function RfpPanelStatePanel({ state }: Props): JSX.Element | null {
  if (state === "success") {
    return null;
  }

  return <p>{STATE_TEXT[state]}</p>;
}


export default RfpPanelStatePanel;