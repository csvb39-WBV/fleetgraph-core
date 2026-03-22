import React from "react";


type Props = {
  totalSignals: number;
  highPrioritySignals: number;
  panelStatus: string;
};


export function RfpPanelSummary({
  totalSignals,
  highPrioritySignals,
  panelStatus,
}: Props): JSX.Element {
  return (
    <section aria-label="RFP Panel Summary">
      <h3>Summary</h3>
      <ul>
        <li>Total Signals: {totalSignals}</li>
        <li>High Priority Signals: {highPrioritySignals}</li>
        <li>Panel Status: {panelStatus}</li>
      </ul>
    </section>
  );
}


export default RfpPanelSummary;