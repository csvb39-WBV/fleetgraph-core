import React from "react";


type Props = {
  icpName: string;
  fitScore: number;
  rationale: string;
};


export function NextBestIcpPanel({ icpName, fitScore, rationale }: Props): JSX.Element {
  return (
    <section aria-label="Next Best ICP Panel">
      <h3>Next Best ICP</h3>
      <ul>
        <li>ICP Name: {icpName}</li>
        <li>Fit Score: {fitScore}</li>
        <li>Rationale: {rationale}</li>
      </ul>
    </section>
  );
}


export default NextBestIcpPanel;