import React from "react";


type Props = {
  totalPipelineRecords: number;
  highPriorityRecords: number;
  activeCompanies: number;
};


export function PrioritySummaryCards({
  totalPipelineRecords,
  highPriorityRecords,
  activeCompanies,
}: Props): JSX.Element {
  return (
    <section aria-label="Priority Summary Cards">
      <h3>Summary</h3>
      <ul>
        <li>Total Pipeline Records: {totalPipelineRecords}</li>
        <li>High Priority Records: {highPriorityRecords}</li>
        <li>Active Companies: {activeCompanies}</li>
      </ul>
    </section>
  );
}


export default PrioritySummaryCards;