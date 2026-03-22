import React from "react";


type Props = {
  companyName: string;
  highestScore: number;
  prioritySummary: string;
};


export function CompanyIntelligenceSummary({
  companyName,
  highestScore,
  prioritySummary,
}: Props): JSX.Element {
  return (
    <section aria-label="Company Intelligence Summary">
      <h3>Summary</h3>
      <ul>
        <li>Company Name: {companyName}</li>
        <li>Highest Score: {highestScore}</li>
        <li>Priority Summary: {prioritySummary}</li>
      </ul>
    </section>
  );
}


export default CompanyIntelligenceSummary;
