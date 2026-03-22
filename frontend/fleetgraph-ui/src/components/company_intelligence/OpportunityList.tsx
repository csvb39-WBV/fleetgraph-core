import React from "react";


export type OpportunityRow = {
  opportunity_id: string;
  title: string;
  score: number;
  stage: string;
};

type Props = {
  opportunities: OpportunityRow[];
};


export function OpportunityList({ opportunities }: Props): JSX.Element {
  return (
    <section aria-label="Opportunities">
      <h3>Opportunities</h3>
      <table>
        <thead>
          <tr>
            <th>Title</th>
            <th>Score</th>
            <th>Stage</th>
          </tr>
        </thead>
        <tbody>
          {opportunities.map((item) => (
            <tr key={item.opportunity_id}>
              <td>{item.title}</td>
              <td>{item.score}</td>
              <td>{item.stage}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  );
}


export default OpportunityList;
