import React from "react";


export type PipelineRecordRow = {
  record_id: string;
  company_name: string;
  opportunity_title: string;
  priority_score: number;
  stage: string;
};

type Props = {
  records: PipelineRecordRow[];
};


export function RelatedPipelineRecords({ records }: Props): JSX.Element {
  return (
    <section aria-label="Related Pipeline Records">
      <h3>Related Pipeline Records</h3>
      <table>
        <thead>
          <tr>
            <th>Company Name</th>
            <th>Opportunity Title</th>
            <th>Priority Score</th>
            <th>Stage</th>
          </tr>
        </thead>
        <tbody>
          {records.map((item) => (
            <tr key={item.record_id}>
              <td>{item.company_name}</td>
              <td>{item.opportunity_title}</td>
              <td>{item.priority_score}</td>
              <td>{item.stage}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  );
}


export default RelatedPipelineRecords;
