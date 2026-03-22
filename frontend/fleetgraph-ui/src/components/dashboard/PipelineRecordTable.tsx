import React from "react";


export type PipelineRecordRow = {
  record_id: string;
  company_name: string;
  opportunity_title: string;
  priority_score: number;
  stage: string;
  top_icp: string;
};

type Props = {
  records: PipelineRecordRow[];
};


export function PipelineRecordTable({ records }: Props): JSX.Element {
  return (
    <section aria-label="Pipeline Records">
      <h3>Pipeline Records</h3>
      <table>
        <thead>
          <tr>
            <th>Company Name</th>
            <th>Opportunity Title</th>
            <th>Priority Score</th>
            <th>Stage</th>
            <th>Top ICP</th>
          </tr>
        </thead>
        <tbody>
          {records.map((record) => (
            <tr key={record.record_id}>
              <td>{record.company_name}</td>
              <td>{record.opportunity_title}</td>
              <td>{record.priority_score}</td>
              <td>{record.stage}</td>
              <td>{record.top_icp}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  );
}


export default PipelineRecordTable;