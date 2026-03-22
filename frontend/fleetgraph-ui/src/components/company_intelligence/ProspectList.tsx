import React from "react";


export type ProspectRow = {
  prospect_id: string;
  name: string;
  role: string;
  priority: string;
};

type Props = {
  prospects: ProspectRow[];
};


export function ProspectList({ prospects }: Props): JSX.Element {
  return (
    <section aria-label="Prospects">
      <h3>Prospects</h3>
      <table>
        <thead>
          <tr>
            <th>Name</th>
            <th>Role</th>
            <th>Priority</th>
          </tr>
        </thead>
        <tbody>
          {prospects.map((item) => (
            <tr key={item.prospect_id}>
              <td>{item.name}</td>
              <td>{item.role}</td>
              <td>{item.priority}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  );
}


export default ProspectList;
