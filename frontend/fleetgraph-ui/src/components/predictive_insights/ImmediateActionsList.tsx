import React from "react";


export type ImmediateActionRow = {
  action_id: string;
  action_title: string;
  action_priority: string;
};

type Props = {
  actions: ImmediateActionRow[];
};


export function ImmediateActionsList({ actions }: Props): JSX.Element {
  return (
    <section aria-label="Immediate Actions">
      <h3>Immediate Actions</h3>
      {actions.length === 0 ? (
        <p>No immediate actions available.</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Action Title</th>
              <th>Priority</th>
            </tr>
          </thead>
          <tbody>
            {actions.map((item) => (
              <tr key={item.action_id}>
                <td>{item.action_title}</td>
                <td>{item.action_priority}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}


export default ImmediateActionsList;