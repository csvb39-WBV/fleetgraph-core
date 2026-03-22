import React from "react";


export type HighPrioritySignalRow = {
  signal_id: string;
  signal_title: string;
  signal_priority: string;
};

type Props = {
  signals: HighPrioritySignalRow[];
};


export function HighPrioritySignalsList({ signals }: Props): JSX.Element {
  return (
    <section aria-label="High Priority Signals">
      <h3>High Priority Signals</h3>
      {signals.length === 0 ? (
        <p>No high priority signals available.</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Signal Title</th>
              <th>Priority</th>
            </tr>
          </thead>
          <tbody>
            {signals.map((item) => (
              <tr key={item.signal_id}>
                <td>{item.signal_title}</td>
                <td>{item.signal_priority}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}


export default HighPrioritySignalsList;