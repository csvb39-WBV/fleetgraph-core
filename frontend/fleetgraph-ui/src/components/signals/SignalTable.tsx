import React from 'react';

import type { TodaySignal } from '../../services/signalApi';

type Props = {
  signals: TodaySignal[];
  selectedCompany: string | null;
  onSelectCompany: (company: string) => void;
};

function priorityChip(priority: 'HIGH' | 'MEDIUM'): JSX.Element {
  const styles =
    priority === 'HIGH'
      ? { background: '#fee2e2', color: '#991b1b' }
      : { background: '#fef3c7', color: '#92400e' };

  return (
    <span
      style={{
        ...styles,
        display: 'inline-flex',
        alignItems: 'center',
        justifyContent: 'center',
        minWidth: '72px',
        padding: '6px 10px',
        borderRadius: '999px',
        fontSize: '12px',
        fontWeight: 700,
        letterSpacing: '0.06em',
      }}
    >
      {priority}
    </span>
  );
}

export function SignalTable({ signals, selectedCompany, onSelectCompany }: Props): JSX.Element {
  return (
    <section aria-label="Signal Review Table">
      <table style={{ width: '100%', borderCollapse: 'collapse', background: '#ffffff', borderRadius: '16px', overflow: 'hidden' }}>
        <thead style={{ background: '#e2e8f0', color: '#0f172a' }}>
          <tr>
            <th style={{ padding: '14px', textAlign: 'left' }}>Company</th>
            <th style={{ padding: '14px', textAlign: 'left' }}>Signal Type</th>
            <th style={{ padding: '14px', textAlign: 'left' }}>Event Summary</th>
            <th style={{ padding: '14px', textAlign: 'left' }}>Date</th>
            <th style={{ padding: '14px', textAlign: 'left' }}>Priority</th>
            <th style={{ padding: '14px', textAlign: 'left' }}>Recommended Action</th>
          </tr>
        </thead>
        <tbody>
          {signals.map((signal) => {
            const isSelected = selectedCompany === signal.company;
            return (
              <tr
                key={`${signal.company}-${signal.signal_type}-${signal.date_detected}`}
                onClick={() => onSelectCompany(signal.company)}
                style={{
                  cursor: 'pointer',
                  background: isSelected ? '#eff6ff' : '#ffffff',
                  borderTop: '1px solid #e2e8f0',
                }}
              >
                <td style={{ padding: '14px', fontWeight: 700 }}>{signal.company}</td>
                <td style={{ padding: '14px', textTransform: 'capitalize' }}>{signal.signal_type.replace('_', ' ')}</td>
                <td style={{ padding: '14px', color: '#334155' }}>{signal.event_summary}</td>
                <td style={{ padding: '14px' }}>{signal.date_detected}</td>
                <td style={{ padding: '14px' }}>{priorityChip(signal.priority)}</td>
                <td style={{ padding: '14px', fontWeight: 700, color: '#0f766e' }}>{signal.recommended_action}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </section>
  );
}

export default SignalTable;
