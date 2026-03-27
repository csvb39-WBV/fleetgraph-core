import React from 'react';

import type { TodaySignal } from '../../services/signalApi';

type Props = {
  signal: TodaySignal | null;
};

function whyThisMatters(signal: TodaySignal | null): string {
  if (!signal) {
    return 'Select a signal to review its sales implications.';
  }
  if (signal.signal_type === 'litigation') {
    return 'Litigation can indicate active disruption, urgent vendor pain, and a short sales window.';
  }
  if (signal.signal_type === 'audit') {
    return 'Audit activity often signals compliance pressure, process scrutiny, and leadership attention.';
  }
  if (signal.signal_type === 'project_distress') {
    return 'Project distress can reveal schedule risk, document sprawl, and immediate coordination gaps.';
  }
  return 'Government activity can surface contract movement, oversight pressure, and timing-sensitive follow-up.';
}

function estimatedDocumentVolume(signal: TodaySignal | null): string {
  if (!signal) {
    return 'Unknown';
  }
  if (signal.priority === 'HIGH') {
    return 'Medium to High';
  }
  return 'Low to Medium';
}

function factLedgerFit(signal: TodaySignal | null): string {
  if (!signal) {
    return 'Awaiting signal selection';
  }
  if (signal.signal_type === 'audit' || signal.signal_type === 'litigation') {
    return 'Strong fit for document chronology, evidentiary tracing, and stakeholder review.';
  }
  return 'Strong fit for project-level relationship mapping and issue handoff tracking.';
}

export function SignalDetailPanel({ signal }: Props): JSX.Element {
  return (
    <aside
      aria-label="Signal Detail Panel"
      style={{
        border: '1px solid #cbd5e1',
        borderRadius: '18px',
        background: '#ffffff',
        padding: '20px',
        boxShadow: '0 12px 30px rgba(15, 23, 42, 0.08)',
      }}
    >
      <h3 style={{ marginTop: 0, marginBottom: '8px', fontSize: '22px' }}>{signal ? signal.company : 'Signal Detail'}</h3>
      <p style={{ marginTop: 0, color: '#475569' }}>{signal ? signal.event_summary : 'Choose a signal from the table to inspect the underlying lead.'}</p>
      <dl style={{ display: 'grid', gap: '12px', margin: 0 }}>
        <div>
          <dt style={{ fontSize: '12px', textTransform: 'uppercase', letterSpacing: '0.08em', color: '#64748b' }}>Raw Text</dt>
          <dd style={{ margin: '4px 0 0', color: '#0f172a' }}>{signal ? signal.raw_text : 'N/A'}</dd>
        </div>
        <div>
          <dt style={{ fontSize: '12px', textTransform: 'uppercase', letterSpacing: '0.08em', color: '#64748b' }}>Source Reference</dt>
          <dd style={{ margin: '4px 0 0', color: '#0f172a' }}>{signal ? signal.source : 'N/A'}</dd>
        </div>
        <div>
          <dt style={{ fontSize: '12px', textTransform: 'uppercase', letterSpacing: '0.08em', color: '#64748b' }}>Why This Matters</dt>
          <dd style={{ margin: '4px 0 0', color: '#0f172a' }}>{whyThisMatters(signal)}</dd>
        </div>
        <div>
          <dt style={{ fontSize: '12px', textTransform: 'uppercase', letterSpacing: '0.08em', color: '#64748b' }}>Estimated Document Volume</dt>
          <dd style={{ margin: '4px 0 0', color: '#0f172a' }}>{estimatedDocumentVolume(signal)}</dd>
        </div>
        <div>
          <dt style={{ fontSize: '12px', textTransform: 'uppercase', letterSpacing: '0.08em', color: '#64748b' }}>FactLedger Fit</dt>
          <dd style={{ margin: '4px 0 0', color: '#0f172a' }}>{factLedgerFit(signal)}</dd>
        </div>
        <div>
          <dt style={{ fontSize: '12px', textTransform: 'uppercase', letterSpacing: '0.08em', color: '#64748b' }}>Recommended Action</dt>
          <dd style={{ margin: '4px 0 0', color: '#0f766e', fontWeight: 700 }}>{signal ? signal.recommended_action : 'N/A'}</dd>
        </div>
      </dl>
    </aside>
  );
}

export default SignalDetailPanel;
