import React from 'react';

import type { TodaySignal } from '../../services/signalApi';

type Props = {
  signal: TodaySignal | null;
};

function combinedText(signal: TodaySignal | null): string {
  if (!signal) {
    return '';
  }
  return `${signal.signal_type} ${signal.event_summary} ${signal.raw_text}`.toLowerCase();
}

function sourceLabel(source: string | null): 'News' | 'Legal' | 'Web' {
  if (!source) {
    return 'Web';
  }
  const normalizedSource = source.toLowerCase();
  if (normalizedSource.includes('rss') || normalizedSource.includes('news')) {
    return 'News';
  }
  if (
    normalizedSource.includes('court')
    || normalizedSource.includes('law')
    || normalizedSource.includes('legal')
    || normalizedSource.includes('counsel')
    || normalizedSource.includes('docket')
  ) {
    return 'Legal';
  }
  return 'Web';
}

function qualityBadge(signal: TodaySignal | null): 'HIGH CONFIDENCE' | 'MEDIUM CONFIDENCE' {
  if (!signal) {
    return 'MEDIUM CONFIDENCE';
  }
  const label = sourceLabel(signal.source);
  if (signal.confidence_score >= 5 || (signal.confidence_score >= 4 && label === 'News')) {
    return 'HIGH CONFIDENCE';
  }
  return 'MEDIUM CONFIDENCE';
}

function whyThisMatters(signal: TodaySignal | null): string {
  if (!signal) {
    return 'Select a signal to review its operational implications.';
  }

  const text = combinedText(signal);

  if (text.includes('subpoena') || text.includes('document production') || text.includes('ediscovery')) {
    return 'Subpoena and document-production activity can trigger urgent preservation, collection, and review workloads.';
  }
  if (text.includes('investigation') || text.includes('forensic review') || text.includes('regulatory inquiry')) {
    return 'Investigations often create fast-moving document review demands, privilege handling, and cross-team coordination pressure.';
  }
  if (signal.signal_type === 'litigation') {
    return 'Litigation can indicate active legal pressure, stakeholder escalation, and immediate evidence-management needs.';
  }
  if (signal.signal_type === 'audit') {
    return 'Audit activity often signals compliance pressure, process scrutiny, and leadership attention.';
  }
  if (signal.signal_type === 'project_distress') {
    return 'Project distress can reveal schedule risk, coordination gaps, and rising documentation burdens across counterparties.';
  }
  return 'Government activity can surface oversight pressure, contract movement, and timing-sensitive follow-up.';
}

function estimatedDocumentVolume(signal: TodaySignal | null): string {
  if (!signal) {
    return 'Unknown';
  }

  const text = combinedText(signal);
  if (text.includes('subpoena') || text.includes('document production') || text.includes('ediscovery') || text.includes('forensic review')) {
    return 'High';
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

  const text = combinedText(signal);
  if (text.includes('subpoena') || text.includes('document production') || text.includes('ediscovery')) {
    return 'Strong fit for collection tracking, review coordination, privilege-sensitive chronology, and production readiness.';
  }
  if (text.includes('investigation') || text.includes('forensic review') || text.includes('regulatory inquiry')) {
    return 'Strong fit for internal review workflows, evidence tracking, and cross-functional issue management.';
  }
  if (signal.signal_type === 'audit' || signal.signal_type === 'litigation') {
    return 'Strong fit for document chronology, evidentiary tracing, and stakeholder review.';
  }
  return 'Strong fit for relationship mapping, issue escalation tracking, and operational handoff visibility.';
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
          <dt style={{ fontSize: '12px', textTransform: 'uppercase', letterSpacing: '0.08em', color: '#64748b' }}>Signal Quality</dt>
          <dd style={{ margin: '4px 0 0', color: '#0f172a', fontWeight: 700 }}>{qualityBadge(signal)}</dd>
        </div>
        <div>
          <dt style={{ fontSize: '12px', textTransform: 'uppercase', letterSpacing: '0.08em', color: '#64748b' }}>Source Type</dt>
          <dd style={{ margin: '4px 0 0', color: '#0f172a' }}>{signal ? sourceLabel(signal.source) : 'N/A'}</dd>
        </div>
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
