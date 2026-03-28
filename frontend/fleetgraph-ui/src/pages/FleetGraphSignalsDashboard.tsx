import React, { useEffect, useMemo, useState } from 'react';

import SignalDetailPanel from '../components/signals/SignalDetailPanel';
import SignalDisclosure from '../components/signals/SignalDisclosure';
import SignalSummaryCards from '../components/signals/SignalSummaryCards';
import SignalTable from '../components/signals/SignalTable';
import { getTodaySignals, type TodaySignal, type TodaySignalsPayload } from '../services/signalApi';

function topFiveSignals(signals: TodaySignal[]): TodaySignal[] {
  return signals.slice(0, 5);
}

function sourceLabel(source: string): 'News' | 'Legal' | 'Web' {
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

function qualityBadge(signal: TodaySignal): 'HIGH CONFIDENCE' | 'MEDIUM CONFIDENCE' {
  const label = sourceLabel(signal.source);
  if (signal.confidence_score >= 5 || (signal.confidence_score >= 4 && label === 'News')) {
    return 'HIGH CONFIDENCE';
  }
  return 'MEDIUM CONFIDENCE';
}

export function FleetGraphSignalsDashboard(): JSX.Element {
  const [payload, setPayload] = useState<TodaySignalsPayload | null>(null);
  const [errorMessage, setErrorMessage] = useState('');
  const [selectedCompany, setSelectedCompany] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    getTodaySignals()
      .then((response) => {
        if (cancelled) {
          return;
        }
        setPayload(response);
        setSelectedCompany(response.top_signals.length > 0 ? response.top_signals[0].company : null);
        setErrorMessage('');
      })
      .catch((error) => {
        if (cancelled) {
          return;
        }
        setErrorMessage(error instanceof Error ? error.message : 'Failed to load today signals');
      });

    return () => {
      cancelled = true;
    };
  }, []);

  const selectedSignal = useMemo(() => {
    if (!payload || !selectedCompany) {
      return null;
    }
    return payload.top_signals.find((signal) => signal.company === selectedCompany) ?? payload.top_signals[0] ?? null;
  }, [payload, selectedCompany]);

  if (errorMessage) {
    return <main><p>{errorMessage}</p></main>;
  }

  if (!payload) {
    return <main><p>Loading today signals...</p></main>;
  }

  const heroSignals = topFiveSignals(payload.top_signals);

  return (
    <main
      style={{
        minHeight: '100vh',
        padding: '28px',
        background: 'linear-gradient(180deg, #f8fafc 0%, #e2e8f0 100%)',
        color: '#0f172a',
      }}
    >
      <div style={{ maxWidth: '1280px', margin: '0 auto', display: 'grid', gap: '18px' }}>
        <header
          style={{
            borderRadius: '24px',
            padding: '24px',
            background: 'linear-gradient(135deg, #0f172a 0%, #1e293b 60%, #334155 100%)',
            color: '#f8fafc',
            boxShadow: '0 18px 40px rgba(15, 23, 42, 0.22)',
          }}
        >
          <div style={{ display: 'flex', justifyContent: 'space-between', gap: '16px', flexWrap: 'wrap' }}>
            <div>
              <div style={{ fontSize: '13px', letterSpacing: '0.12em', textTransform: 'uppercase', color: '#93c5fd' }}>FleetGraph Daily Signals</div>
              <h1 style={{ margin: '8px 0 0', fontSize: '34px' }}>Top Signals Ready for Operator Review</h1>
              <p style={{ margin: '10px 0 0', maxWidth: '760px', color: '#cbd5e1' }}>
                Today&apos;s highest-confidence public-source litigation, compliance, government, and project-risk signals, formatted for rapid review and direct action.
              </p>
            </div>
            <div style={{ minWidth: '220px', display: 'grid', gap: '8px' }}>
              <div style={{ fontSize: '13px', color: '#cbd5e1' }}>Run Date: {payload.run_date}</div>
              <div style={{ fontSize: '13px', color: '#cbd5e1' }}>Run Status: {payload.status}</div>
              <div style={{ fontSize: '13px', color: '#cbd5e1' }}>Exported: {payload.exported_count}</div>
              <div style={{ fontSize: '13px', color: '#cbd5e1' }}>Retained: {payload.retained_count}</div>
            </div>
          </div>
          <div style={{ marginTop: '18px', display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '12px' }}>
            {heroSignals.map((signal) => (
              <article
                key={`${signal.company}-${signal.signal_type}`}
                style={{
                  borderRadius: '18px',
                  background: signal.priority === 'HIGH' ? 'rgba(248, 113, 113, 0.16)' : 'rgba(251, 191, 36, 0.16)',
                  border: signal.priority === 'HIGH' ? '1px solid rgba(248, 113, 113, 0.45)' : '1px solid rgba(251, 191, 36, 0.45)',
                  padding: '14px',
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', gap: '8px', alignItems: 'center' }}>
                  <div style={{ fontSize: '11px', letterSpacing: '0.1em', textTransform: 'uppercase', color: '#e2e8f0' }}>{signal.priority}</div>
                  <div style={{ fontSize: '11px', letterSpacing: '0.08em', textTransform: 'uppercase', color: '#bfdbfe' }}>{qualityBadge(signal)}</div>
                </div>
                <div style={{ marginTop: '6px', fontWeight: 700 }}>{signal.company}</div>
                <div style={{ marginTop: '4px', color: '#cbd5e1', fontSize: '14px' }}>{signal.event_summary}</div>
                <div style={{ marginTop: '8px', fontSize: '12px', color: '#93c5fd' }}>Source: {sourceLabel(signal.source)}</div>
              </article>
            ))}
          </div>
        </header>

        <SignalDisclosure />
        <SignalSummaryCards summary={payload.summary} retainedCount={payload.retained_count} />

        <section style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 2fr) minmax(320px, 1fr)', gap: '18px', alignItems: 'start' }}>
          <SignalTable signals={payload.top_signals} selectedCompany={selectedCompany} onSelectCompany={setSelectedCompany} />
          <SignalDetailPanel signal={selectedSignal} />
        </section>
      </div>
    </main>
  );
}

export default FleetGraphSignalsDashboard;
