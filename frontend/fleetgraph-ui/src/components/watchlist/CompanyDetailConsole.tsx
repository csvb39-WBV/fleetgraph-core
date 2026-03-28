import React from 'react';

import type { WatchlistCompanyRecord, WatchlistRefreshStatus } from '../../services/watchlistApi';
import ContactsPanel from './ContactsPanel';
import KeyPeoplePanel from './KeyPeoplePanel';
import ProjectsLocationsPanel from './ProjectsLocationsPanel';
import RecentSignalsPanel from './RecentSignalsPanel';
import SourceConfidencePanel from './SourceConfidencePanel';

type Props = {
  company: WatchlistCompanyRecord | null;
  refreshStatus: WatchlistRefreshStatus;
  refreshErrorMessage: string;
  onRefresh: () => void;
};

function enrichmentSummary(company: WatchlistCompanyRecord): string {
  if (company.enrichment_state === 'enriched') {
    return 'This company has a full watchlist enrichment record available for operator review.';
  }
  if (company.enrichment_state === 'partial') {
    return 'This company has a partial enrichment record. Source-backed fields are available, but coverage is still incomplete.';
  }
  return 'This company is seeded in Watchlist Mode, but enrichment has not been completed yet.';
}

function refreshStatusLabel(refreshStatus: WatchlistRefreshStatus): string {
  if (refreshStatus === 'refreshing') {
    return 'Refreshing selected company...';
  }
  if (refreshStatus === 'refresh_succeeded') {
    return 'Refresh succeeded.';
  }
  if (refreshStatus === 'refresh_failed') {
    return 'Refresh failed.';
  }
  return 'Idle';
}

export function CompanyDetailConsole({ company, refreshStatus, refreshErrorMessage, onRefresh }: Props): JSX.Element {
  if (!company) {
    return (
      <section aria-label="Company Detail Console" style={{ border: '1px solid #d9e2ec', borderRadius: '16px', background: '#ffffff', padding: '20px' }}>
        <h3 style={{ marginTop: 0 }}>Company Detail Console</h3>
        <p>Select a watchlist company to review seeded details and enrichment results.</p>
      </section>
    );
  }

  return (
    <section aria-label="Company Detail Console" style={{ display: 'grid', gap: '12px' }}>
      <article style={{ border: '1px solid #d9e2ec', borderRadius: '16px', background: '#ffffff', padding: '20px' }}>
        <div style={{ fontSize: '12px', textTransform: 'uppercase', letterSpacing: '0.08em', color: '#1d4ed8', fontWeight: 700 }}>
          Watchlist Company Detail
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: '12px', alignItems: 'start', flexWrap: 'wrap' }}>
          <h2 style={{ marginBottom: '8px' }}>{company.company_name}</h2>
          <button
            type="button"
            onClick={onRefresh}
            disabled={refreshStatus === 'refreshing'}
            style={{
              border: '1px solid #1d4ed8',
              borderRadius: '10px',
              background: refreshStatus === 'refreshing' ? '#bfdbfe' : '#dbeafe',
              color: '#1d4ed8',
              padding: '10px 14px',
              fontWeight: 700,
              cursor: refreshStatus === 'refreshing' ? 'wait' : 'pointer',
            }}
          >
            Refresh Company
          </button>
        </div>
        <p style={{ marginTop: 0, color: '#475569' }}>{enrichmentSummary(company)}</p>
        <div style={{ display: 'grid', gap: '4px', marginBottom: '10px', fontSize: '13px', color: '#334155' }}>
          <div><strong>Refresh Status:</strong> {refreshStatusLabel(refreshStatus)}</div>
          {refreshErrorMessage ? <div><strong>Refresh Error:</strong> {refreshErrorMessage}</div> : null}
        </div>
        <div style={{ display: 'grid', gap: '6px', fontSize: '14px' }}>
          <div><strong>Category:</strong> {company.category}</div>
          <div><strong>Segment:</strong> {company.segment}</div>
          <div><strong>Priority Tier:</strong> {company.priority_tier}</div>
          <div><strong>Verification Status:</strong> {company.verification_status}</div>
          <div><strong>Website:</strong> {company.website || 'No website stored'}</div>
          <div><strong>HQ:</strong> {company.hq_city && company.hq_state ? `${company.hq_city}, ${company.hq_state}` : 'HQ details are incomplete'}</div>
          <div><strong>Notes:</strong> {company.notes || 'No operator notes stored'}</div>
        </div>
      </article>

      <KeyPeoplePanel people={company.key_people} />
      <ContactsPanel mainPhone={company.main_phone} publishedEmails={company.published_emails} emailPatternGuess={company.email_pattern_guess} />
      <RecentSignalsPanel signals={company.recent_signals} />
      <ProjectsLocationsPanel website={company.website} hqCity={company.hq_city} hqState={company.hq_state} recentProjects={company.recent_projects} />
      <SourceConfidencePanel sourceLinks={company.source_links} confidenceLevel={company.confidence_level} lastEnrichedAt={company.last_enriched_at} />
    </section>
  );
}

export default CompanyDetailConsole;
