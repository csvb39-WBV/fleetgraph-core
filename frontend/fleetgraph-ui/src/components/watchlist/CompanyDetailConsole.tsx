import React from 'react';

import type { ContactConfidenceLevel, WatchlistCompanyRecord, WatchlistRefreshStatus } from '../../services/watchlistApi';
import ContactsPanel from './ContactsPanel';
import KeyPeoplePanel from './KeyPeoplePanel';
import OutreachDraftPanel from './OutreachDraftPanel';
import ProjectsLocationsPanel from './ProjectsLocationsPanel';
import RecentSignalsPanel from './RecentSignalsPanel';
import SourceConfidencePanel from './SourceConfidencePanel';

type Props = {
  company: WatchlistCompanyRecord | null;
  refreshStatus: WatchlistRefreshStatus;
  refreshErrorMessage: string;
  onRefresh: () => void;
  onMarkDrafted: () => void;
  onMarkSuppressed: () => void;
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

function formatReasonCodes(reasonCodes: string[]): string {
  if (reasonCodes.length === 0) {
    return 'No priority reasons available.';
  }
  return reasonCodes.join(', ');
}

function deltaSummaryText(company: WatchlistCompanyRecord): string {
  if (!company.delta_summary || company.delta_summary.change_detected !== true) {
    return 'No new changes detected since the last comparison.';
  }

  const parts: string[] = [];
  if (company.delta_summary.new_signal_count > 0) {
    parts.push(`${company.delta_summary.new_signal_count} new signal(s)`);
  }
  if (company.delta_summary.new_project_count > 0) {
    parts.push(`${company.delta_summary.new_project_count} new project(s)`);
  }
  if (company.delta_summary.new_email_count > 0) {
    parts.push(`${company.delta_summary.new_email_count} new email(s)`);
  }
  if (company.delta_summary.new_key_people_count > 0) {
    parts.push(`${company.delta_summary.new_key_people_count} new key people`);
  }
  if (company.delta_summary.confidence_changed) {
    parts.push('confidence changed');
  }

  if (parts.length === 0) {
    return company.delta_summary.change_types.join(', ') || 'Change detected.';
  }
  return parts.join(', ');
}

function reachabilityLabel(reachabilityScore: number, confidence: ContactConfidenceLevel): string {
  if (reachabilityScore >= 55 || confidence === 'high') {
    return 'HIGH REACHABILITY';
  }
  if (reachabilityScore >= 25 || confidence === 'medium') {
    return 'MEDIUM REACHABILITY';
  }
  return 'LOW REACHABILITY';
}

export function CompanyDetailConsole({
  company,
  refreshStatus,
  refreshErrorMessage,
  onRefresh,
  onMarkDrafted,
  onMarkSuppressed,
}: Props): JSX.Element {
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
        <div style={{ display: 'grid', gap: '6px', marginBottom: '12px', fontSize: '13px', color: '#334155' }}>
          <div><strong>Changed Since Last Run:</strong> {company.delta_summary?.change_detected ? 'Yes' : 'No'}</div>
          <div><strong>Delta Summary:</strong> {deltaSummaryText(company)}</div>
          <div><strong>Priority Score:</strong> {company.priority_summary?.priority_score ?? company.delta_summary?.priority_score ?? 0}</div>
          <div><strong>Priority Reasons:</strong> {formatReasonCodes(company.priority_summary?.priority_reason_codes ?? company.delta_summary?.priority_reason_codes ?? [])}</div>
          <div><strong>Needs Review:</strong> {company.priority_summary?.needs_review ? 'Yes' : 'No'}</div>
          <div><strong>Reachability:</strong> {reachabilityLabel(company.reachability_score, company.contact_confidence_level)}</div>
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
      <ContactsPanel
        directPhones={company.direct_phones}
        generalEmails={company.general_emails}
        publishedEmails={company.published_emails}
        contactPages={company.contact_pages}
        leadershipPages={company.leadership_pages}
        emailPatternGuess={company.email_pattern_guess}
        contactConfidenceLevel={company.contact_confidence_level}
        reachabilityScore={company.reachability_score}
      />
      <OutreachDraftPanel
        outreachRecord={company.outreach_record}
        onMarkDrafted={onMarkDrafted}
        onMarkSuppressed={onMarkSuppressed}
      />
      <RecentSignalsPanel signals={company.recent_signals} />
      <ProjectsLocationsPanel website={company.website} hqCity={company.hq_city} hqState={company.hq_state} recentProjects={company.recent_projects} />
      <SourceConfidencePanel sourceLinks={company.source_links} confidenceLevel={company.confidence_level} lastEnrichedAt={company.last_enriched_at} />
    </section>
  );
}

export default CompanyDetailConsole;
