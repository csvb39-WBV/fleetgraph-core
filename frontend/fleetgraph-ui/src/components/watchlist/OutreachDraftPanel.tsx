import React from 'react';

import type { WatchlistOutreachRecord } from '../../services/watchlistApi';

type Props = {
  outreachRecord: WatchlistOutreachRecord | undefined;
  onMarkDrafted: () => void;
  onMarkSuppressed: () => void;
};

function statusLabel(status: WatchlistOutreachRecord['outreach_status']): string {
  if (status === 'ready_to_draft') {
    return 'READY TO DRAFT';
  }
  if (status === 'drafted') {
    return 'DRAFTED';
  }
  if (status === 'suppressed') {
    return 'SUPPRESSED';
  }
  return 'NOT READY';
}

function copyValue(value: string): void {
  if (typeof navigator !== 'undefined' && navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
    void navigator.clipboard.writeText(value);
  }
}

export function OutreachDraftPanel({ outreachRecord, onMarkDrafted, onMarkSuppressed }: Props): JSX.Element {
  if (!outreachRecord) {
    return (
      <section aria-label="Outreach Draft Panel" style={{ border: '1px solid #d9e2ec', borderRadius: '16px', background: '#ffffff', padding: '16px' }}>
        <h3 style={{ marginTop: 0 }}>Outreach Draft</h3>
        <p>No outreach record is available for this company yet.</p>
      </section>
    );
  }

  const fullDraft = `${outreachRecord.subject_line}\n\n${outreachRecord.email_body}`;

  return (
    <section aria-label="Outreach Draft Panel" style={{ border: '1px solid #d9e2ec', borderRadius: '16px', background: '#ffffff', padding: '16px', display: 'grid', gap: '12px' }}>
      <div>
        <h3 style={{ marginTop: 0, marginBottom: '8px' }}>Outreach Draft</h3>
        <div style={{ display: 'grid', gap: '6px', fontSize: '14px', color: '#334155' }}>
          <div><strong>Outreach Status:</strong> {statusLabel(outreachRecord.outreach_status)}</div>
          <div><strong>Readiness:</strong> {outreachRecord.readiness_state === 'ready_to_draft' ? 'Ready to draft' : 'Not ready'}</div>
          <div><strong>Chosen Contact:</strong> {outreachRecord.contact_name || 'No named contact'}{outreachRecord.contact_email ? ` - ${outreachRecord.contact_email}` : outreachRecord.contact_phone ? ` - ${outreachRecord.contact_phone}` : ''}</div>
          <div><strong>Contact Type:</strong> {outreachRecord.contact_type}</div>
          <div><strong>Target Role Guess:</strong> {outreachRecord.target_role_guess || 'No role guess available'}</div>
        </div>
      </div>

      <div style={{ display: 'grid', gap: '6px' }}>
        <div><strong>Why Now</strong></div>
        <div>{outreachRecord.why_now || 'No why-now summary available.'}</div>
      </div>

      <div style={{ display: 'grid', gap: '6px' }}>
        <div><strong>Why This Company</strong></div>
        <div>{outreachRecord.why_this_company || 'No why-this-company summary available.'}</div>
      </div>

      <div style={{ display: 'grid', gap: '6px' }}>
        <div><strong>Signal Summary</strong></div>
        <div>{outreachRecord.signal_summary || 'No signal summary available.'}</div>
      </div>

      <div style={{ display: 'grid', gap: '8px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: '12px', alignItems: 'center', flexWrap: 'wrap' }}>
          <strong>Subject Line</strong>
          <button type="button" onClick={() => copyValue(outreachRecord.subject_line)}>Copy Subject</button>
        </div>
        <pre style={{ margin: 0, whiteSpace: 'pre-wrap', background: '#f8fafc', borderRadius: '10px', padding: '12px' }}>{outreachRecord.subject_line || 'No subject generated.'}</pre>
      </div>

      <div style={{ display: 'grid', gap: '8px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: '12px', alignItems: 'center', flexWrap: 'wrap' }}>
          <strong>Email Body</strong>
          <button type="button" onClick={() => copyValue(outreachRecord.email_body)}>Copy Body</button>
        </div>
        <pre style={{ margin: 0, whiteSpace: 'pre-wrap', background: '#f8fafc', borderRadius: '10px', padding: '12px' }}>{outreachRecord.email_body || 'No draft body generated.'}</pre>
      </div>

      <div style={{ display: 'grid', gap: '8px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', gap: '12px', alignItems: 'center', flexWrap: 'wrap' }}>
          <strong>Qualification Reasons</strong>
          <button type="button" onClick={() => copyValue(fullDraft)}>Copy Full Draft</button>
        </div>
        {outreachRecord.qualification_reasons.length === 0 ? (
          <div>No qualification reasons available.</div>
        ) : (
          <ul style={{ margin: 0, paddingLeft: '18px', display: 'grid', gap: '6px' }}>
            {outreachRecord.qualification_reasons.map((reason) => (
              <li key={reason}>{reason}</li>
            ))}
          </ul>
        )}
      </div>

      <div style={{ display: 'flex', gap: '10px', flexWrap: 'wrap' }}>
        <button type="button" onClick={onMarkDrafted}>Mark Drafted</button>
        <button type="button" onClick={onMarkSuppressed}>Mark Suppressed</button>
      </div>
    </section>
  );
}

export default OutreachDraftPanel;
