import React from 'react';

import type { WatchlistEmailContact, WatchlistPhoneContact } from '../../services/watchlistApi';

type Props = {
  directPhones: WatchlistPhoneContact[];
  generalEmails: WatchlistEmailContact[];
  publishedEmails: WatchlistEmailContact[];
  contactPages: string[];
  leadershipPages: string[];
  emailPatternGuess: string;
  contactConfidenceLevel: 'high' | 'medium' | 'low';
  reachabilityScore: number;
};

function contactQualityMessage(
  directEmails: WatchlistEmailContact[],
  directPhones: WatchlistPhoneContact[],
  generalEmails: WatchlistEmailContact[],
  emailPatternGuess: string,
): string {
  if (directEmails.length > 0) {
    return 'Direct contact available';
  }
  if (directPhones.length > 0) {
    return 'Phone available';
  }
  if (generalEmails.length > 0) {
    return 'General inbox only';
  }
  if (emailPatternGuess) {
    return 'No direct contact - pattern only';
  }
  return 'No public contact found';
}

function renderEmailContact(contact: WatchlistEmailContact): JSX.Element {
  return (
    <li key={`${contact.email}-${contact.source_url}`}>
      <strong>{contact.email}</strong>
      <div style={{ fontSize: '13px', color: '#475569' }}>
        Type: {contact.type} - Confidence: {contact.confidence} - Direct: {contact.is_direct ? 'Yes' : 'No'}
      </div>
      <div style={{ fontSize: '13px', color: '#475569' }}>Source: {contact.source_url}</div>
    </li>
  );
}

function renderPhoneContact(contact: WatchlistPhoneContact): JSX.Element {
  return (
    <li key={`${contact.phone}-${contact.source_url}`}>
      <strong>{contact.phone}</strong>
      <div style={{ fontSize: '13px', color: '#475569' }}>
        Type: phone - Confidence: {contact.confidence}
      </div>
      <div style={{ fontSize: '13px', color: '#475569' }}>Source: {contact.source_url}</div>
    </li>
  );
}

function renderPageLink(page: string, type: 'page' | 'pattern'): JSX.Element {
  return (
    <li key={`${type}-${page}`}>
      <strong>{page}</strong>
      <div style={{ fontSize: '13px', color: '#475569' }}>Type: {type}</div>
    </li>
  );
}

export function ContactsPanel({
  directPhones,
  generalEmails,
  publishedEmails,
  contactPages,
  leadershipPages,
  emailPatternGuess,
  contactConfidenceLevel,
  reachabilityScore,
}: Props): JSX.Element {
  const bestContactOptions = [...publishedEmails, ...directPhones];
  const secondaryContactOptions = [...generalEmails];
  const dedupedPageLinks = [...new Set([...contactPages, ...leadershipPages])];

  return (
    <section aria-label="Contacts Panel" style={{ border: '1px solid #d9e2ec', borderRadius: '16px', background: '#ffffff', padding: '16px' }}>
      <h3 style={{ marginTop: 0 }}>Contact Intelligence</h3>
      <div style={{ display: 'grid', gap: '8px', marginBottom: '12px' }}>
        <div><strong>Reachability Score:</strong> {reachabilityScore}</div>
        <div><strong>Contact Confidence:</strong> {contactConfidenceLevel}</div>
        <div><strong>Status:</strong> {contactQualityMessage(publishedEmails, directPhones, generalEmails, emailPatternGuess)}</div>
      </div>

      <div style={{ display: 'grid', gap: '14px' }}>
        <div>
          <strong>Best Contact Options</strong>
          {bestContactOptions.length === 0 ? (
            <p style={{ marginBottom: 0 }}>No direct contact options are currently available.</p>
          ) : (
            <ul style={{ margin: '6px 0 0', paddingLeft: '18px', display: 'grid', gap: '8px' }}>
              {publishedEmails.map((contact) => renderEmailContact(contact))}
              {directPhones.map((contact) => renderPhoneContact(contact))}
            </ul>
          )}
        </div>

        <div>
          <strong>Secondary Contact Options</strong>
          {secondaryContactOptions.length === 0 && dedupedPageLinks.length === 0 ? (
            <p style={{ marginBottom: 0 }}>No general inboxes or contact pages are currently available.</p>
          ) : (
            <ul style={{ margin: '6px 0 0', paddingLeft: '18px', display: 'grid', gap: '8px' }}>
              {secondaryContactOptions.map((contact) => renderEmailContact(contact))}
              {dedupedPageLinks.map((page) => renderPageLink(page, 'page'))}
            </ul>
          )}
        </div>

        <div>
          <strong>Fallback</strong>
          {!emailPatternGuess ? (
            <p style={{ marginBottom: 0 }}>No direct contact - pattern only is not available yet.</p>
          ) : (
            <ul style={{ margin: '6px 0 0', paddingLeft: '18px', display: 'grid', gap: '8px' }}>
              {renderPageLink(emailPatternGuess, 'pattern')}
            </ul>
          )}
        </div>
      </div>
    </section>
  );
}

export default ContactsPanel;
