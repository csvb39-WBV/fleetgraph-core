import React from 'react';

type Props = {
  mainPhone: string;
  publishedEmails: string[];
  emailPatternGuess: string;
};

export function ContactsPanel({ mainPhone, publishedEmails, emailPatternGuess }: Props): JSX.Element {
  return (
    <section aria-label="Contacts Panel" style={{ border: '1px solid #d9e2ec', borderRadius: '16px', background: '#ffffff', padding: '16px' }}>
      <h3 style={{ marginTop: 0 }}>Contact Intelligence</h3>
      <div style={{ display: 'grid', gap: '10px' }}>
        <div>
          <strong>Main Phone:</strong> {mainPhone || 'No public phone currently stored'}
        </div>
        <div>
          <strong>Published Emails:</strong>
          {publishedEmails.length === 0 ? (
            <div>No public direct emails are currently stored for this company.</div>
          ) : (
            <ul style={{ margin: '6px 0 0', paddingLeft: '18px' }}>
              {publishedEmails.map((email) => (
                <li key={email}>{email}</li>
              ))}
            </ul>
          )}
        </div>
        <div>
          <strong>Email Pattern Guess:</strong> {emailPatternGuess || 'No pattern guess available yet'}
        </div>
        <div style={{ fontSize: '13px', color: '#475569' }}>
          Direct emails are shown only when publicly listed. Otherwise the console displays only a pattern guess.
        </div>
      </div>
    </section>
  );
}

export default ContactsPanel;
