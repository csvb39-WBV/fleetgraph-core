import React from 'react';

import type { WatchlistPerson } from '../../services/watchlistApi';

type Props = {
  people: WatchlistPerson[];
};

export function KeyPeoplePanel({ people }: Props): JSX.Element {
  return (
    <section aria-label="Key People Panel" style={{ border: '1px solid #d9e2ec', borderRadius: '16px', background: '#ffffff', padding: '16px' }}>
      <h3 style={{ marginTop: 0 }}>Key People</h3>
      {people.length === 0 ? (
        <p>No verified key people are available yet for this company.</p>
      ) : (
        <ul style={{ margin: 0, paddingLeft: '18px', display: 'grid', gap: '10px' }}>
          {people.map((person) => (
            <li key={`${person.name}-${person.title}`}>
              <strong>{person.name}</strong> — {person.title}
              <div style={{ fontSize: '13px', color: '#475569' }}>
                Source-backed via {person.source_url} · Confidence: {person.confidence} · Basis: {person.basis}
              </div>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}

export default KeyPeoplePanel;
