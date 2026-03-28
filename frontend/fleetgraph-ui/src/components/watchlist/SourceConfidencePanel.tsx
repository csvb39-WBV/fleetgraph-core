import React from 'react';

type Props = {
  sourceLinks: string[];
  confidenceLevel: string;
  lastEnrichedAt: string;
};

export function SourceConfidencePanel({ sourceLinks, confidenceLevel, lastEnrichedAt }: Props): JSX.Element {
  return (
    <section aria-label="Source Confidence Panel" style={{ border: '1px solid #d9e2ec', borderRadius: '16px', background: '#ffffff', padding: '16px' }}>
      <h3 style={{ marginTop: 0 }}>Source & Confidence</h3>
      <div style={{ display: 'grid', gap: '8px' }}>
        <div><strong>Confidence Level:</strong> {confidenceLevel}</div>
        <div><strong>Last Refreshed:</strong> {lastEnrichedAt || 'Not enriched yet'}</div>
      </div>
      <div style={{ marginTop: '12px' }}>
        <strong>Source Links</strong>
        {sourceLinks.length === 0 ? (
          <p style={{ marginBottom: 0 }}>No source-backed links are available yet for this company.</p>
        ) : (
          <ul style={{ margin: '8px 0 0', paddingLeft: '18px', display: 'grid', gap: '8px' }}>
            {sourceLinks.map((link) => (
              <li key={link}>{link}</li>
            ))}
          </ul>
        )}
      </div>
    </section>
  );
}

export default SourceConfidencePanel;
