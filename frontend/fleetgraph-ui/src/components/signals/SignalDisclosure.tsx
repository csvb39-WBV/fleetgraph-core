import React from 'react';

export function SignalDisclosure(): JSX.Element {
  return (
    <section
      aria-label="Signal Disclosure"
      style={{
        border: '1px solid #cbd5e1',
        borderRadius: '14px',
        background: '#f8fafc',
        color: '#334155',
        padding: '14px 16px',
        fontSize: '14px',
        lineHeight: 1.5,
      }}
    >
      FleetGraph surfaces external signals based on configured public-source searches and deterministic processing rules. Results are operational leads, not factual conclusions or recommendations.
    </section>
  );
}

export default SignalDisclosure;
