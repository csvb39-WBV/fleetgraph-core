import React from 'react';
import { act } from 'react';
import { createRoot } from 'react-dom/client';
import { afterEach, expect, test } from 'vitest';

import { SignalTable } from '../SignalTable';

const signals = [
  {
    company: 'Smith & Jones LLP',
    signal_type: 'litigation',
    event_summary: 'Document production ordered',
    source: 'court.example',
    date_detected: '2026-03-27',
    confidence_score: 5,
    priority: 'HIGH' as const,
    raw_text: 'Document production ordered for outside counsel Smith & Jones LLP.',
    recommended_action: 'CALL NOW',
  },
  {
    company: 'Atlas Services Group',
    signal_type: 'government',
    event_summary: 'Regulatory inquiry opened',
    source: 'agency.example',
    date_detected: '2026-03-27',
    confidence_score: 3,
    priority: 'MEDIUM' as const,
    raw_text: 'Regulatory inquiry opened for Atlas Services Group.',
    recommended_action: 'CALL NOW',
  },
];

afterEach(() => {
  document.body.innerHTML = '';
});

test('table rendering with broader names', async () => {
  const container = document.createElement('div');
  document.body.appendChild(container);
  const root = createRoot(container);

  await act(async () => {
    root.render(
      <SignalTable signals={signals} selectedCompany={'Smith & Jones LLP'} onSelectCompany={() => undefined} />
    );
  });

  const html = container.innerHTML;
  expect(html).toContain('Company');
  expect(html).toContain('Signal Type');
  expect(html).toContain('Event Summary');
  expect(html).toContain('Date');
  expect(html).toContain('Priority');
  expect(html).toContain('Recommended Action');
  expect(html).toContain('Smith & Jones LLP');
  expect(html).toContain('Atlas Services Group');
  expect(html).toContain('CALL NOW');

  root.unmount();
});
