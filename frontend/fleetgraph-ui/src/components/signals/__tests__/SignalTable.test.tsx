import React from 'react';
import { act } from 'react';
import { createRoot } from 'react-dom/client';
import { afterEach, expect, test } from 'vitest';

import { SignalTable } from '../SignalTable';

const signals = [
  {
    company: 'Atlas Build Co',
    signal_type: 'litigation',
    event_summary: 'Lawsuit filed',
    source: 'court.example',
    date_detected: '2026-03-27',
    confidence_score: 5,
    priority: 'HIGH' as const,
    raw_text: 'Construction contractor lawsuit filed.',
    recommended_action: 'CALL NOW',
  },
];

afterEach(() => {
  document.body.innerHTML = '';
});

test('table rendering sanity', async () => {
  const container = document.createElement('div');
  document.body.appendChild(container);
  const root = createRoot(container);

  await act(async () => {
    root.render(
      <SignalTable signals={signals} selectedCompany={'Atlas Build Co'} onSelectCompany={() => undefined} />
    );
  });

  const html = container.innerHTML;
  expect(html).toContain('Company');
  expect(html).toContain('Signal Type');
  expect(html).toContain('Event Summary');
  expect(html).toContain('Date');
  expect(html).toContain('Priority');
  expect(html).toContain('Recommended Action');
  expect(html).toContain('Atlas Build Co');
  expect(html).toContain('CALL NOW');

  root.unmount();
});
