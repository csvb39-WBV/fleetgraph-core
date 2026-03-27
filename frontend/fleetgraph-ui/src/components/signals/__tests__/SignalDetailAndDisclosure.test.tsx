import React from 'react';
import { act } from 'react';
import { createRoot } from 'react-dom/client';
import { afterEach, expect, test } from 'vitest';

import SignalDisclosure from '../SignalDisclosure';
import { SignalDetailPanel } from '../SignalDetailPanel';

const signal = {
  company: 'Atlas Build Co',
  signal_type: 'litigation' as const,
  event_summary: 'Lawsuit filed',
  source: 'court.example',
  date_detected: '2026-03-27',
  confidence_score: 5,
  priority: 'HIGH' as const,
  raw_text: 'Construction contractor lawsuit filed.',
  recommended_action: 'CALL NOW',
};

afterEach(() => {
  document.body.innerHTML = '';
});

test('disclosure presence', async () => {
  const container = document.createElement('div');
  document.body.appendChild(container);
  const root = createRoot(container);

  await act(async () => {
    root.render(<SignalDisclosure />);
  });

  expect(container.innerHTML).toContain('FleetGraph surfaces external signals based on configured public-source searches and deterministic processing rules.');
  root.unmount();
});

test('detail panel renders deterministic why-this-matters mapping', async () => {
  const container = document.createElement('div');
  document.body.appendChild(container);
  const root = createRoot(container);

  await act(async () => {
    root.render(<SignalDetailPanel signal={signal} />);
  });

  const html = container.innerHTML;
  expect(html).toContain('Atlas Build Co');
  expect(html).toContain('Litigation can indicate active disruption, urgent vendor pain, and a short sales window.');
  expect(html).toContain('Medium to High');
  expect(html).toContain('CALL NOW');

  root.unmount();
});
