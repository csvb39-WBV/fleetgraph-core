import React from 'react';
import { act } from 'react';
import { createRoot } from 'react-dom/client';
import { afterEach, expect, test } from 'vitest';

import SignalDisclosure from '../SignalDisclosure';
import { SignalDetailPanel } from '../SignalDetailPanel';

const legalSignal = {
  company: 'Smith & Jones LLP',
  signal_type: 'litigation' as const,
  event_summary: 'Document production ordered',
  source: 'rss_news://court-feed',
  date_detected: '2026-03-28',
  confidence_score: 5,
  priority: 'HIGH' as const,
  raw_text: 'Document production ordered for outside counsel Smith & Jones LLP.',
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

test('detail panel renders source and deterministic legal-oriented mapping', async () => {
  const container = document.createElement('div');
  document.body.appendChild(container);
  const root = createRoot(container);

  await act(async () => {
    root.render(<SignalDetailPanel signal={legalSignal} />);
  });

  const html = container.innerHTML;
  expect(html).toContain('Smith & Jones LLP');
  expect(html).toContain('HIGH CONFIDENCE');
  expect(html).toContain('News');
  expect(html).toContain('Subpoena and document-production activity can trigger urgent preservation, collection, and review workloads.');
  expect(html).toContain('High');
  expect(html).toContain('Strong fit for collection tracking, review coordination, privilege-sensitive chronology, and production readiness.');
  expect(html).toContain('CALL NOW');

  root.unmount();
});
