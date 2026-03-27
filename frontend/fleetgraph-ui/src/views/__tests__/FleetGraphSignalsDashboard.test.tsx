import React from 'react';
import { act } from 'react';
import { createRoot } from 'react-dom/client';
import { afterEach, beforeEach, expect, test, vi } from 'vitest';

import FleetGraphSignalsDashboard from '../../pages/FleetGraphSignalsDashboard';

vi.mock('../../services/signalApi', () => ({
  getTodaySignals: vi.fn(),
}));

import { getTodaySignals } from '../../services/signalApi';

const mockGetTodaySignals = vi.mocked(getTodaySignals);

async function flush(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

beforeEach(() => {
  mockGetTodaySignals.mockReset();
});

afterEach(() => {
  document.body.innerHTML = '';
});

test('dashboard render sanity and top 5 hero mapping', async () => {
  mockGetTodaySignals.mockResolvedValue({
    top_signals: [
      { company: 'Atlas Build Co', signal_type: 'litigation', event_summary: 'Lawsuit filed', source: 'court.example', date_detected: '2026-03-27', confidence_score: 5, priority: 'HIGH', raw_text: 'A', recommended_action: 'CALL NOW' },
      { company: 'Beacon Masonry', signal_type: 'audit', event_summary: 'Audit notice posted', source: 'audit.example', date_detected: '2026-03-27', confidence_score: 4, priority: 'HIGH', raw_text: 'B', recommended_action: 'CALL NOW' },
      { company: 'Civic Review LLC', signal_type: 'government', event_summary: 'Review opened', source: 'gov.example', date_detected: '2026-03-27', confidence_score: 3, priority: 'MEDIUM', raw_text: 'C', recommended_action: 'CALL NOW' },
      { company: 'Delta Works', signal_type: 'project_distress', event_summary: 'Delay dispute', source: 'project.example', date_detected: '2026-03-27', confidence_score: 4, priority: 'HIGH', raw_text: 'D', recommended_action: 'CALL NOW' },
      { company: 'Evergreen Build', signal_type: 'audit', event_summary: 'Audit review', source: 'audit2.example', date_detected: '2026-03-27', confidence_score: 4, priority: 'HIGH', raw_text: 'E', recommended_action: 'CALL NOW' },
      { company: 'Foundry Civil', signal_type: 'government', event_summary: 'Hearing notice', source: 'gov2.example', date_detected: '2026-03-27', confidence_score: 3, priority: 'MEDIUM', raw_text: 'F', recommended_action: 'CALL NOW' },
    ],
    retained_count: 8,
    exported_count: 6,
    run_date: '2026-03-27',
    status: 'success',
    csv_path: 'C:/signals/daily_signals.csv',
    summary: {
      count_by_signal_type: { audit: 2, government: 2, litigation: 1, project_distress: 1 },
      count_by_priority: { HIGH: 4, MEDIUM: 2 },
      total_exported_count: 6,
      top_companies: ['Atlas Build Co', 'Beacon Masonry', 'Civic Review LLC', 'Delta Works', 'Evergreen Build', 'Foundry Civil'],
    },
  });

  const container = document.createElement('div');
  document.body.appendChild(container);
  const root = createRoot(container);

  await act(async () => {
    root.render(<FleetGraphSignalsDashboard />);
    await flush();
  });

  const html = container.innerHTML;
  expect(html).toContain('Top Signals Ready for Sales Review');
  expect(html).toContain('Run Status: success');
  expect(html).toContain('Exported: 6');
  expect(html).toContain('Retained: 8');
  expect(html).toContain('Atlas Build Co');
  expect(html).toContain('Beacon Masonry');
  expect(html).toContain('Evergreen Build');
  expect(html).not.toContain('Foundry Civil</div></article>');
  expect(html).toContain('Primary Signals');
  expect(html).toContain('Litigation');
  expect(html).toContain('Project Distress');
  expect(html).toContain('Signal Review Table');

  root.unmount();
});

test('deterministic UI mapping for priority and counts', async () => {
  mockGetTodaySignals.mockResolvedValue({
    top_signals: [
      { company: 'Atlas Build Co', signal_type: 'litigation', event_summary: 'Lawsuit filed', source: 'court.example', date_detected: '2026-03-27', confidence_score: 5, priority: 'HIGH', raw_text: 'A', recommended_action: 'CALL NOW' },
      { company: 'Beacon Masonry', signal_type: 'government', event_summary: 'Notice filed', source: 'gov.example', date_detected: '2026-03-27', confidence_score: 3, priority: 'MEDIUM', raw_text: 'B', recommended_action: 'CALL NOW' },
    ],
    retained_count: 4,
    exported_count: 2,
    run_date: '2026-03-27',
    status: 'success',
    csv_path: 'C:/signals/daily_signals.csv',
    summary: {
      count_by_signal_type: { audit: 0, government: 1, litigation: 1, project_distress: 0 },
      count_by_priority: { HIGH: 1, MEDIUM: 1 },
      total_exported_count: 2,
      top_companies: ['Atlas Build Co', 'Beacon Masonry'],
    },
  });

  const firstContainer = document.createElement('div');
  document.body.appendChild(firstContainer);
  const firstRoot = createRoot(firstContainer);

  await act(async () => {
    firstRoot.render(<FleetGraphSignalsDashboard />);
    await flush();
  });

  const firstHtml = firstContainer.innerHTML;
  firstRoot.unmount();

  const secondContainer = document.createElement('div');
  document.body.appendChild(secondContainer);
  const secondRoot = createRoot(secondContainer);

  await act(async () => {
    secondRoot.render(<FleetGraphSignalsDashboard />);
    await flush();
  });

  expect(secondContainer.innerHTML).toBe(firstHtml);
  secondRoot.unmount();
});
