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

test('dashboard render with mixed entity types', async () => {
  mockGetTodaySignals.mockResolvedValue({
    top_signals: [
      { company: 'Smith & Jones LLP', signal_type: 'litigation', event_summary: 'Document production ordered', source: 'court.example', date_detected: '2026-03-27', confidence_score: 5, priority: 'HIGH', raw_text: 'A', recommended_action: 'CALL NOW' },
      { company: 'Beacon Holdings', signal_type: 'audit', event_summary: 'Audit notice posted', source: 'audit.example', date_detected: '2026-03-27', confidence_score: 4, priority: 'HIGH', raw_text: 'B', recommended_action: 'CALL NOW' },
      { company: 'Atlas Services Group', signal_type: 'government', event_summary: 'Regulatory inquiry opened', source: 'gov.example', date_detected: '2026-03-27', confidence_score: 3, priority: 'MEDIUM', raw_text: 'C', recommended_action: 'CALL NOW' },
      { company: 'North Harbor Developers', signal_type: 'project_distress', event_summary: 'Delay dispute reported', source: 'project.example', date_detected: '2026-03-27', confidence_score: 4, priority: 'HIGH', raw_text: 'D', recommended_action: 'CALL NOW' },
      { company: 'Gray Counsel PLLC', signal_type: 'litigation', event_summary: 'Subpoena issued', source: 'court2.example', date_detected: '2026-03-27', confidence_score: 5, priority: 'HIGH', raw_text: 'E', recommended_action: 'CALL NOW' },
      { company: 'Mercury Legal Department', signal_type: 'government', event_summary: 'Inquiry notice received', source: 'gov2.example', date_detected: '2026-03-27', confidence_score: 3, priority: 'MEDIUM', raw_text: 'F', recommended_action: 'CALL NOW' },
    ],
    retained_count: 8,
    exported_count: 6,
    run_date: '2026-03-27',
    status: 'success',
    csv_path: 'C:/signals/daily_signals.csv',
    summary: {
      count_by_signal_type: { audit: 1, government: 2, litigation: 2, project_distress: 1 },
      count_by_priority: { HIGH: 4, MEDIUM: 2 },
      total_exported_count: 6,
      top_companies: ['Atlas Services Group', 'Beacon Holdings', 'Gray Counsel PLLC', 'Mercury Legal Department', 'North Harbor Developers', 'Smith & Jones LLP'],
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
  expect(html).toContain('Top Signals Ready for Operator Review');
  expect(html).toContain('litigation, compliance, government, and project-risk signals');
  expect(html).toContain('Smith & Jones LLP');
  expect(html).toContain('Beacon Holdings');
  expect(html).toContain('Gray Counsel PLLC');
  expect(html).not.toContain('Mercury Legal Department</div></article>');
  expect(html).toContain('Signal Review Table');

  root.unmount();
});

test('deterministic UI mapping for priority and counts', async () => {
  mockGetTodaySignals.mockResolvedValue({
    top_signals: [
      { company: 'Smith & Jones LLP', signal_type: 'litigation', event_summary: 'Document production ordered', source: 'court.example', date_detected: '2026-03-27', confidence_score: 5, priority: 'HIGH', raw_text: 'A', recommended_action: 'CALL NOW' },
      { company: 'Atlas Services Group', signal_type: 'government', event_summary: 'Notice filed', source: 'gov.example', date_detected: '2026-03-27', confidence_score: 3, priority: 'MEDIUM', raw_text: 'B', recommended_action: 'CALL NOW' },
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
      top_companies: ['Atlas Services Group', 'Smith & Jones LLP'],
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
