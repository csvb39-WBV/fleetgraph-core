import { afterEach, beforeEach, expect, test, vi } from 'vitest';

import { getTodaySignals } from './signalApi';

const mockFetch = vi.fn();

beforeEach(() => {
  mockFetch.mockReset();
  vi.stubGlobal('fetch', mockFetch);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

test('service validates successful response shape', async () => {
  mockFetch.mockResolvedValue({
    ok: true,
    json: async () => ({
      ok: true,
      today_signals: {
        top_signals: [],
        retained_count: 4,
        exported_count: 2,
        run_date: '2026-03-27',
        status: 'success',
        csv_path: 'C:/signals/daily_signals.csv',
        summary: {
          count_by_signal_type: {
            audit: 1,
            government: 0,
            litigation: 1,
            project_distress: 0,
          },
          count_by_priority: {
            HIGH: 2,
            MEDIUM: 0,
          },
          total_exported_count: 2,
          top_companies: ['Atlas Build Co', 'Beacon Masonry'],
        },
      },
      error_code: null,
    }),
  });

  const result = await getTodaySignals();

  expect(result.exported_count).toBe(2);
  expect(result.summary.count_by_priority.HIGH).toBe(2);
});

test('service rejects invalid payloads', async () => {
  mockFetch.mockResolvedValue({
    ok: true,
    json: async () => ({ ok: false, today_signals: null, error_code: 'missing_manifest' }),
  });

  await expect(getTodaySignals()).rejects.toThrow('missing_manifest');
});
