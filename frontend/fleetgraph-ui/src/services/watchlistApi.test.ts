import { afterEach, beforeEach, expect, test, vi } from 'vitest';

import {
  filterWatchlistCompanies,
  getWatchlistChangedCompanies,
  getWatchlistCompanies,
  getWatchlistCompanyDetail,
  getWatchlistNeedsReview,
  getWatchlistTopTargets,
  refreshWatchlistCompany,
} from './watchlistApi';

const mockFetch = vi.fn();

const beaconCompany = {
  company_id: 'beacon-holdings',
  company_name: 'Beacon Holdings',
  category: 'Logistics',
  segment: 'Fleet Operations',
  priority_tier: 'Tier 1',
  website: 'https://www.beaconholdings.example',
  hq_city: 'Chicago',
  hq_state: 'IL',
  hq_zip: '60601',
  phone: '312-555-0100',
  ceo_name: 'Morgan Hale',
  cfo_name: 'Jordan Pike',
  chief_risk_officer_name: 'Avery Stone',
  verification_status: 'verified',
  notes: 'Pilot verified subset company',
  main_phone: '312-555-0100',
  key_people: [
    {
      name: 'Morgan Hale',
      title: 'Chief Executive Officer',
      source_url: 'https://www.beaconholdings.example/leadership',
      confidence: 'HIGH',
      basis: 'seed',
    },
  ],
  published_emails: [],
  email_pattern_guess: 'first.last@beaconholdings.example',
  recent_signals: [
    {
      title: 'Audit notice posted for Beacon Holdings',
      signal_type: 'audit',
      source_url: 'https://audit.example/beacon-holdings-notice',
      confidence: 'HIGH',
      status: 'open',
    },
  ],
  recent_projects: [],
  source_links: ['https://audit.example/beacon-holdings-notice'],
  last_enriched_at: '2026-03-28T09:00:00Z',
  confidence_level: 'HIGH',
  enrichment_state: 'enriched',
  delta_summary: {
    company_id: 'beacon-holdings',
    company_name: 'Beacon Holdings',
    change_detected: true,
    change_types: ['new_signal_added', 'last_enriched_at_changed'],
    previous_enrichment_state: 'partial',
    current_enrichment_state: 'enriched',
    new_signal_count: 1,
    new_project_count: 0,
    new_email_count: 0,
    new_key_people_count: 0,
    confidence_changed: false,
    last_enriched_at: '2026-03-28T09:00:00Z',
    priority_score: 96,
    priority_reason_codes: ['tier_1_company', 'new_signal_detected'],
  },
  priority_summary: {
    company_id: 'beacon-holdings',
    company_name: 'Beacon Holdings',
    priority_score: 96,
    priority_band: 'HIGH',
    priority_reason_codes: ['tier_1_company', 'new_signal_detected'],
    changed_since_last_run: true,
    needs_review: true,
    needs_review_reasons: ['changed_since_last_run'],
  },
} as const;

const smithCompany = {
  company_id: 'smith-jones-llp',
  company_name: 'Smith & Jones LLP',
  category: 'Professional Services',
  segment: 'Legal Services',
  priority_tier: 'Tier 1',
  website: 'https://www.smithjonesllp.example',
  hq_city: 'New York',
  hq_state: 'NY',
  hq_zip: '10005',
  phone: '212-555-0199',
  ceo_name: '',
  cfo_name: 'Taylor Brooks',
  chief_risk_officer_name: '',
  verification_status: 'verified',
  notes: 'Pilot verified subset company',
  main_phone: '212-555-0199',
  key_people: [],
  published_emails: [],
  email_pattern_guess: 'first_initiallast@smithjonesllp.example',
  recent_signals: [],
  recent_projects: [],
  source_links: [],
  last_enriched_at: '',
  confidence_level: 'MEDIUM',
  enrichment_state: 'partial',
} as const;

beforeEach(() => {
  mockFetch.mockReset();
  vi.stubGlobal('fetch', mockFetch);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

test('live watchlist companies load deterministically', async () => {
  mockFetch.mockResolvedValue({
    ok: true,
    json: async () => ({
      ok: true,
      companies: [smithCompany, beaconCompany],
      error_code: null,
    }),
  });

  const first = await getWatchlistCompanies();
  const second = await getWatchlistCompanies();

  expect(first).toEqual(second);
  expect(first.map((company) => company.company_id)).toEqual([
    'beacon-holdings',
    'smith-jones-llp',
  ]);
});

test('company detail binds from live detail endpoint', async () => {
  mockFetch.mockResolvedValue({
    ok: true,
    json: async () => ({
      ok: true,
      company: beaconCompany,
      error_code: null,
    }),
  });

  const result = await getWatchlistCompanyDetail('beacon-holdings');

  expect(result.company_name).toBe('Beacon Holdings');
  expect(result.last_enriched_at).toBe('2026-03-28T09:00:00Z');
});

test('refresh returns updated company record', async () => {
  mockFetch.mockResolvedValue({
    ok: true,
    json: async () => ({
      ok: true,
      company: {
        ...beaconCompany,
        last_enriched_at: '2026-03-28T10:45:00Z',
      },
      refresh_status: 'refresh_succeeded',
      error_code: null,
    }),
  });

  const result = await refreshWatchlistCompany('beacon-holdings');

  expect(result.last_enriched_at).toBe('2026-03-28T10:45:00Z');
});

test('changed companies payload parses deterministically', async () => {
  mockFetch.mockResolvedValue({
    ok: true,
    json: async () => ({
      ok: true,
      changed_companies: [beaconCompany.delta_summary],
      error_code: null,
    }),
  });

  const result = await getWatchlistChangedCompanies();

  expect(result).toEqual([beaconCompany.delta_summary]);
});

test('top targets payload parses deterministically', async () => {
  mockFetch.mockResolvedValue({
    ok: true,
    json: async () => ({
      ok: true,
      top_targets: [
        {
          company_id: 'beacon-holdings',
          company_name: 'Beacon Holdings',
          priority_score: 96,
          priority_band: 'HIGH',
          reason_summary: 'Tier 1 company with new signal activity.',
          current_enrichment_state: 'enriched',
          change_detected: true,
          priority_reason_codes: ['tier_1_company', 'new_signal_detected'],
        },
      ],
      error_code: null,
    }),
  });

  const result = await getWatchlistTopTargets();

  expect(result[0].priority_score).toBe(96);
  expect(result[0].reason_summary).toBe('Tier 1 company with new signal activity.');
});

test('needs review payload parses deterministically', async () => {
  mockFetch.mockResolvedValue({
    ok: true,
    json: async () => ({
      ok: true,
      needs_review: [
        {
          company_id: 'beacon-holdings',
          company_name: 'Beacon Holdings',
          review_reason_summary: 'Changed since last run.',
          current_enrichment_state: 'enriched',
          priority_score: 96,
          last_enriched_at: '2026-03-28T09:00:00Z',
        },
      ],
      error_code: null,
    }),
  });

  const result = await getWatchlistNeedsReview();

  expect(result[0].company_name).toBe('Beacon Holdings');
  expect(result[0].priority_score).toBe(96);
});

test('filters apply deterministically to live company records', async () => {
  const filtered = filterWatchlistCompanies([beaconCompany, smithCompany], {
    category: 'Professional Services',
    segment: 'Legal Services',
    priority_tier: 'Tier 1',
    verification_status: 'verified',
    enrichment_state: 'partial',
  });

  expect(filtered.map((company) => company.company_id)).toEqual(['smith-jones-llp']);
});
