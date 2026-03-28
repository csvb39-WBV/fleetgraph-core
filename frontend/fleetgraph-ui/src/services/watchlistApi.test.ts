import { afterEach, beforeEach, expect, test, vi } from 'vitest';

import {
  filterWatchlistCompanies,
  getWatchlistChangedCompanies,
  getWatchlistCompanies,
  getWatchlistCompanyDetail,
  getWatchlistNeedsReview,
  getWatchlistOutreachQueue,
  getWatchlistTopTargets,
  refreshWatchlistCompany,
  updateWatchlistOutreachStatus,
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
  direct_phones: [
    {
      phone: '312-555-0101',
      source_url: 'https://www.beaconholdings.example/contact',
      confidence: 'medium',
    },
  ],
  general_emails: [
    {
      email: 'info@beaconholdings.example',
      source_url: 'https://www.beaconholdings.example/contact',
      confidence: 'high',
      type: 'general_email',
      is_direct: false,
    },
  ],
  key_people: [
    {
      name: 'Morgan Hale',
      title: 'Chief Executive Officer',
      source_url: 'https://www.beaconholdings.example/leadership',
      confidence: 'HIGH',
      basis: 'seed',
    },
  ],
  published_emails: [
    {
      email: 'morgan.hale@beaconholdings.example',
      source_url: 'https://www.beaconholdings.example/team',
      confidence: 'high',
      type: 'direct_email',
      is_direct: true,
    },
  ],
  contact_pages: ['https://www.beaconholdings.example/contact'],
  leadership_pages: ['https://www.beaconholdings.example/leadership'],
  address_lines: ['100 Main St, Chicago, IL 60601'],
  contact_sources: [
    'https://www.beaconholdings.example/contact',
    'https://www.beaconholdings.example/team',
  ],
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
  contact_confidence_level: 'high',
  reachability_score: 80,
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
    new_email_count: 1,
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
  outreach_record: {
    company_id: 'beacon-holdings',
    company_name: 'Beacon Holdings',
    contact_name: 'Morgan Hale',
    contact_email: 'morgan.hale@beaconholdings.example',
    contact_phone: '312-555-0101',
    contact_type: 'direct_email',
    target_role_guess: 'Chief Executive Officer',
    signal_summary: 'Audit notice posted for Beacon Holdings.',
    why_now: 'A new public audit signal suggests active document pressure.',
    why_this_company: 'Beacon Holdings has active watchlist signals and direct contact coverage.',
    subject_line: 'Beacon Holdings audit activity',
    email_body: 'Morgan Hale,\n\nWe noticed public audit activity involving Beacon Holdings and wanted to share how FactLedger helps teams respond quickly.\n\nWould a short conversation be useful?',
    source_links: ['https://audit.example/beacon-holdings-notice'],
    outreach_status: 'ready_to_draft',
    qualification_reasons: ['meaningful_signal_present', 'direct_email_available'],
    readiness_state: 'ready_to_draft',
    draft_generated_at: '2026-03-28T09:05:00Z',
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
  direct_phones: [],
  general_emails: [],
  key_people: [],
  published_emails: [],
  contact_pages: [],
  leadership_pages: [],
  address_lines: [],
  contact_sources: [],
  email_pattern_guess: 'first_initiallast@smithjonesllp.example',
  recent_signals: [],
  recent_projects: [],
  source_links: [],
  last_enriched_at: '',
  confidence_level: 'MEDIUM',
  contact_confidence_level: 'low',
  reachability_score: 0,
  enrichment_state: 'partial',
  outreach_record: {
    company_id: 'smith-jones-llp',
    company_name: 'Smith & Jones LLP',
    contact_name: '',
    contact_email: '',
    contact_phone: '',
    contact_type: 'none',
    target_role_guess: 'Finance or Legal',
    signal_summary: 'Document production ordered in active litigation.',
    why_now: 'Public litigation activity suggests possible document burden.',
    why_this_company: 'The company has active review-worthy signals but lacks a usable contact path.',
    subject_line: '',
    email_body: '',
    source_links: ['https://court.example/document-production-order'],
    outreach_status: 'not_ready',
    qualification_reasons: ['missing_contact_method'],
    readiness_state: 'not_ready',
    draft_generated_at: '',
  },
} as const;

beforeEach(() => {
  mockFetch.mockReset();
  vi.stubGlobal('fetch', mockFetch);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

test('live watchlist companies load deterministically with contact fields', async () => {
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
  expect(first[0].published_emails[0].type).toBe('direct_email');
  expect(first[0].direct_phones[0].phone).toBe('312-555-0101');
});

test('company detail binds from live detail endpoint with reachability fields', async () => {
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
  expect(result.contact_confidence_level).toBe('high');
  expect(result.reachability_score).toBe(80);
});

test('refresh returns updated company record with contact intelligence intact', async () => {
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
  expect(result.published_emails[0].email).toBe('morgan.hale@beaconholdings.example');
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

test('top targets payload parses deterministically with reachability fields', async () => {
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
          reachability_score: 80,
          contact_confidence_level: 'high',
        },
      ],
      error_code: null,
    }),
  });

  const result = await getWatchlistTopTargets();

  expect(result[0].reachability_score).toBe(80);
  expect(result[0].contact_confidence_level).toBe('high');
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

test('outreach queue payload parses deterministically', async () => {
  mockFetch.mockResolvedValue({
    ok: true,
    json: async () => ({
      ok: true,
      outreach_queue: [
        {
          company_id: 'beacon-holdings',
          company_name: 'Beacon Holdings',
          contact_name: 'Morgan Hale',
          contact_email: 'morgan.hale@beaconholdings.example',
          outreach_status: 'ready_to_draft',
          readiness_state: 'ready_to_draft',
        },
      ],
      error_code: null,
    }),
  });

  const result = await getWatchlistOutreachQueue();

  expect(result[0].company_name).toBe('Beacon Holdings');
  expect(result[0].outreach_status).toBe('ready_to_draft');
});

test('outreach status update returns deterministic record', async () => {
  mockFetch.mockResolvedValue({
    ok: true,
    json: async () => ({
      ok: true,
      outreach_record: {
        ...beaconCompany.outreach_record,
        outreach_status: 'drafted',
      },
      error_code: null,
    }),
  });

  const result = await updateWatchlistOutreachStatus('beacon-holdings', 'drafted');

  expect(result.outreach_status).toBe('drafted');
  expect(result.subject_line).toBe('Beacon Holdings audit activity');
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
