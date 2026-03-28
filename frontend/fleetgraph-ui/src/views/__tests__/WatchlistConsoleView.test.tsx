
import React from 'react';
import { act } from 'react';
import { createRoot } from 'react-dom/client';
import { afterEach, beforeEach, expect, test, vi } from 'vitest';

import { WatchlistConsoleView } from '../../views/WatchlistConsoleView';

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
      confidence: 'medium' as const,
    },
  ],
  general_emails: [
    {
      email: 'info@beaconholdings.example',
      source_url: 'https://www.beaconholdings.example/contact',
      confidence: 'high' as const,
      type: 'general_email' as const,
      is_direct: false,
    },
  ],
  key_people: [
    {
      name: 'Morgan Hale',
      title: 'Chief Executive Officer',
      source_url: 'https://www.beaconholdings.example/leadership',
      confidence: 'HIGH',
      basis: 'seed' as const,
    },
  ],
  published_emails: [
    {
      email: 'morgan.hale@beaconholdings.example',
      source_url: 'https://www.beaconholdings.example/team',
      confidence: 'high' as const,
      type: 'direct_email' as const,
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
  confidence_level: 'HIGH' as const,
  contact_confidence_level: 'high' as const,
  reachability_score: 80,
  enrichment_state: 'enriched' as const,
  outreach_record: {
    company_id: 'beacon-holdings',
    company_name: 'Beacon Holdings',
    contact_name: 'Morgan Hale',
    contact_email: 'morgan.hale@beaconholdings.example',
    contact_phone: '312-555-0101',
    contact_type: 'direct_email' as const,
    target_role_guess: 'CEO',
    signal_summary: 'Audit notice posted',
    why_now: 'Signal detected',
    why_this_company: 'High priority',
    subject_line: 'Beacon Holdings audit activity',
    email_body: 'Test body',
    source_links: [],
    outreach_status: 'ready_to_draft' as const,
    qualification_reasons: [],
    readiness_state: 'ready_to_draft' as const,
    draft_generated_at: '',
  },
};

const mockApi = vi.hoisted(() => ({
  getWatchlistCompanies: vi.fn(),
  getWatchlistCompanyDetail: vi.fn(),
  refreshWatchlistCompany: vi.fn(),
  filterWatchlistCompanies: vi.fn(),
  getWatchlistChangedCompanies: vi.fn(),
  getWatchlistTopTargets: vi.fn(),
  getWatchlistNeedsReview: vi.fn(),
  getWatchlistOutreachQueue: vi.fn(),
  updateWatchlistOutreachStatus: vi.fn(),
}));

vi.mock('../../services/watchlistApi', async () => {
  const actual = await vi.importActual<typeof import('../../services/watchlistApi')>(
    '../../services/watchlistApi',
  );
  return {
    ...actual,
    getWatchlistCompanies: mockApi.getWatchlistCompanies,
    getWatchlistCompanyDetail: mockApi.getWatchlistCompanyDetail,
    refreshWatchlistCompany: mockApi.refreshWatchlistCompany,
    filterWatchlistCompanies: mockApi.filterWatchlistCompanies,
    getWatchlistChangedCompanies: mockApi.getWatchlistChangedCompanies,
    getWatchlistTopTargets: mockApi.getWatchlistTopTargets,
    getWatchlistNeedsReview: mockApi.getWatchlistNeedsReview,
    getWatchlistOutreachQueue: mockApi.getWatchlistOutreachQueue,
    updateWatchlistOutreachStatus: mockApi.updateWatchlistOutreachStatus,
  };
});

async function flush(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

beforeEach(() => {
mockApi.getWatchlistCompanies.mockResolvedValue([beaconCompany]);
mockApi.getWatchlistCompanyDetail.mockResolvedValue(beaconCompany);
mockApi.getWatchlistChangedCompanies.mockResolvedValue([]);
mockApi.getWatchlistTopTargets.mockResolvedValue([]);
mockApi.getWatchlistNeedsReview.mockResolvedValue([]);
mockApi.getWatchlistOutreachQueue.mockResolvedValue([
  {
    company_id: 'beacon-holdings',
    company_name: 'Beacon Holdings',
    contact_name: 'Morgan Hale',
    contact_email: 'morgan.hale@beaconholdings.example',
    outreach_status: 'ready_to_draft',
    readiness_state: 'ready_to_draft',
  },
]);
mockApi.updateWatchlistOutreachStatus.mockResolvedValue({
  ...beaconCompany.outreach_record,
  outreach_status: 'drafted',
});
mockApi.filterWatchlistCompanies.mockImplementation((companies: typeof beaconCompany[]) => companies);
});

afterEach(() => {
  document.body.innerHTML = '';
});

test('renders and shows outreach queue', async () => {
  const container = document.createElement('div');
  const root = createRoot(container);

  await act(async () => {
    root.render(<WatchlistConsoleView />);
    await flush();
  });

  expect(container.innerHTML).toContain('Outreach Queue');
  expect(container.innerHTML).toContain('Beacon Holdings');
  expect(container.innerHTML).toContain('READY TO DRAFT');

  root.unmount();
});

test('draft button updates status', async () => {
  const container = document.createElement('div');
  const root = createRoot(container);

  await act(async () => {
    root.render(<WatchlistConsoleView />);
    await flush();
  });

  const btn = Array.from(container.querySelectorAll('button')).find(
    (b) => b.textContent === 'Mark Drafted',
  ) as HTMLButtonElement;

  await act(async () => {
  btn.click();
  await flush();
  await flush();
});

expect(mockApi.updateWatchlistOutreachStatus).toHaveBeenCalledWith('beacon-holdings', 'drafted');

root.unmount();
});