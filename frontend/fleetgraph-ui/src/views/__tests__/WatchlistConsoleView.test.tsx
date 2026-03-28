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
  key_people: [
    {
      name: 'Morgan Hale',
      title: 'Chief Executive Officer',
      source_url: 'https://www.beaconholdings.example/leadership',
      confidence: 'HIGH',
      basis: 'seed' as const,
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
  recent_projects: [
    {
      name: 'Midwest Fleet Refresh',
      location: 'Chicago, IL',
      status: 'active',
      source_url: 'https://projects.example/midwest-fleet-refresh',
      confidence: 'MEDIUM',
    },
  ],
  source_links: [
    'https://www.beaconholdings.example/leadership',
    'https://audit.example/beacon-holdings-notice',
  ],
  last_enriched_at: '2026-03-28T09:00:00Z',
  confidence_level: 'HIGH' as const,
  enrichment_state: 'enriched' as const,
};

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
  key_people: [
    {
      name: 'Taylor Brooks',
      title: 'Chief Financial Officer',
      source_url: 'https://www.smithjonesllp.example/team',
      confidence: 'HIGH',
      basis: 'seed' as const,
    },
  ],
  published_emails: [],
  email_pattern_guess: 'first_initiallast@smithjonesllp.example',
  recent_signals: [
    {
      title: 'Document production ordered in active litigation',
      signal_type: 'litigation',
      source_url: 'https://court.example/document-production-order',
      confidence: 'HIGH',
      status: 'active',
    },
  ],
  recent_projects: [],
  source_links: [
    'https://www.smithjonesllp.example/team',
    'https://court.example/document-production-order',
  ],
  last_enriched_at: '2026-03-28T09:15:00Z',
  confidence_level: 'HIGH' as const,
  enrichment_state: 'partial' as const,
};

const atlasCompany = {
  company_id: 'atlas-services-group',
  company_name: 'Atlas Services Group',
  category: 'Field Service',
  segment: 'Commercial Services',
  priority_tier: 'Tier 2',
  website: 'https://www.atlasservices.example',
  hq_city: 'Dallas',
  hq_state: 'TX',
  hq_zip: '75201',
  phone: '',
  ceo_name: 'Casey Long',
  cfo_name: '',
  chief_risk_officer_name: '',
  verification_status: 'seed',
  notes: 'Pending enrichment refresh',
  main_phone: '',
  key_people: [],
  published_emails: [],
  email_pattern_guess: '',
  recent_signals: [],
  recent_projects: [],
  source_links: [],
  last_enriched_at: '',
  confidence_level: 'LOW' as const,
  enrichment_state: 'seed_only' as const,
};

const mockApi = vi.hoisted(() => ({
  getWatchlistCompanies: vi.fn(),
  getWatchlistCompanyDetail: vi.fn(),
  refreshWatchlistCompany: vi.fn(),
  filterWatchlistCompanies: vi.fn(),
}));

vi.mock('../../services/watchlistApi', async () => {
  const actual = await vi.importActual<typeof import('../../services/watchlistApi')>('../../services/watchlistApi');
  return {
    ...actual,
    getWatchlistCompanies: mockApi.getWatchlistCompanies,
    getWatchlistCompanyDetail: mockApi.getWatchlistCompanyDetail,
    refreshWatchlistCompany: mockApi.refreshWatchlistCompany,
    filterWatchlistCompanies: mockApi.filterWatchlistCompanies,
  };
});

async function flush(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

async function renderView(): Promise<{ container: HTMLDivElement; root: ReturnType<typeof createRoot> }> {
  const container = document.createElement('div');
  document.body.appendChild(container);
  const root = createRoot(container);

  await act(async () => {
    root.render(<WatchlistConsoleView />);
    await flush();
  });

  return { container, root };
}

async function changeSelect(container: HTMLDivElement, ariaLabel: string, value: string): Promise<void> {
  const select = container.querySelector(`select[aria-label="${ariaLabel}"]`) as HTMLSelectElement | null;
  if (!select) {
    throw new Error(`Missing select: ${ariaLabel}`);
  }

  await act(async () => {
    select.value = value;
    select.dispatchEvent(new Event('change', { bubbles: true }));
    await flush();
  });
}

async function clickCompanyRow(container: HTMLDivElement, label: string): Promise<void> {
  const row = Array.from(container.querySelectorAll('tr')).find((item) => item.textContent?.includes(label)) as HTMLTableRowElement | undefined;
  if (!row) {
    throw new Error(`Missing row for ${label}`);
  }

  await act(async () => {
    row.dispatchEvent(new MouseEvent('click', { bubbles: true }));
    await flush();
  });
}

async function clickRefresh(container: HTMLDivElement): Promise<void> {
  const button = Array.from(container.querySelectorAll('button')).find((item) => item.textContent === 'Refresh Company') as HTMLButtonElement | undefined;
  if (!button) {
    throw new Error('Missing refresh button');
  }

  await act(async () => {
    button.dispatchEvent(new MouseEvent('click', { bubbles: true }));
    await flush();
  });
}

beforeEach(() => {
  mockApi.getWatchlistCompanies.mockReset();
  mockApi.getWatchlistCompanyDetail.mockReset();
  mockApi.refreshWatchlistCompany.mockReset();
  mockApi.filterWatchlistCompanies.mockReset();

  mockApi.getWatchlistCompanies.mockResolvedValue([beaconCompany, smithCompany, atlasCompany]);
  mockApi.getWatchlistCompanyDetail.mockImplementation(async (companyId: string) => {
    const match = [beaconCompany, smithCompany, atlasCompany].find((company) => company.company_id === companyId);
    if (!match) {
      throw new Error('unknown_company_id');
    }
    return match;
  });
  mockApi.refreshWatchlistCompany.mockResolvedValue({
    ...beaconCompany,
    last_enriched_at: '2026-03-28T10:45:00Z',
  });
  mockApi.filterWatchlistCompanies.mockImplementation((companies, filters) => (
    companies.filter((company: typeof beaconCompany) => {
      if (filters.category && company.category !== filters.category) {
        return false;
      }
      if (filters.segment && company.segment !== filters.segment) {
        return false;
      }
      if (filters.priority_tier && company.priority_tier !== filters.priority_tier) {
        return false;
      }
      if (filters.verification_status && company.verification_status !== filters.verification_status) {
        return false;
      }
      if (filters.enrichment_state && company.enrichment_state !== filters.enrichment_state) {
        return false;
      }
      return true;
    })
  ));
});

afterEach(() => {
  document.body.innerHTML = '';
});

test('live watchlist data loads into the console', async () => {
  const { container, root } = await renderView();

  const html = container.innerHTML;
  expect(html).toContain('Watchlist Mode');
  expect(html).toContain('Discovery Mode remains separate');
  expect(html).toContain('Beacon Holdings');
  expect(html).toContain('Smith & Jones LLP');
  expect(html).toContain('Atlas Services Group');
  expect(mockApi.getWatchlistCompanies).toHaveBeenCalledTimes(1);
  expect(mockApi.getWatchlistCompanyDetail).toHaveBeenCalledWith('beacon-holdings');

  root.unmount();
});

test('selected company detail binds correctly from live detail service', async () => {
  const { container, root } = await renderView();

  await clickCompanyRow(container, 'Smith & Jones LLP');

  const html = container.innerHTML;
  expect(html).toContain('Smith & Jones LLP');
  expect(html).toContain('Legal Services');
  expect(html).toContain('2026-03-28T09:15:00Z');
  expect(html).toContain('partial enrichment record');

  root.unmount();
});

test('refresh success updates visible state', async () => {
  const { container, root } = await renderView();

  await clickRefresh(container);

  const html = container.innerHTML;
  expect(mockApi.refreshWatchlistCompany).toHaveBeenCalledWith('beacon-holdings');
  expect(html).toContain('Refresh succeeded.');
  expect(html).toContain('2026-03-28T10:45:00Z');

  root.unmount();
});

test('refresh failure state renders correctly', async () => {
  mockApi.refreshWatchlistCompany.mockRejectedValueOnce(new Error('refresh_connector_failure'));

  const { container, root } = await renderView();

  await clickRefresh(container);

  const html = container.innerHTML;
  expect(html).toContain('Refresh failed.');
  expect(html).toContain('refresh_connector_failure');

  root.unmount();
});

test('empty partial and enriched states render cleanly', async () => {
  const { container, root } = await renderView();

  await clickCompanyRow(container, 'Atlas Services Group');

  const html = container.innerHTML;
  expect(html).toContain('This company is seeded in Watchlist Mode, but enrichment has not been completed yet.');
  expect(html).toContain('No verified key people are available yet for this company.');
  expect(html).toContain('No public direct emails are currently stored for this company.');
  expect(html).toContain('No recent signals have been retained for this company yet.');
  expect(html).toContain('No recent projects are currently attached to this company.');
  expect(html).toContain('No source-backed links are available yet for this company.');

  root.unmount();
});

test('filters still behave deterministically with live data', async () => {
  const { container, root } = await renderView();

  await changeSelect(container, 'Category Filter', 'Professional Services');
  await changeSelect(container, 'Segment Filter', 'Legal Services');
  await changeSelect(container, 'Priority Tier Filter', 'Tier 1');
  await changeSelect(container, 'Verification Status Filter', 'verified');
  await changeSelect(container, 'Enrichment State Filter', 'partial');

  const html = container.innerHTML;
  expect(html).toContain('Smith & Jones LLP');
  expect(html).not.toContain('Beacon Holdings</td>');
  expect(html).not.toContain('Atlas Services Group</td>');

  root.unmount();
});

test('rendering remains deterministic for fixed inputs', async () => {
  const first = await renderView();
  const firstHtml = first.container.innerHTML;
  first.root.unmount();

  const second = await renderView();
  expect(second.container.innerHTML).toBe(firstHtml);
  second.root.unmount();
});
