import React from 'react';
import { act } from 'react';
import { createRoot } from 'react-dom/client';
import { afterEach, beforeEach, expect, test, vi } from 'vitest';

import { WatchlistConsoleView } from '../../views/WatchlistConsoleView';

const mockedCompanies = [
  {
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
    confidence_level: 'HIGH',
    enrichment_state: 'enriched',
  },
  {
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
        basis: 'seed',
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
    confidence_level: 'HIGH',
    enrichment_state: 'partial',
  },
  {
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
    confidence_level: 'LOW',
    enrichment_state: 'seed_only',
  },
];

vi.mock('../../services/watchlistApi', async () => {
  const actual = await vi.importActual<typeof import('../../services/watchlistApi')>('../../services/watchlistApi');
  return {
    ...actual,
    getWatchlistPilotCompanies: vi.fn(async () => mockedCompanies.map((company) => ({
      ...company,
      key_people: company.key_people.map((person) => ({ ...person })),
      published_emails: [...company.published_emails],
      recent_signals: company.recent_signals.map((signal) => ({ ...signal })),
      recent_projects: company.recent_projects.map((project) => ({ ...project })),
      source_links: [...company.source_links],
    }))),
  };
});

async function flush(): Promise<void> {
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

afterEach(() => {
  document.body.innerHTML = '';
});

beforeEach(() => {
  document.body.innerHTML = '';
});

test('watchlist dashboard renders pilot companies and mode distinction', async () => {
  const { container, root } = await renderView();

  const html = container.innerHTML;
  expect(html).toContain('Watchlist Mode');
  expect(html).toContain('Discovery Mode remains separate');
  expect(html).toContain('Beacon Holdings');
  expect(html).toContain('Smith & Jones LLP');
  expect(html).toContain('Atlas Services Group');
  expect(html).toContain('Watchlist Company Detail');

  root.unmount();
});

test('filters work deterministically', async () => {
  const { container, root } = await renderView();

  await changeSelect(container, 'Category Filter', 'Professional Services');
  await changeSelect(container, 'Segment Filter', 'Legal Services');
  await changeSelect(container, 'Priority Tier Filter', 'Tier 1');
  await changeSelect(container, 'Verification Status Filter', 'verified');
  await changeSelect(container, 'Enrichment State Filter', 'partial');

  const html = container.innerHTML;
  expect(html).toContain('Smith & Jones LLP');
  expect(html).not.toContain('Atlas Services Group</td>');
  expect(html).not.toContain('Beacon Holdings</td>');

  root.unmount();
});

test('company detail renders seeded and enriched fields when data exists', async () => {
  const { container, root } = await renderView();

  const html = container.innerHTML;
  expect(html).toContain('Pilot verified subset company');
  expect(html).toContain('Key People');
  expect(html).toContain('Contact Intelligence');
  expect(html).toContain('Recent Signals');
  expect(html).toContain('Projects & Locations');
  expect(html).toContain('Source & Confidence');
  expect(html).toContain('first.last@beaconholdings.example');
  expect(html).toContain('2026-03-28T09:00:00Z');

  root.unmount();
});

test('partial and empty states render cleanly', async () => {
  const { container, root } = await renderView();

  await act(async () => {
    const row = Array.from(container.querySelectorAll('tr')).find((item) => item.textContent?.includes('Atlas Services Group')) as HTMLTableRowElement | undefined;
    row?.dispatchEvent(new MouseEvent('click', { bubbles: true }));
    await flush();
  });

  const html = container.innerHTML;
  expect(html).toContain('This company is seeded in Watchlist Mode, but enrichment has not been completed yet.');
  expect(html).toContain('No verified key people are available yet for this company.');
  expect(html).toContain('No public direct emails are currently stored for this company.');
  expect(html).toContain('No recent signals have been retained for this company yet.');
  expect(html).toContain('No recent projects are currently attached to this company.');
  expect(html).toContain('No source-backed links are available yet for this company.');

  root.unmount();
});

test('deterministic rendering is preserved', async () => {
  const first = await renderView();
  const firstHtml = first.container.innerHTML;
  first.root.unmount();

  const second = await renderView();
  expect(second.container.innerHTML).toBe(firstHtml);
  second.root.unmount();
});
