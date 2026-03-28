import { expect, test } from 'vitest';

import { filterWatchlistCompanies, getWatchlistPilotCompanies } from './watchlistApi';

test('pilot companies load deterministically', async () => {
  const first = await getWatchlistPilotCompanies();
  const second = await getWatchlistPilotCompanies();

  expect(first).toEqual(second);
  expect(first.map((company) => company.company_name)).toEqual([
    'Beacon Holdings',
    'Smith & Jones LLP',
    'Atlas Services Group',
  ]);
});

test('filters apply deterministically', async () => {
  const companies = await getWatchlistPilotCompanies();

  const filtered = filterWatchlistCompanies(companies, {
    category: 'Professional Services',
    segment: 'Legal Services',
    priority_tier: 'Tier 1',
    verification_status: 'verified',
    enrichment_state: 'partial',
  });

  expect(filtered.map((company) => company.company_name)).toEqual(['Smith & Jones LLP']);
});
