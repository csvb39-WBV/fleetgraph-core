import React, { useEffect, useMemo, useState } from 'react';

import CompanyDetailConsole from '../components/watchlist/CompanyDetailConsole';
import WatchlistCompanyTable from '../components/watchlist/WatchlistCompanyTable';
import WatchlistFilters from '../components/watchlist/WatchlistFilters';
import WatchlistModeBanner from '../components/watchlist/WatchlistModeBanner';
import WatchlistSummaryCards from '../components/watchlist/WatchlistSummaryCards';
import {
  filterWatchlistCompanies,
  getWatchlistPilotCompanies,
  type WatchlistCompanyRecord,
  type WatchlistFilterState,
} from '../services/watchlistApi';

const EMPTY_FILTERS: WatchlistFilterState = {
  category: '',
  segment: '',
  priority_tier: '',
  verification_status: '',
  enrichment_state: '',
};

function uniqueValues(values: string[]): string[] {
  return [...new Set(values)].sort((left, right) => left.localeCompare(right));
}

export function WatchlistConsoleView(): JSX.Element {
  const [companies, setCompanies] = useState<WatchlistCompanyRecord[]>([]);
  const [filters, setFilters] = useState<WatchlistFilterState>(EMPTY_FILTERS);
  const [selectedCompanyName, setSelectedCompanyName] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    getWatchlistPilotCompanies().then((records) => {
      if (cancelled) {
        return;
      }
      setCompanies(records);
      setSelectedCompanyName(records.length > 0 ? records[0].company_name : null);
    });

    return () => {
      cancelled = true;
    };
  }, []);

  const filteredCompanies = useMemo(() => filterWatchlistCompanies(companies, filters), [companies, filters]);

  useEffect(() => {
    if (filteredCompanies.length === 0) {
      setSelectedCompanyName(null);
      return;
    }

    const stillVisible = filteredCompanies.some((company) => company.company_name === selectedCompanyName);
    if (!stillVisible) {
      setSelectedCompanyName(filteredCompanies[0].company_name);
    }
  }, [filteredCompanies, selectedCompanyName]);

  const selectedCompany = useMemo(
    () => filteredCompanies.find((company) => company.company_name === selectedCompanyName) ?? null,
    [filteredCompanies, selectedCompanyName],
  );

  const categories = useMemo(() => uniqueValues(companies.map((company) => company.category)), [companies]);
  const segments = useMemo(() => uniqueValues(companies.map((company) => company.segment)), [companies]);
  const priorityTiers = useMemo(() => uniqueValues(companies.map((company) => company.priority_tier)), [companies]);
  const verificationStatuses = useMemo(() => uniqueValues(companies.map((company) => company.verification_status)), [companies]);
  const enrichmentStates = useMemo(() => uniqueValues(companies.map((company) => company.enrichment_state)), [companies]);

  return (
    <section aria-label="Watchlist Console View" style={{ display: 'grid', gap: '16px' }}>
      <WatchlistModeBanner />
      <WatchlistSummaryCards companies={filteredCompanies.length > 0 ? filteredCompanies : companies} />
      <WatchlistFilters
        filters={filters}
        categories={categories}
        segments={segments}
        priorityTiers={priorityTiers}
        verificationStatuses={verificationStatuses}
        enrichmentStates={enrichmentStates}
        onChange={setFilters}
      />
      {filteredCompanies.length === 0 ? (
        <section aria-label="Watchlist Empty State" style={{ border: '1px solid #d9e2ec', borderRadius: '16px', background: '#ffffff', padding: '20px' }}>
          No watchlist companies match the current filters.
        </section>
      ) : (
        <section style={{ display: 'grid', gridTemplateColumns: 'minmax(0, 1.2fr) minmax(340px, 1fr)', gap: '16px', alignItems: 'start' }}>
          <WatchlistCompanyTable
            companies={filteredCompanies}
            selectedCompanyName={selectedCompanyName}
            onSelectCompany={setSelectedCompanyName}
          />
          <CompanyDetailConsole company={selectedCompany} />
        </section>
      )}
    </section>
  );
}

export default WatchlistConsoleView;
