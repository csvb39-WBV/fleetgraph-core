import React, { useEffect, useMemo, useState } from 'react';

import CompanyDetailConsole from '../components/watchlist/CompanyDetailConsole';
import WatchlistCompanyTable from '../components/watchlist/WatchlistCompanyTable';
import WatchlistFilters from '../components/watchlist/WatchlistFilters';
import WatchlistModeBanner from '../components/watchlist/WatchlistModeBanner';
import WatchlistSummaryCards from '../components/watchlist/WatchlistSummaryCards';
import {
  filterWatchlistCompanies,
  getWatchlistCompanies,
  getWatchlistCompanyDetail,
  refreshWatchlistCompany,
  type WatchlistCompanyRecord,
  type WatchlistFilterState,
  type WatchlistRefreshStatus,
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
  const [selectedCompanyId, setSelectedCompanyId] = useState<string | null>(null);
  const [selectedCompanyDetail, setSelectedCompanyDetail] = useState<WatchlistCompanyRecord | null>(null);
  const [refreshStatus, setRefreshStatus] = useState<WatchlistRefreshStatus>('idle');
  const [refreshErrorMessage, setRefreshErrorMessage] = useState('');
  const [loadErrorMessage, setLoadErrorMessage] = useState('');

  useEffect(() => {
    let cancelled = false;

    getWatchlistCompanies()
      .then((records) => {
        if (cancelled) {
          return;
        }
        setCompanies(records);
        setSelectedCompanyId(records.length > 0 ? records[0].company_id : null);
        setLoadErrorMessage('');
      })
      .catch((error) => {
        if (cancelled) {
          return;
        }
        setCompanies([]);
        setSelectedCompanyId(null);
        setSelectedCompanyDetail(null);
        setLoadErrorMessage(error instanceof Error ? error.message : 'Failed to load watchlist companies');
      });

    return () => {
      cancelled = true;
    };
  }, []);

  const filteredCompanies = useMemo(() => filterWatchlistCompanies(companies, filters), [companies, filters]);

  useEffect(() => {
    if (filteredCompanies.length === 0) {
      setSelectedCompanyId(null);
      setSelectedCompanyDetail(null);
      return;
    }

    const stillVisible = filteredCompanies.some((company) => company.company_id === selectedCompanyId);
    if (!stillVisible) {
      setSelectedCompanyId(filteredCompanies[0].company_id);
    }
  }, [filteredCompanies, selectedCompanyId]);

  useEffect(() => {
    let cancelled = false;

    if (!selectedCompanyId) {
      setSelectedCompanyDetail(null);
      return;
    }

    getWatchlistCompanyDetail(selectedCompanyId)
      .then((companyDetail) => {
        if (cancelled) {
          return;
        }
        setSelectedCompanyDetail(companyDetail);
      })
      .catch(() => {
        if (cancelled) {
          return;
        }
        const fallbackRecord = filteredCompanies.find((company) => company.company_id === selectedCompanyId) ?? null;
        setSelectedCompanyDetail(fallbackRecord);
      });

    return () => {
      cancelled = true;
    };
  }, [filteredCompanies, selectedCompanyId]);

  useEffect(() => {
    setRefreshStatus('idle');
    setRefreshErrorMessage('');
  }, [selectedCompanyId]);

  const selectedCompany = useMemo(() => {
    if (!selectedCompanyId) {
      return null;
    }
    if (selectedCompanyDetail && selectedCompanyDetail.company_id === selectedCompanyId) {
      return selectedCompanyDetail;
    }
    return filteredCompanies.find((company) => company.company_id === selectedCompanyId) ?? null;
  }, [filteredCompanies, selectedCompanyDetail, selectedCompanyId]);

  const categories = useMemo(() => uniqueValues(companies.map((company) => company.category)), [companies]);
  const segments = useMemo(() => uniqueValues(companies.map((company) => company.segment)), [companies]);
  const priorityTiers = useMemo(() => uniqueValues(companies.map((company) => company.priority_tier)), [companies]);
  const verificationStatuses = useMemo(() => uniqueValues(companies.map((company) => company.verification_status)), [companies]);
  const enrichmentStates = useMemo(() => uniqueValues(companies.map((company) => company.enrichment_state)), [companies]);

  async function handleRefreshSelectedCompany(): Promise<void> {
    if (!selectedCompanyId) {
      return;
    }

    setRefreshStatus('refreshing');
    setRefreshErrorMessage('');

    try {
      const refreshedCompany = await refreshWatchlistCompany(selectedCompanyId);
      setCompanies((currentCompanies) => currentCompanies.map((company) => (
        company.company_id === refreshedCompany.company_id ? refreshedCompany : company
      )));
      setSelectedCompanyDetail(refreshedCompany);
      setRefreshStatus('refresh_succeeded');
    } catch (error) {
      setRefreshStatus('refresh_failed');
      setRefreshErrorMessage(error instanceof Error ? error.message : 'Watchlist refresh failed');
    }
  }

  if (loadErrorMessage) {
    return (
      <section aria-label="Watchlist Console View" style={{ display: 'grid', gap: '16px' }}>
        <WatchlistModeBanner />
        <section aria-label="Watchlist Load Error State" style={{ border: '1px solid #d9e2ec', borderRadius: '16px', background: '#ffffff', padding: '20px' }}>
          Failed to load watchlist companies: {loadErrorMessage}
        </section>
      </section>
    );
  }

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
            selectedCompanyId={selectedCompanyId}
            onSelectCompany={setSelectedCompanyId}
          />
          <CompanyDetailConsole
            company={selectedCompany}
            refreshStatus={refreshStatus}
            refreshErrorMessage={refreshErrorMessage}
            onRefresh={handleRefreshSelectedCompany}
          />
        </section>
      )}
    </section>
  );
}

export default WatchlistConsoleView;
