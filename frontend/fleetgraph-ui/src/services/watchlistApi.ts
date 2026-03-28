export type WatchlistPriorityTier = 'Tier 1' | 'Tier 2' | 'Tier 3';
export type WatchlistVerificationStatus = 'verified' | 'seed';
export type WatchlistEnrichmentState = 'seed_only' | 'partial' | 'enriched';
export type WatchlistConfidenceLevel = 'HIGH' | 'MEDIUM' | 'LOW';

export type WatchlistSeedCompany = {
  company_name: string;
  category: string;
  segment: string;
  priority_tier: WatchlistPriorityTier;
  website: string;
  hq_city: string;
  hq_state: string;
  hq_zip: string;
  phone: string;
  ceo_name: string;
  cfo_name: string;
  chief_risk_officer_name: string;
  verification_status: WatchlistVerificationStatus;
  notes: string;
};

export type WatchlistPerson = {
  name: string;
  title: string;
  source_url: string;
  confidence: WatchlistConfidenceLevel;
  basis: 'seed' | 'public';
};

export type WatchlistSignal = {
  title: string;
  signal_type: string;
  source_url: string;
  confidence: WatchlistConfidenceLevel;
  status: string;
};

export type WatchlistProject = {
  name: string;
  location: string;
  status: string;
  source_url: string;
  confidence: WatchlistConfidenceLevel;
};

export type WatchlistCompanyRecord = WatchlistSeedCompany & {
  main_phone: string;
  key_people: WatchlistPerson[];
  published_emails: string[];
  email_pattern_guess: string;
  recent_signals: WatchlistSignal[];
  recent_projects: WatchlistProject[];
  source_links: string[];
  last_enriched_at: string;
  confidence_level: WatchlistConfidenceLevel;
  enrichment_state: WatchlistEnrichmentState;
};

export type WatchlistFilterState = {
  category: string;
  segment: string;
  priority_tier: string;
  verification_status: string;
  enrichment_state: string;
};

const WATCHLIST_PILOT_DATA: WatchlistCompanyRecord[] = [
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
      {
        name: 'Avery Stone',
        title: 'Chief Risk Officer',
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
      'https://projects.example/midwest-fleet-refresh',
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

function cloneRecord(record: WatchlistCompanyRecord): WatchlistCompanyRecord {
  return {
    ...record,
    key_people: record.key_people.map((person) => ({ ...person })),
    published_emails: [...record.published_emails],
    recent_signals: record.recent_signals.map((signal) => ({ ...signal })),
    recent_projects: record.recent_projects.map((project) => ({ ...project })),
    source_links: [...record.source_links],
  };
}

function sortRecords(records: WatchlistCompanyRecord[]): WatchlistCompanyRecord[] {
  const priorityOrder: Record<WatchlistPriorityTier, number> = {
    'Tier 1': 0,
    'Tier 2': 1,
    'Tier 3': 2,
  };

  return [...records].sort((a, b) => {
    const priorityDelta = priorityOrder[a.priority_tier] - priorityOrder[b.priority_tier];
    if (priorityDelta !== 0) {
      return priorityDelta;
    }
    if (a.company_name !== b.company_name) {
      return a.company_name.localeCompare(b.company_name);
    }
    return a.segment.localeCompare(b.segment);
  });
}

export async function getWatchlistPilotCompanies(): Promise<WatchlistCompanyRecord[]> {
  return sortRecords(WATCHLIST_PILOT_DATA.map(cloneRecord));
}

export function filterWatchlistCompanies(
  companies: WatchlistCompanyRecord[],
  filters: WatchlistFilterState,
): WatchlistCompanyRecord[] {
  return sortRecords(
    companies.filter((company) => {
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
    }),
  ).map(cloneRecord);
}
