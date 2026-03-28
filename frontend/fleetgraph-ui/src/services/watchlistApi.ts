import { API_BASE } from './api';

export type WatchlistPriorityTier = 'Tier 1' | 'Tier 2' | 'Tier 3';
export type WatchlistVerificationStatus = 'verified' | 'seed';
export type WatchlistEnrichmentState = 'seed_only' | 'partial' | 'enriched';
export type WatchlistConfidenceLevel = 'HIGH' | 'MEDIUM' | 'LOW';
export type WatchlistRefreshStatus = 'idle' | 'refreshing' | 'refresh_succeeded' | 'refresh_failed';
export type WatchlistPriorityBand = 'HIGH' | 'MEDIUM' | 'LOW';
export type ContactConfidenceLevel = 'high' | 'medium' | 'low';
export type ContactType = 'direct_email' | 'general_email' | 'phone' | 'page' | 'pattern';
export type WatchlistOutreachStatus = 'not_ready' | 'ready_to_draft' | 'drafted' | 'suppressed';

export type WatchlistSeedCompany = {
  company_id: string;
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

export type WatchlistEmailContact = {
  email: string;
  source_url: string;
  confidence: ContactConfidenceLevel;
  type: 'direct_email' | 'general_email';
  is_direct: boolean;
};

export type WatchlistPhoneContact = {
  phone: string;
  source_url: string;
  confidence: ContactConfidenceLevel;
};

export type WatchlistOutreachRecord = {
  company_id: string;
  company_name: string;
  contact_name: string;
  contact_email: string;
  contact_phone: string;
  contact_type: 'direct_email' | 'general_email' | 'phone' | 'none';
  target_role_guess: string;
  signal_summary: string;
  why_now: string;
  why_this_company: string;
  subject_line: string;
  email_body: string;
  source_links: string[];
  outreach_status: WatchlistOutreachStatus;
  qualification_reasons: string[];
  readiness_state: 'not_ready' | 'ready_to_draft';
  draft_generated_at: string;
};

export type WatchlistOutreachQueueItem = {
  company_id: string;
  company_name: string;
  contact_name: string;
  contact_email: string;
  outreach_status: WatchlistOutreachStatus;
  readiness_state: 'not_ready' | 'ready_to_draft';
};

export type WatchlistCompanyRecord = WatchlistSeedCompany & {
  main_phone: string;
  direct_phones: WatchlistPhoneContact[];
  general_emails: WatchlistEmailContact[];
  key_people: WatchlistPerson[];
  published_emails: WatchlistEmailContact[];
  contact_pages: string[];
  leadership_pages: string[];
  address_lines: string[];
  contact_sources: string[];
  email_pattern_guess: string;
  recent_signals: WatchlistSignal[];
  recent_projects: WatchlistProject[];
  source_links: string[];
  last_enriched_at: string;
  confidence_level: WatchlistConfidenceLevel;
  contact_confidence_level: ContactConfidenceLevel;
  reachability_score: number;
  enrichment_state: WatchlistEnrichmentState;
  delta_summary?: WatchlistDeltaSummary;
  priority_summary?: WatchlistPrioritySummary;
  outreach_record?: WatchlistOutreachRecord;
};

export type WatchlistDeltaSummary = {
  company_id: string;
  company_name: string;
  change_detected: boolean;
  change_types: string[];
  previous_enrichment_state: string;
  current_enrichment_state: string;
  new_signal_count: number;
  new_project_count: number;
  new_email_count: number;
  new_key_people_count: number;
  confidence_changed: boolean;
  last_enriched_at: string;
  priority_score: number;
  priority_reason_codes: string[];
};

export type WatchlistPrioritySummary = {
  company_id: string;
  company_name: string;
  priority_score: number;
  priority_band: WatchlistPriorityBand;
  priority_reason_codes: string[];
  changed_since_last_run: boolean;
  needs_review: boolean;
  needs_review_reasons: string[];
};

export type WatchlistChangedCompany = WatchlistDeltaSummary;

export type WatchlistTopTarget = {
  company_id: string;
  company_name: string;
  priority_score: number;
  priority_band: WatchlistPriorityBand;
  reason_summary: string;
  current_enrichment_state: string;
  change_detected: boolean;
  priority_reason_codes: string[];
  reachability_score?: number;
  contact_confidence_level?: ContactConfidenceLevel;
};

export type WatchlistNeedsReviewItem = {
  company_id: string;
  company_name: string;
  review_reason_summary: string;
  current_enrichment_state: string;
  priority_score: number;
  last_enriched_at: string;
};

export type WatchlistFilterState = {
  category: string;
  segment: string;
  priority_tier: string;
  verification_status: string;
  enrichment_state: string;
};

type WatchlistCollectionResponse = {
  ok?: boolean;
  companies?: unknown;
  error_code?: string | null;
};

type WatchlistDetailResponse = {
  ok?: boolean;
  company?: unknown;
  error_code?: string | null;
};

type WatchlistRefreshResponse = {
  ok?: boolean;
  company?: unknown;
  refresh_status?: string;
  error_code?: string | null;
};

type WatchlistChangedCompaniesResponse = {
  ok?: boolean;
  changed_companies?: unknown;
  error_code?: string | null;
};

type WatchlistTopTargetsResponse = {
  ok?: boolean;
  top_targets?: unknown;
  error_code?: string | null;
};

type WatchlistNeedsReviewResponse = {
  ok?: boolean;
  needs_review?: unknown;
  error_code?: string | null;
};

type WatchlistOutreachQueueResponse = {
  ok?: boolean;
  outreach_queue?: unknown;
  error_code?: string | null;
};

type WatchlistOutreachStatusResponse = {
  ok?: boolean;
  outreach_record?: unknown;
  error_code?: string | null;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function isString(value: unknown): value is string {
  return typeof value === 'string';
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === 'string');
}

function isConfidenceLevel(value: unknown): value is WatchlistConfidenceLevel {
  return value === 'HIGH' || value === 'MEDIUM' || value === 'LOW';
}

function isContactConfidenceLevel(value: unknown): value is ContactConfidenceLevel {
  return value === 'high' || value === 'medium' || value === 'low';
}

function isPriorityTier(value: unknown): value is WatchlistPriorityTier {
  return value === 'Tier 1' || value === 'Tier 2' || value === 'Tier 3';
}

function isVerificationStatus(value: unknown): value is WatchlistVerificationStatus {
  return value === 'verified' || value === 'seed';
}

function isEnrichmentState(value: unknown): value is WatchlistEnrichmentState {
  return value === 'seed_only' || value === 'partial' || value === 'enriched';
}

function isPriorityBand(value: unknown): value is WatchlistPriorityBand {
  return value === 'HIGH' || value === 'MEDIUM' || value === 'LOW';
}

function isOutreachStatus(value: unknown): value is WatchlistOutreachStatus {
  return value === 'not_ready' || value === 'ready_to_draft' || value === 'drafted' || value === 'suppressed';
}

function isNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

function parsePerson(value: unknown): WatchlistPerson {
  if (!isRecord(value)) {
    throw new Error('Invalid watchlist person');
  }
  if (
    !isString(value.name)
    || !isString(value.title)
    || !isString(value.source_url)
    || !isConfidenceLevel(value.confidence)
    || (value.basis !== 'seed' && value.basis !== 'public')
  ) {
    throw new Error('Invalid watchlist person');
  }

  return {
    name: value.name,
    title: value.title,
    source_url: value.source_url,
    confidence: value.confidence,
    basis: value.basis,
  };
}

function parseSignal(value: unknown): WatchlistSignal {
  if (!isRecord(value)) {
    throw new Error('Invalid watchlist signal');
  }
  if (
    !isString(value.title)
    || !isString(value.signal_type)
    || !isString(value.source_url)
    || !isConfidenceLevel(value.confidence)
    || !isString(value.status)
  ) {
    throw new Error('Invalid watchlist signal');
  }

  return {
    title: value.title,
    signal_type: value.signal_type,
    source_url: value.source_url,
    confidence: value.confidence,
    status: value.status,
  };
}

function parseProject(value: unknown): WatchlistProject {
  if (!isRecord(value)) {
    throw new Error('Invalid watchlist project');
  }
  if (
    !isString(value.name)
    || !isString(value.location)
    || !isString(value.status)
    || !isString(value.source_url)
    || !isConfidenceLevel(value.confidence)
  ) {
    throw new Error('Invalid watchlist project');
  }

  return {
    name: value.name,
    location: value.location,
    status: value.status,
    source_url: value.source_url,
    confidence: value.confidence,
  };
}

function parseEmailContact(value: unknown, allowedType: 'direct_email' | 'general_email'): WatchlistEmailContact {
  if (!isRecord(value)) {
    throw new Error('Invalid email contact');
  }
  if (
    !isString(value.email)
    || !isString(value.source_url)
    || !isContactConfidenceLevel(value.confidence)
    || value.type !== allowedType
    || typeof value.is_direct !== 'boolean'
  ) {
    throw new Error('Invalid email contact');
  }

  return {
    email: value.email,
    source_url: value.source_url,
    confidence: value.confidence,
    type: value.type,
    is_direct: value.is_direct,
  };
}

function parsePhoneContact(value: unknown): WatchlistPhoneContact {
  if (!isRecord(value)) {
    throw new Error('Invalid phone contact');
  }
  if (
    !isString(value.phone)
    || !isString(value.source_url)
    || !isContactConfidenceLevel(value.confidence)
  ) {
    throw new Error('Invalid phone contact');
  }

  return {
    phone: value.phone,
    source_url: value.source_url,
    confidence: value.confidence,
  };
}

function cloneRecord(record: WatchlistCompanyRecord): WatchlistCompanyRecord {
  return {
    ...record,
    direct_phones: record.direct_phones.map((phone) => ({ ...phone })),
    general_emails: record.general_emails.map((email) => ({ ...email })),
    key_people: record.key_people.map((person) => ({ ...person })),
    published_emails: record.published_emails.map((email) => ({ ...email })),
    contact_pages: [...record.contact_pages],
    leadership_pages: [...record.leadership_pages],
    address_lines: [...record.address_lines],
    contact_sources: [...record.contact_sources],
    recent_signals: record.recent_signals.map((signal) => ({ ...signal })),
    recent_projects: record.recent_projects.map((project) => ({ ...project })),
    source_links: [...record.source_links],
    delta_summary: record.delta_summary ? {
      ...record.delta_summary,
      change_types: [...record.delta_summary.change_types],
      priority_reason_codes: [...record.delta_summary.priority_reason_codes],
    } : undefined,
    priority_summary: record.priority_summary ? {
      ...record.priority_summary,
      priority_reason_codes: [...record.priority_summary.priority_reason_codes],
      needs_review_reasons: [...record.priority_summary.needs_review_reasons],
    } : undefined,
    outreach_record: record.outreach_record ? {
      ...record.outreach_record,
      source_links: [...record.outreach_record.source_links],
      qualification_reasons: [...record.outreach_record.qualification_reasons],
    } : undefined,
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
    return a.company_id.localeCompare(b.company_id);
  });
}

function parseCompanyRecord(value: unknown): WatchlistCompanyRecord {
  if (!isRecord(value)) {
    throw new Error('Invalid watchlist company');
  }

  if (
    !isString(value.company_id)
    || !isString(value.company_name)
    || !isString(value.category)
    || !isString(value.segment)
    || !isPriorityTier(value.priority_tier)
    || !isString(value.website)
    || !isString(value.hq_city)
    || !isString(value.hq_state)
    || !isString(value.hq_zip)
    || !isString(value.phone)
    || !isString(value.ceo_name)
    || !isString(value.cfo_name)
    || !isString(value.chief_risk_officer_name)
    || !isVerificationStatus(value.verification_status)
    || !isString(value.notes)
    || !isString(value.main_phone)
    || !Array.isArray(value.direct_phones)
    || !Array.isArray(value.general_emails)
    || !Array.isArray(value.key_people)
    || !Array.isArray(value.published_emails)
    || !Array.isArray(value.contact_pages)
    || !Array.isArray(value.leadership_pages)
    || !Array.isArray(value.address_lines)
    || !Array.isArray(value.contact_sources)
    || !isString(value.email_pattern_guess)
    || !Array.isArray(value.recent_signals)
    || !Array.isArray(value.recent_projects)
    || !Array.isArray(value.source_links)
    || !isString(value.last_enriched_at)
    || !isConfidenceLevel(value.confidence_level)
    || !isContactConfidenceLevel(value.contact_confidence_level)
    || !isNumber(value.reachability_score)
    || !isEnrichmentState(value.enrichment_state)
  ) {
    throw new Error('Invalid watchlist company');
  }

  return {
    company_id: value.company_id,
    company_name: value.company_name,
    category: value.category,
    segment: value.segment,
    priority_tier: value.priority_tier,
    website: value.website,
    hq_city: value.hq_city,
    hq_state: value.hq_state,
    hq_zip: value.hq_zip,
    phone: value.phone,
    ceo_name: value.ceo_name,
    cfo_name: value.cfo_name,
    chief_risk_officer_name: value.chief_risk_officer_name,
    verification_status: value.verification_status,
    notes: value.notes,
    main_phone: value.main_phone,
    direct_phones: value.direct_phones.map((phone) => parsePhoneContact(phone)),
    general_emails: value.general_emails.map((email) => parseEmailContact(email, 'general_email')),
    key_people: value.key_people.map((person) => parsePerson(person)),
    published_emails: value.published_emails.map((email) => parseEmailContact(email, 'direct_email')),
    contact_pages: [...value.contact_pages],
    leadership_pages: [...value.leadership_pages],
    address_lines: [...value.address_lines],
    contact_sources: [...value.contact_sources],
    email_pattern_guess: value.email_pattern_guess,
    recent_signals: value.recent_signals.map((signal) => parseSignal(signal)),
    recent_projects: value.recent_projects.map((project) => parseProject(project)),
    source_links: [...value.source_links],
    last_enriched_at: value.last_enriched_at,
    confidence_level: value.confidence_level,
    contact_confidence_level: value.contact_confidence_level,
    reachability_score: value.reachability_score,
    enrichment_state: value.enrichment_state,
    delta_summary: value.delta_summary === undefined ? undefined : parseDeltaSummary(value.delta_summary),
    priority_summary: value.priority_summary === undefined ? undefined : parsePrioritySummary(value.priority_summary),
    outreach_record: value.outreach_record === undefined ? undefined : parseOutreachRecord(value.outreach_record),
  };
}

function parseDeltaSummary(value: unknown): WatchlistDeltaSummary {
  if (!isRecord(value)) {
    throw new Error('Invalid watchlist delta summary');
  }
  if (
    !isString(value.company_id)
    || !isString(value.company_name)
    || typeof value.change_detected !== 'boolean'
    || !isStringArray(value.change_types)
    || !isString(value.previous_enrichment_state)
    || !isString(value.current_enrichment_state)
    || !isNumber(value.new_signal_count)
    || !isNumber(value.new_project_count)
    || !isNumber(value.new_email_count)
    || !isNumber(value.new_key_people_count)
    || typeof value.confidence_changed !== 'boolean'
    || !isString(value.last_enriched_at)
    || !isNumber(value.priority_score)
    || !isStringArray(value.priority_reason_codes)
  ) {
    throw new Error('Invalid watchlist delta summary');
  }

  return {
    company_id: value.company_id,
    company_name: value.company_name,
    change_detected: value.change_detected,
    change_types: [...value.change_types],
    previous_enrichment_state: value.previous_enrichment_state,
    current_enrichment_state: value.current_enrichment_state,
    new_signal_count: value.new_signal_count,
    new_project_count: value.new_project_count,
    new_email_count: value.new_email_count,
    new_key_people_count: value.new_key_people_count,
    confidence_changed: value.confidence_changed,
    last_enriched_at: value.last_enriched_at,
    priority_score: value.priority_score,
    priority_reason_codes: [...value.priority_reason_codes],
  };
}

function parsePrioritySummary(value: unknown): WatchlistPrioritySummary {
  if (!isRecord(value)) {
    throw new Error('Invalid watchlist priority summary');
  }
  if (
    !isString(value.company_id)
    || !isString(value.company_name)
    || !isNumber(value.priority_score)
    || !isPriorityBand(value.priority_band)
    || !isStringArray(value.priority_reason_codes)
    || typeof value.changed_since_last_run !== 'boolean'
    || typeof value.needs_review !== 'boolean'
    || !isStringArray(value.needs_review_reasons)
  ) {
    throw new Error('Invalid watchlist priority summary');
  }

  return {
    company_id: value.company_id,
    company_name: value.company_name,
    priority_score: value.priority_score,
    priority_band: value.priority_band,
    priority_reason_codes: [...value.priority_reason_codes],
    changed_since_last_run: value.changed_since_last_run,
    needs_review: value.needs_review,
    needs_review_reasons: [...value.needs_review_reasons],
  };
}

function parseTopTarget(value: unknown): WatchlistTopTarget {
  if (!isRecord(value)) {
    throw new Error('Invalid top target');
  }
  if (
    !isString(value.company_id)
    || !isString(value.company_name)
    || !isNumber(value.priority_score)
    || !isPriorityBand(value.priority_band)
    || !isString(value.reason_summary)
    || !isString(value.current_enrichment_state)
    || typeof value.change_detected !== 'boolean'
    || !isStringArray(value.priority_reason_codes)
  ) {
    throw new Error('Invalid top target');
  }

  return {
    company_id: value.company_id,
    company_name: value.company_name,
    priority_score: value.priority_score,
    priority_band: value.priority_band,
    reason_summary: value.reason_summary,
    current_enrichment_state: value.current_enrichment_state,
    change_detected: value.change_detected,
    priority_reason_codes: [...value.priority_reason_codes],
    reachability_score: isNumber(value.reachability_score) ? value.reachability_score : undefined,
    contact_confidence_level: isContactConfidenceLevel(value.contact_confidence_level) ? value.contact_confidence_level : undefined,
  };
}

function parseNeedsReviewItem(value: unknown): WatchlistNeedsReviewItem {
  if (!isRecord(value)) {
    throw new Error('Invalid needs review item');
  }
  if (
    !isString(value.company_id)
    || !isString(value.company_name)
    || !isString(value.review_reason_summary)
    || !isString(value.current_enrichment_state)
    || !isNumber(value.priority_score)
    || !isString(value.last_enriched_at)
  ) {
    throw new Error('Invalid needs review item');
  }

  return {
    company_id: value.company_id,
    company_name: value.company_name,
    review_reason_summary: value.review_reason_summary,
    current_enrichment_state: value.current_enrichment_state,
    priority_score: value.priority_score,
    last_enriched_at: value.last_enriched_at,
  };
}

function parseOutreachRecord(value: unknown): WatchlistOutreachRecord {
  if (!isRecord(value)) {
    throw new Error('Invalid outreach record');
  }
  if (
    !isString(value.company_id)
    || !isString(value.company_name)
    || !isString(value.contact_name)
    || !isString(value.contact_email)
    || !isString(value.contact_phone)
    || (value.contact_type !== 'direct_email' && value.contact_type !== 'general_email' && value.contact_type !== 'phone' && value.contact_type !== 'none')
    || !isString(value.target_role_guess)
    || !isString(value.signal_summary)
    || !isString(value.why_now)
    || !isString(value.why_this_company)
    || !isString(value.subject_line)
    || !isString(value.email_body)
    || !isStringArray(value.source_links)
    || !isOutreachStatus(value.outreach_status)
    || !isStringArray(value.qualification_reasons)
    || (value.readiness_state !== 'not_ready' && value.readiness_state !== 'ready_to_draft')
    || !isString(value.draft_generated_at)
  ) {
    throw new Error('Invalid outreach record');
  }

  return {
    company_id: value.company_id,
    company_name: value.company_name,
    contact_name: value.contact_name,
    contact_email: value.contact_email,
    contact_phone: value.contact_phone,
    contact_type: value.contact_type,
    target_role_guess: value.target_role_guess,
    signal_summary: value.signal_summary,
    why_now: value.why_now,
    why_this_company: value.why_this_company,
    subject_line: value.subject_line,
    email_body: value.email_body,
    source_links: [...value.source_links],
    outreach_status: value.outreach_status,
    qualification_reasons: [...value.qualification_reasons],
    readiness_state: value.readiness_state,
    draft_generated_at: value.draft_generated_at,
  };
}

function parseOutreachQueueItem(value: unknown): WatchlistOutreachQueueItem {
  if (!isRecord(value)) {
    throw new Error('Invalid outreach queue item');
  }
  if (
    !isString(value.company_id)
    || !isString(value.company_name)
    || !isString(value.contact_name)
    || !isString(value.contact_email)
    || !isOutreachStatus(value.outreach_status)
    || (value.readiness_state !== 'not_ready' && value.readiness_state !== 'ready_to_draft')
  ) {
    throw new Error('Invalid outreach queue item');
  }

  return {
    company_id: value.company_id,
    company_name: value.company_name,
    contact_name: value.contact_name,
    contact_email: value.contact_email,
    outreach_status: value.outreach_status,
    readiness_state: value.readiness_state,
  };
}

async function requestJson(path: string, init?: RequestInit): Promise<unknown> {
  const response = await fetch(`${API_BASE}${path}`, init);
  const payload = await response.json().catch(() => null);

  if (!response.ok) {
    const message = isRecord(payload) && isString(payload.error_code)
      ? payload.error_code
      : `Request failed with status ${response.status}`;
    throw new Error(message);
  }

  return payload;
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

export async function getWatchlistCompanies(): Promise<WatchlistCompanyRecord[]> {
  const payload = await requestJson('/watchlist/companies');
  const response = payload as WatchlistCollectionResponse;

  if (response.ok !== true || !Array.isArray(response.companies)) {
    throw new Error(response.error_code ?? 'Invalid watchlist collection payload');
  }

  return sortRecords(response.companies.map((company) => parseCompanyRecord(company))).map(cloneRecord);
}

export async function getWatchlistCompanyDetail(companyId: string): Promise<WatchlistCompanyRecord> {
  const payload = await requestJson(`/watchlist/companies/${encodeURIComponent(companyId)}`);
  const response = payload as WatchlistDetailResponse;

  if (response.ok !== true) {
    throw new Error(response.error_code ?? 'Invalid watchlist detail payload');
  }

  return cloneRecord(parseCompanyRecord(response.company));
}

export async function refreshWatchlistCompany(companyId: string): Promise<WatchlistCompanyRecord> {
  const payload = await requestJson(`/watchlist/companies/${encodeURIComponent(companyId)}/refresh`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({}),
  });
  const response = payload as WatchlistRefreshResponse;

  if (response.ok !== true || response.refresh_status !== 'refresh_succeeded') {
    throw new Error(response.error_code ?? 'Watchlist refresh failed');
  }

  return cloneRecord(parseCompanyRecord(response.company));
}

export async function getWatchlistChangedCompanies(): Promise<WatchlistChangedCompany[]> {
  const payload = await requestJson('/watchlist/changed-companies');
  const response = payload as WatchlistChangedCompaniesResponse;

  if (response.ok !== true || !Array.isArray(response.changed_companies)) {
    throw new Error(response.error_code ?? 'Invalid changed companies payload');
  }

  return response.changed_companies.map((item) => parseDeltaSummary(item));
}

export async function getWatchlistTopTargets(): Promise<WatchlistTopTarget[]> {
  const payload = await requestJson('/watchlist/top-targets');
  const response = payload as WatchlistTopTargetsResponse;

  if (response.ok !== true || !Array.isArray(response.top_targets)) {
    throw new Error(response.error_code ?? 'Invalid top targets payload');
  }

  return response.top_targets.map((item) => parseTopTarget(item));
}

export async function getWatchlistNeedsReview(): Promise<WatchlistNeedsReviewItem[]> {
  const payload = await requestJson('/watchlist/needs-review');
  const response = payload as WatchlistNeedsReviewResponse;

  if (response.ok !== true || !Array.isArray(response.needs_review)) {
    throw new Error(response.error_code ?? 'Invalid needs review payload');
  }

  return response.needs_review.map((item) => parseNeedsReviewItem(item));
}

export async function getWatchlistOutreachQueue(): Promise<WatchlistOutreachQueueItem[]> {
  const payload = await requestJson('/watchlist/outreach-queue');
  const response = payload as WatchlistOutreachQueueResponse;

  if (response.ok !== true || !Array.isArray(response.outreach_queue)) {
    throw new Error(response.error_code ?? 'Invalid outreach queue payload');
  }

  return response.outreach_queue.map((item) => parseOutreachQueueItem(item));
}

export async function updateWatchlistOutreachStatus(
  companyId: string,
  outreachStatus: Extract<WatchlistOutreachStatus, 'drafted' | 'suppressed'>,
): Promise<WatchlistOutreachRecord> {
  const payload = await requestJson(`/watchlist/companies/${encodeURIComponent(companyId)}/outreach-status`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ outreach_status: outreachStatus }),
  });
  const response = payload as WatchlistOutreachStatusResponse;

  if (response.ok !== true) {
    throw new Error(response.error_code ?? 'Watchlist outreach status update failed');
  }

  return parseOutreachRecord(response.outreach_record);
}
