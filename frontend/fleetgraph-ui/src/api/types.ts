export type ApiPayload = Record<string, unknown>;

export type DashboardSummaryResponse = {
  total_records: number;
  high_priority_count: number;
  medium_priority_count: number;
  low_priority_count: number;
};

export type DashboardRecordResponse = {
  record_id?: string;
  company_name?: string;
  opportunity_title?: string;
  priority_score?: number;
  stage?: string;
  top_icp?: string;
  icp?: string;
  priority?: string;
  opportunity_score?: number;
  reason?: string;
};

export type PriorityDashboardResponse = {
  company_id: string;
  dashboard_summary: DashboardSummaryResponse;
  records: DashboardRecordResponse[];
};