export type TodaySignal = {
  company: string;
  signal_type: 'audit' | 'government' | 'litigation' | 'project_distress';
  event_summary: string;
  source: string;
  date_detected: string;
  confidence_score: number;
  priority: 'HIGH' | 'MEDIUM';
  raw_text: string;
  recommended_action: string;
};

export type TodaySignalsSummary = {
  count_by_signal_type: {
    audit: number;
    government: number;
    litigation: number;
    project_distress: number;
  };
  count_by_priority: {
    HIGH: number;
    MEDIUM: number;
  };
  total_exported_count: number;
  top_companies: string[];
};

export type TodaySignalsPayload = {
  top_signals: TodaySignal[];
  retained_count: number;
  exported_count: number;
  run_date: string;
  status: string;
  csv_path: string;
  summary: TodaySignalsSummary;
};

export async function getTodaySignals(): Promise<TodaySignalsPayload> {
  const response = await fetch('/signals/today', {
    method: 'GET',
    headers: {
      Accept: 'application/json',
    },
  });

  if (!response.ok) {
    throw new Error('Failed to load today signals');
  }

  const payload = (await response.json()) as {
    ok?: boolean;
    today_signals?: TodaySignalsPayload;
    error_code?: string | null;
  };

  if (payload.ok !== true || !payload.today_signals) {
    throw new Error(payload.error_code ?? 'Invalid today signals payload');
  }

  return payload.today_signals;
}
