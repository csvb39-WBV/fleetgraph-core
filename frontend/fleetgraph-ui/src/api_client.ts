export type ApiPayload = Record<string, unknown>;
export type ApiResponse = Record<string, unknown>;

export type ApiAdapter = (
  endpoint: string,
  payload: ApiPayload
) => Promise<ApiResponse>;

let _adapter: ApiAdapter = async (
  endpoint: string,
  payload: ApiPayload
): Promise<ApiResponse> => {
  const response = await fetch(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    throw new Error(`API error: ${response.status} ${response.statusText}`);
  }
  return response.json() as Promise<ApiResponse>;
};

export function setAdapter(adapter: ApiAdapter): void {
  _adapter = adapter;
}

export function getPriorityDashboard(payload: ApiPayload): Promise<ApiResponse> {
  return _adapter("/api/priority-dashboard", payload);
}

export function getCompanyIntelligence(payload: ApiPayload): Promise<ApiResponse> {
  return _adapter("/api/company-intelligence", payload);
}

export function getPredictiveInsights(payload: ApiPayload): Promise<ApiResponse> {
  return _adapter("/api/predictive-insights", payload);
}

export function getRfpPanel(payload: ApiPayload): Promise<ApiResponse> {
  return _adapter("/api/rfp-panel", payload);
}
