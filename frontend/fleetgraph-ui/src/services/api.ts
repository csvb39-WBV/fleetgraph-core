export const API_BASE = import.meta.env.VITE_API_BASE_URL || 'http://127.0.0.1:8000'

type UnknownRecord = Record<string, unknown>

export interface RuntimeSummary {
  environment: string
  api_host: string
  api_port: number
  debug: boolean
  log_level: string
}

export interface RuntimeHealth {
  status: string
  checks: {
    config_valid: boolean
    logger_ready: boolean
  }
}

export interface RuntimeMetrics {
  request_metrics: {
    request_count_total: number
    request_success_count: number
    request_failure_count: number
  }
  error_metrics: {
    exception_count: number
  }
}

export interface RuntimeReadiness {
  status: string
  checks: {
    config_loaded: boolean
    bootstrap_complete: boolean
  }
}

export interface DashboardSnapshot {
  summary?: RuntimeSummary
  health?: RuntimeHealth
  metrics?: RuntimeMetrics
  readiness?: RuntimeReadiness
}

export interface DashboardSnapshotResult {
  data: DashboardSnapshot
  errors: string[]
}

function isObject(value: unknown): value is UnknownRecord {
  return typeof value === 'object' && value !== null
}

async function requestJson(path: string): Promise<unknown> {
  const response = await fetch(`${API_BASE}${path}`)
  const payload = await response.json().catch(() => null)

  if (!response.ok) {
    const message = isObject(payload) && typeof payload.message === 'string'
      ? payload.message
      : `Request failed with status ${response.status}`
    throw new Error(`${path}: ${message}`)
  }

  return payload
}

function parseSummary(payload: unknown): RuntimeSummary {
  if (!isObject(payload)) {
    throw new Error('/runtime/summary: invalid response payload')
  }

  return {
    environment: String(payload.environment ?? 'unknown'),
    api_host: String(payload.api_host ?? 'unknown'),
    api_port: Number(payload.api_port ?? 0),
    debug: Boolean(payload.debug),
    log_level: String(payload.log_level ?? 'unknown'),
  }
}

function parseHealth(payload: unknown): RuntimeHealth {
  if (!isObject(payload) || !isObject(payload.checks)) {
    throw new Error('/runtime/health: invalid response payload')
  }

  return {
    status: String(payload.status ?? 'unknown'),
    checks: {
      config_valid: Boolean(payload.checks.config_valid),
      logger_ready: Boolean(payload.checks.logger_ready),
    },
  }
}

function parseMetrics(payload: unknown): RuntimeMetrics {
  if (!isObject(payload) || !isObject(payload.request_metrics) || !isObject(payload.error_metrics)) {
    throw new Error('/runtime/metrics: invalid response payload')
  }

  return {
    request_metrics: {
      request_count_total: Number(payload.request_metrics.request_count_total ?? 0),
      request_success_count: Number(payload.request_metrics.request_success_count ?? 0),
      request_failure_count: Number(payload.request_metrics.request_failure_count ?? 0),
    },
    error_metrics: {
      exception_count: Number(payload.error_metrics.exception_count ?? 0),
    },
  }
}

function parseReadiness(payload: unknown): RuntimeReadiness {
  if (!isObject(payload) || !isObject(payload.checks)) {
    throw new Error('/runtime/readiness: invalid response payload')
  }

  return {
    status: String(payload.status ?? 'unknown'),
    checks: {
      config_loaded: Boolean(payload.checks.config_loaded),
      bootstrap_complete: Boolean(payload.checks.bootstrap_complete),
    },
  }
}

export async function getRuntimeSummary(): Promise<RuntimeSummary> {
  return parseSummary(await requestJson('/runtime/summary'))
}

export async function getRuntimeHealth(): Promise<RuntimeHealth> {
  return parseHealth(await requestJson('/runtime/health'))
}

export async function getRuntimeMetrics(): Promise<RuntimeMetrics> {
  return parseMetrics(await requestJson('/runtime/metrics'))
}

export async function getRuntimeReadiness(): Promise<RuntimeReadiness> {
  return parseReadiness(await requestJson('/runtime/readiness'))
}

export async function getDashboardSnapshot(): Promise<DashboardSnapshotResult> {
  const [summary, health, metrics, readiness] = await Promise.allSettled([
    getRuntimeSummary(),
    getRuntimeHealth(),
    getRuntimeMetrics(),
    getRuntimeReadiness(),
  ])

  const data: DashboardSnapshot = {}
  const errors: string[] = []

  if (summary.status === 'fulfilled') {
    data.summary = summary.value
  } else {
    errors.push(summary.reason instanceof Error ? summary.reason.message : '/runtime/summary: request failed')
  }

  if (health.status === 'fulfilled') {
    data.health = health.value
  } else {
    errors.push(health.reason instanceof Error ? health.reason.message : '/runtime/health: request failed')
  }

  if (metrics.status === 'fulfilled') {
    data.metrics = metrics.value
  } else {
    errors.push(metrics.reason instanceof Error ? metrics.reason.message : '/runtime/metrics: request failed')
  }

  if (readiness.status === 'fulfilled') {
    data.readiness = readiness.value
  } else {
    errors.push(readiness.reason instanceof Error ? readiness.reason.message : '/runtime/readiness: request failed')
  }

  return { data, errors }
}
