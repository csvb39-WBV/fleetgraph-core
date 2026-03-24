import { useCallback, useEffect, useRef, useState } from 'react'
import SectionCard from '../components/dashboard/SectionCard'
import StatCard from '../components/dashboard/StatCard'
import StatusBadge from '../components/dashboard/StatusBadge'
import {
  API_BASE,
  type DashboardSnapshot,
  getDashboardSnapshot,
} from '../services/api'

function formatTimestamp(value: Date | null): string {
  if (!value) {
    return 'Not updated yet'
  }

  return value.toLocaleString()
}

function toYesNo(value: boolean | undefined): string {
  if (value === undefined) {
    return 'Unknown'
  }

  return value ? 'Yes' : 'No'
}

export default function FleetGraphDashboard() {
  const [snapshot, setSnapshot] = useState<DashboardSnapshot>({})
  const [isLoading, setIsLoading] = useState(true)
  const [isSuccess, setIsSuccess] = useState(false)
  const [errorMessage, setErrorMessage] = useState('')
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null)
  const requestInFlightRef = useRef(false)

  const refreshDashboard = useCallback(async () => {
    if (requestInFlightRef.current) {
      return
    }

    requestInFlightRef.current = true
    try {
      const { data, errors } = await getDashboardSnapshot()

      setSnapshot((previous) => ({
        ...previous,
        ...data,
      }))

      if (errors.length > 0) {
        setErrorMessage(errors.join(' | '))
        setIsSuccess(false)
      } else {
        setErrorMessage('')
        setIsSuccess(true)
      }

      setLastUpdated(new Date())
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : 'Failed to fetch dashboard data')
      setIsSuccess(false)
    } finally {
      setIsLoading(false)
      requestInFlightRef.current = false
    }
  }, [])

  useEffect(() => {
    refreshDashboard()
    const intervalId = window.setInterval(refreshDashboard, 15000)

    return () => {
      window.clearInterval(intervalId)
    }
  }, [refreshDashboard])

  const healthState = snapshot.health?.status === 'healthy' ? 'good' : 'bad'
  const readinessState = snapshot.readiness?.status === 'ready' ? 'good' : 'bad'

  return (
    <main
      style={{
        minHeight: '100vh',
        padding: '24px',
        background:
          'radial-gradient(circle at top left, rgba(14, 116, 144, 0.17), transparent 40%), linear-gradient(180deg, #f8fafc, #e2e8f0)',
        color: '#0f172a',
      }}
    >
      <div style={{ maxWidth: '1120px', margin: '0 auto' }}>
        <header
          style={{
            border: '1px solid #cbd5e1',
            borderRadius: '16px',
            padding: '20px',
            marginBottom: '20px',
            background: 'rgba(255, 255, 255, 0.86)',
            backdropFilter: 'blur(2px)',
          }}
        >
          <h1 style={{ margin: 0, fontSize: '28px', letterSpacing: '0.02em' }}>FleetGraph Client Dashboard</h1>
          <p style={{ margin: '8px 0 0', color: '#334155', fontSize: '14px' }}>
            API Base: {API_BASE}
          </p>
          <p style={{ margin: '4px 0 0', color: '#334155', fontSize: '14px' }}>
            Last updated: {formatTimestamp(lastUpdated)}
          </p>
          <div style={{ marginTop: '14px', display: 'flex', gap: '10px', flexWrap: 'wrap' }}>
            <StatusBadge label={`Health: ${snapshot.health?.status ?? 'unknown'}`} state={healthState} />
            <StatusBadge label={`Readiness: ${snapshot.readiness?.status ?? 'unknown'}`} state={readinessState} />
            <StatusBadge label={isLoading ? 'Loading' : isSuccess ? 'Synchronized' : 'Degraded'} state={isLoading ? 'neutral' : isSuccess ? 'good' : 'bad'} />
          </div>
        </header>

        {errorMessage ? (
          <section
            style={{
              border: '1px solid #fca5a5',
              borderRadius: '12px',
              background: '#fef2f2',
              padding: '12px 14px',
              color: '#7f1d1d',
              marginBottom: '20px',
              fontSize: '14px',
            }}
          >
            {errorMessage}
          </section>
        ) : null}

        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))',
            gap: '16px',
          }}
        >
          <SectionCard title="Runtime Summary" subtitle="Current bootstrap and environment settings.">
            <div
              style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))',
                gap: '10px',
              }}
            >
              <StatCard label="environment" value={snapshot.summary?.environment ?? 'Unavailable'} />
              <StatCard label="api_host" value={snapshot.summary?.api_host ?? 'Unavailable'} />
              <StatCard label="api_port" value={snapshot.summary?.api_port ?? 'Unavailable'} />
              <StatCard label="debug" value={toYesNo(snapshot.summary?.debug)} />
              <StatCard label="log_level" value={snapshot.summary?.log_level ?? 'Unavailable'} />
            </div>
          </SectionCard>

          <SectionCard title="Runtime Metrics" subtitle="Live request and exception counters.">
            <div
              style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))',
                gap: '10px',
              }}
            >
              <StatCard
                label="request_count_total"
                value={snapshot.metrics?.request_metrics.request_count_total ?? 0}
              />
              <StatCard
                label="request_success_count"
                value={snapshot.metrics?.request_metrics.request_success_count ?? 0}
              />
              <StatCard
                label="request_failure_count"
                value={snapshot.metrics?.request_metrics.request_failure_count ?? 0}
              />
              <StatCard label="exception_count" value={snapshot.metrics?.error_metrics.exception_count ?? 0} />
            </div>
          </SectionCard>

          <SectionCard title="Readiness Checks" subtitle="Runtime readiness guardrails.">
            <div
              style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))',
                gap: '10px',
              }}
            >
              <StatCard
                label="config_loaded"
                value={toYesNo(snapshot.readiness?.checks.config_loaded)}
              />
              <StatCard
                label="bootstrap_complete"
                value={toYesNo(snapshot.readiness?.checks.bootstrap_complete)}
              />
              <StatCard label="status" value={snapshot.readiness?.status ?? 'Unavailable'} />
            </div>
          </SectionCard>

          <SectionCard title="Health Checks" subtitle="Runtime health endpoint diagnostics.">
            <div
              style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))',
                gap: '10px',
              }}
            >
              <StatCard label="config_valid" value={toYesNo(snapshot.health?.checks.config_valid)} />
              <StatCard label="logger_ready" value={toYesNo(snapshot.health?.checks.logger_ready)} />
              <StatCard label="status" value={snapshot.health?.status ?? 'Unavailable'} />
            </div>
          </SectionCard>
        </div>
      </div>
    </main>
  )
}
