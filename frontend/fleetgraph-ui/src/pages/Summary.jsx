import React from 'react'

export default function Summary({ summary, records, demoModeEnabled, demoStage }) {
  function toReadableLabel(fieldName) {
    return fieldName
      .split('_')
      .map((part) => (part ? part.charAt(0).toUpperCase() + part.slice(1) : part))
      .join(' ')
  }

  function renderValue(value) {
    if (value === null || value === undefined || value === '') {
      return <span>Not provided</span>
    }

    if (Array.isArray(value)) {
      if (value.length === 0) {
        return <span>Not provided</span>
      }

      return (
        <ul style={{ margin: '0.3rem 0 0 1.2rem' }}>
          {value.map((item, index) => (
            <li key={index}>{renderValue(item)}</li>
          ))}
        </ul>
      )
    }

    if (typeof value === 'object') {
      const entries = Object.entries(value)
      if (entries.length === 0) {
        return <span>Not provided</span>
      }

      return (
        <div style={{ display: 'grid', gap: '0.25rem' }}>
          {entries.map(([key, nestedValue]) => (
            <div key={key}>
              <strong>{toReadableLabel(key)}:</strong> {renderValue(nestedValue)}
            </div>
          ))}
        </div>
      )
    }

    return <span>{String(value)}</span>
  }

  const supportsOrganizationCount = records.every(
    (record) => typeof record.organization_count === 'number'
  )
  const supportsLinkCount = records.every((record) => typeof record.link_count === 'number')

  const totalRecords =
    summary && typeof summary.record_count === 'number' ? summary.record_count : records.length
  const totalOrganizations = supportsOrganizationCount
    ? records.reduce((total, record) => total + record.organization_count, 0)
    : null
  const totalLinks = supportsLinkCount
    ? records.reduce((total, record) => total + record.link_count, 0)
    : null
  const domainCount = records.length
  const summaryEntries = summary ? Object.entries(summary) : []

  return (
    <section style={{ display: 'grid', gap: '1rem' }}>
      <h2>Summary</h2>
      <p>Read-only summary view of relationship signal data.</p>

      {demoModeEnabled ? (
        <section style={{ border: '1px solid #d8d8d8', padding: '0.75rem' }}>
          <h3>Demo Guidance</h3>
          <p>This page provides an executive summary of the current live relationship signal dataset.</p>
          <p>The overview metrics represent the current loaded records without changing backend truth.</p>
          <p>The detailed payload below reflects exactly what the backend summary is reporting.</p>
          <p>Current demo stage: {demoStage}.</p>
        </section>
      ) : null}

      <section style={{ border: '1px solid #d8d8d8', padding: '0.75rem' }}>
        <h3>Overview</h3>
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(2, minmax(140px, 1fr))',
            gap: '0.5rem',
          }}
        >
          <div style={{ border: '1px solid #e2e2e2', padding: '0.5rem' }}>
            <strong>Total Records</strong>
            <div>{totalRecords}</div>
          </div>

          {totalOrganizations !== null ? (
            <div style={{ border: '1px solid #e2e2e2', padding: '0.5rem' }}>
              <strong>Total Organizations</strong>
              <div>{totalOrganizations}</div>
            </div>
          ) : null}

          {totalLinks !== null ? (
            <div style={{ border: '1px solid #e2e2e2', padding: '0.5rem' }}>
              <strong>Total Links</strong>
              <div>{totalLinks}</div>
            </div>
          ) : null}

          <div style={{ border: '1px solid #e2e2e2', padding: '0.5rem' }}>
            <strong>Domain Count</strong>
            <div>{domainCount}</div>
          </div>
        </div>
      </section>

      <section style={{ border: '1px solid #d8d8d8', padding: '0.75rem' }}>
        <h3>Summary Details</h3>

        {summaryEntries.length === 0 ? (
          <p>No summary data available.</p>
        ) : (
          <dl style={{ display: 'grid', gap: '0.6rem' }}>
            {summaryEntries.map(([fieldName, fieldValue]) => (
              <React.Fragment key={fieldName}>
                <dt>
                  <strong>{toReadableLabel(fieldName)}</strong>
                </dt>
                <dd style={{ marginInlineStart: '1rem' }}>{renderValue(fieldValue)}</dd>
              </React.Fragment>
            ))}
          </dl>
        )}
      </section>
    </section>
  )
}
