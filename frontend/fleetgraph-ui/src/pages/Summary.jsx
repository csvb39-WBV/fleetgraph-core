import React from 'react'

export default function Summary({ summary, records, demoModeEnabled, demoStage }) {
  const cardStyle = {
    border: '1px solid #e5e7eb',
    borderRadius: '8px',
    padding: '16px',
    marginBottom: '16px',
    background: '#ffffff',
  }

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
        <ul style={{ margin: '8px 0 0 16px', display: 'grid', gap: '8px' }}>
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
        <div style={{ display: 'grid', gap: '8px' }}>
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
    <section style={{ display: 'grid', gap: '16px', color: '#111827' }}>
      <div>
        <h2 style={{ fontSize: '24px', fontWeight: 700, margin: 0 }}>Summary</h2>
        <p style={{ fontSize: '16px', color: '#6b7280', margin: '8px 0 0 0' }}>
          Read-only summary view of relationship signal data.
        </p>
      </div>

      {demoModeEnabled ? (
        <section style={cardStyle}>
          <h3 style={{ fontSize: '18px', fontWeight: 700, margin: 0 }}>Demo Guidance</h3>
          <div style={{ display: 'grid', gap: '8px', marginTop: '12px', color: '#6b7280', fontSize: '14px' }}>
            <p style={{ margin: 0 }}>
              This page provides an executive summary of the current live relationship signal dataset.
            </p>
            <p style={{ margin: 0 }}>
              The overview metrics represent the current loaded records without changing backend truth.
            </p>
            <p style={{ margin: 0 }}>
              The detailed payload below reflects exactly what the backend summary is reporting.
            </p>
            <p style={{ margin: 0 }}>Current demo stage: {demoStage}.</p>
          </div>
        </section>
      ) : null}

      <section style={cardStyle}>
        <h3 style={{ fontSize: '18px', fontWeight: 700, margin: 0 }}>Overview</h3>
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))',
            gap: '16px',
            marginTop: '16px',
          }}
        >
          <div style={cardStyle}>
            <div style={{ fontSize: '13px', color: '#6b7280' }}>Total Records</div>
            <div style={{ fontSize: '16px', color: '#111827', fontWeight: 700, marginTop: '8px' }}>{totalRecords}</div>
          </div>

          {totalOrganizations !== null ? (
            <div style={cardStyle}>
              <div style={{ fontSize: '13px', color: '#6b7280' }}>Total Organizations</div>
              <div style={{ fontSize: '16px', color: '#111827', fontWeight: 700, marginTop: '8px' }}>
                {totalOrganizations}
              </div>
            </div>
          ) : null}

          {totalLinks !== null ? (
            <div style={cardStyle}>
              <div style={{ fontSize: '13px', color: '#6b7280' }}>Total Links</div>
              <div style={{ fontSize: '16px', color: '#111827', fontWeight: 700, marginTop: '8px' }}>
                {totalLinks}
              </div>
            </div>
          ) : null}

          <div style={cardStyle}>
            <div style={{ fontSize: '13px', color: '#6b7280' }}>Domain Count</div>
            <div style={{ fontSize: '16px', color: '#111827', fontWeight: 700, marginTop: '8px' }}>{domainCount}</div>
          </div>
        </div>
      </section>

      <section style={cardStyle}>
        <h3 style={{ fontSize: '18px', fontWeight: 700, margin: 0 }}>Summary Details</h3>

        {summaryEntries.length === 0 ? (
          <p style={{ marginTop: '16px', color: '#6b7280', fontSize: '14px' }}>No summary data available.</p>
        ) : (
          <dl style={{ display: 'grid', gap: '12px', marginTop: '16px', marginBottom: 0 }}>
            {summaryEntries.map(([fieldName, fieldValue]) => (
              <React.Fragment key={fieldName}>
                <dt>
                  <strong style={{ fontSize: '13px', color: '#6b7280' }}>{toReadableLabel(fieldName)}</strong>
                </dt>
                <dd style={{ marginInlineStart: '16px', marginBottom: 0, fontSize: '14px', color: '#111827' }}>
                  {renderValue(fieldValue)}
                </dd>
              </React.Fragment>
            ))}
          </dl>
        )}
      </section>
    </section>
  )
}
