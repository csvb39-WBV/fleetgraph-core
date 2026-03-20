import React from 'react'

export default function Signals({ records, selectedSignalId, onSelectSignal }) {
  const PRIMARY_FIELDS = [
    'id',
    'record_id',
    'signal_id',
    'organization',
    'organization_name',
    'domain',
    'source_domain',
    'target_domain',
    'relationship_type',
  ]
  const RELATIONSHIP_FIELDS = [
    'link_count',
    'organization_count',
    'shared_domain_count',
    'signal_type',
    'connection_type',
    'match_type',
    'strength',
    'confidence',
  ]
  const EVIDENCE_FIELDS = [
    'evidence',
    'evidence_count',
    'domains',
    'organizations',
    'links',
    'signals',
    'details',
    'summary',
    'reason',
    'reasons',
    'supporting_data',
  ]
  const CENTRAL_LABEL_FIELDS = [
    'organization_name',
    'organization',
    'domain',
    'signal_id',
    'record_id',
    'id',
  ]
  const RELATIONSHIP_GRAPH_FIELDS = [
    'source_domain',
    'target_domain',
    'domain',
    'relationship_type',
    'signal_type',
    'connection_type',
    'match_type',
  ]
  const ARRAY_GRAPH_FIELDS = ['domains', 'organizations', 'links', 'signals']

  function toReadableLabel(fieldName) {
    return fieldName
      .split('_')
      .map((part) => (part ? part.charAt(0).toUpperCase() + part.slice(1) : part))
      .join(' ')
  }

  function renderObjectValue(value) {
    const entries = Object.entries(value)
    if (entries.length === 0) {
      return <span>Not provided</span>
    }
    return (
      <div>
        {entries.map(([key, nestedValue]) => (
          <div key={key}>
            <strong>{toReadableLabel(key)}:</strong> {renderValue(nestedValue)}
          </div>
        ))}
      </div>
    )
  }

  function renderArrayValue(values) {
    if (values.length === 0) {
      return <span>Not provided</span>
    }
    return (
      <ul>
        {values.map((item, index) => (
          <li key={index}>
            {item === null || item === undefined || item === '' ? (
              <span>Not provided</span>
            ) : Array.isArray(item) ? (
              renderArrayValue(item)
            ) : typeof item === 'object' ? (
              renderObjectValue(item)
            ) : (
              <span>{String(item)}</span>
            )}
          </li>
        ))}
      </ul>
    )
  }

  function renderValue(value) {
    if (value === null || value === undefined || value === '') {
      return <span>Not provided</span>
    }
    if (Array.isArray(value)) {
      return renderArrayValue(value)
    }
    if (typeof value === 'object') {
      return renderObjectValue(value)
    }
    return <span>{String(value)}</span>
  }

  function getRecordLabel(record, index) {
    const candidateFields = [
      'signal_id',
      'id',
      'record_id',
      'domain',
      'organization',
      'organization_name',
    ]
    for (const fieldName of candidateFields) {
      const value = record[fieldName]
      if (value !== null && value !== undefined && value !== '') {
        return String(value)
      }
    }
    return `Record ${index + 1}`
  }

  function renderFieldGroup(title, fieldNames, record, renderedFields) {
    const presentFieldNames = []
    for (const fieldName of fieldNames) {
      if (Object.prototype.hasOwnProperty.call(record, fieldName)) {
        presentFieldNames.push(fieldName)
        renderedFields.add(fieldName)
      }
    }

    if (presentFieldNames.length === 0) {
      return null
    }

    return (
      <section>
        <h4>{title}</h4>
        <dl>
          {presentFieldNames.map((fieldName) => (
            <React.Fragment key={fieldName}>
              <dt>{toReadableLabel(fieldName)}</dt>
              <dd>{renderValue(record[fieldName])}</dd>
            </React.Fragment>
          ))}
        </dl>
      </section>
    )
  }

  function getBestScalarLabel(value) {
    if (value === null || value === undefined || value === '') {
      return null
    }
    if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
      return String(value)
    }
    return null
  }

  function getObjectItemLabel(item, index) {
    const objectLabelFields = [
      'name',
      'id',
      'domain',
      'organization',
      'organization_name',
      'type',
      'label',
    ]

    for (const fieldName of objectLabelFields) {
      if (Object.prototype.hasOwnProperty.call(item, fieldName)) {
        const value = item[fieldName]
        const scalarLabel = getBestScalarLabel(value)
        if (scalarLabel !== null) {
          return scalarLabel
        }
      }
    }

    return `Item ${index + 1}`
  }

  function buildRelationshipGraph(record) {
    if (!record || typeof record !== 'object') {
      return { centralLabel: 'Selected Record', nodes: [] }
    }

    let centralLabel = 'Selected Record'
    for (const fieldName of CENTRAL_LABEL_FIELDS) {
      if (Object.prototype.hasOwnProperty.call(record, fieldName)) {
        const label = getBestScalarLabel(record[fieldName])
        if (label !== null) {
          centralLabel = label
          break
        }
      }
    }

    const candidates = []

    for (const fieldName of RELATIONSHIP_GRAPH_FIELDS) {
      if (!Object.prototype.hasOwnProperty.call(record, fieldName)) {
        continue
      }
      const value = record[fieldName]
      const label = getBestScalarLabel(value)
      if (label !== null) {
        candidates.push({
          id: `field:${fieldName}`,
          sourceField: fieldName,
          label,
        })
      }
    }

    for (const fieldName of ARRAY_GRAPH_FIELDS) {
      if (!Object.prototype.hasOwnProperty.call(record, fieldName)) {
        continue
      }
      const value = record[fieldName]
      if (!Array.isArray(value)) {
        continue
      }

      const limitedItems = value.slice(0, 5)
      limitedItems.forEach((item, index) => {
        if (item === null || item === undefined || item === '') {
          return
        }

        let label = null
        if (typeof item === 'string' || typeof item === 'number' || typeof item === 'boolean') {
          label = String(item)
        } else if (typeof item === 'object') {
          label = getObjectItemLabel(item, index)
        }

        if (label !== null) {
          candidates.push({
            id: `array:${fieldName}:${index}`,
            sourceField: fieldName,
            label,
          })
        }
      })
    }

    const nodes = candidates.slice(0, 11)
    return { centralLabel, nodes }
  }

  const selectedRecord = records.find((record) => record.signal_id === selectedSignalId)
  const graph = buildRelationshipGraph(selectedRecord)

  let selectedRecordDetails = null
  if (!selectedRecord) {
    selectedRecordDetails = <p>No record selected.</p>
  } else {
    const selectedRecordKeys = Object.keys(selectedRecord)
    if (selectedRecordKeys.length === 0) {
      selectedRecordDetails = <p>No record details available.</p>
    } else {
      const renderedFields = new Set()
      const primarySection = renderFieldGroup(
        'Primary Details',
        PRIMARY_FIELDS,
        selectedRecord,
        renderedFields
      )
      const relationshipSection = renderFieldGroup(
        'Relationship Details',
        RELATIONSHIP_FIELDS,
        selectedRecord,
        renderedFields
      )
      const evidenceSection = renderFieldGroup(
        'Evidence / Supporting Data',
        EVIDENCE_FIELDS,
        selectedRecord,
        renderedFields
      )

      const additionalFields = selectedRecordKeys.filter(
        (fieldName) => !renderedFields.has(fieldName)
      )

      const additionalSection = additionalFields.length > 0 ? (
        <section>
          <h4>Additional Fields</h4>
          <dl>
            {additionalFields.map((fieldName) => (
              <React.Fragment key={fieldName}>
                <dt>{toReadableLabel(fieldName)}</dt>
                <dd>{renderValue(selectedRecord[fieldName])}</dd>
              </React.Fragment>
            ))}
          </dl>
        </section>
      ) : null

      const renderedSections = [
        primarySection,
        relationshipSection,
        evidenceSection,
        additionalSection,
      ].filter(Boolean)

      selectedRecordDetails =
        renderedSections.length > 0 ? renderedSections : <p>No record details available.</p>
    }
  }

  return (
    <section>
      <h2>Signals</h2>
      <p>Read-only relationship signal explorer.</p>

      <h3>Record List</h3>
      <ul>
        {records.map((record, index) => (
          <li key={record.signal_id || index}>
            <button type="button" onClick={() => onSelectSignal(record.signal_id)}>
              {record.signal_id === selectedSignalId ? '[Selected] ' : ''}
              {getRecordLabel(record, index)}
            </button>
          </li>
        ))}
      </ul>

      <h3>Selected Record Detail</h3>
      {selectedRecordDetails}

      <h3>Relationship View</h3>
      {graph.nodes.length === 0 ? (
        <p>No graphable relationship view available for this record.</p>
      ) : (
        <svg
          width="760"
          height="420"
          viewBox="0 0 760 420"
          role="img"
          aria-label="Deterministic relationship view"
        >
          <rect x="1" y="1" width="758" height="418" fill="#ffffff" stroke="#d6d6d6" />

          {graph.nodes.map((node, index) => {
            const centerX = 380
            const centerY = 210
            const radius = 145
            const angle = (2 * Math.PI * index) / graph.nodes.length - Math.PI / 2
            const nodeX = centerX + radius * Math.cos(angle)
            const nodeY = centerY + radius * Math.sin(angle)

            return (
              <g key={node.id}>
                <line x1={centerX} y1={centerY} x2={nodeX} y2={nodeY} stroke="#7c7c7c" />
                <text
                  x={(centerX + nodeX) / 2}
                  y={(centerY + nodeY) / 2 - 4}
                  textAnchor="middle"
                  fontSize="10"
                  fill="#555555"
                >
                  {toReadableLabel(node.sourceField)}
                </text>
                <circle cx={nodeX} cy={nodeY} r="28" fill="#f1f7ff" stroke="#5c8ed8" />
                <text
                  x={nodeX}
                  y={nodeY + 4}
                  textAnchor="middle"
                  fontSize="10"
                  fill="#1a1a1a"
                >
                  {node.label.length > 16 ? `${node.label.slice(0, 16)}...` : node.label}
                </text>
              </g>
            )
          })}

          <circle cx="380" cy="210" r="42" fill="#e6f3ec" stroke="#4a9166" />
          <text x="380" y="214" textAnchor="middle" fontSize="11" fill="#1a1a1a">
            {graph.centralLabel.length > 20
              ? `${graph.centralLabel.slice(0, 20)}...`
              : graph.centralLabel}
          </text>
        </svg>
      )}
    </section>
  )
}
