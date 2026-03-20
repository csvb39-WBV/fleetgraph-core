import React from 'react'
import { NavLink } from 'react-router-dom'

export default function Sidebar() {
  return (
    <aside
      style={{
        width: '220px',
        borderRight: '1px solid #e5e7eb',
        padding: '16px',
        background: '#ffffff',
      }}
    >
      <nav style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
        <ul style={{ listStyle: 'none', padding: 0, margin: 0, display: 'grid', gap: '12px' }}>
          <li>
            <NavLink
              to="/home"
              style={({ isActive }) => ({
                display: 'block',
                textDecoration: 'none',
                color: '#111827',
                fontSize: '14px',
                fontWeight: isActive ? 600 : 400,
                background: isActive ? '#f3f4f6' : '#ffffff',
                padding: '8px',
                borderRadius: '6px',
              })}
            >
              Home
            </NavLink>
          </li>
          <li>
            <NavLink
              to="/signals"
              style={({ isActive }) => ({
                display: 'block',
                textDecoration: 'none',
                color: '#111827',
                fontSize: '14px',
                fontWeight: isActive ? 600 : 400,
                background: isActive ? '#f3f4f6' : '#ffffff',
                padding: '8px',
                borderRadius: '6px',
              })}
            >
              Signals
            </NavLink>
          </li>
          <li>
            <NavLink
              to="/summary"
              style={({ isActive }) => ({
                display: 'block',
                textDecoration: 'none',
                color: '#111827',
                fontSize: '14px',
                fontWeight: isActive ? 600 : 400,
                background: isActive ? '#f3f4f6' : '#ffffff',
                padding: '8px',
                borderRadius: '6px',
              })}
            >
              Summary
            </NavLink>
          </li>
        </ul>
      </nav>
    </aside>
  )
}
