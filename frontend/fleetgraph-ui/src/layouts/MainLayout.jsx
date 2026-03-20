import React from 'react'
import { Outlet } from 'react-router-dom'
import Header from '../components/Header.jsx'
import Sidebar from '../components/Sidebar.jsx'

export default function MainLayout() {
  return (
    <div style={{ border: '1px solid #e5e7eb', borderRadius: '8px', background: '#ffffff' }}>
      <Header />
      <div style={{ display: 'flex', alignItems: 'stretch' }}>
        <Sidebar />
        <main style={{ flex: 1, padding: '24px', minWidth: 0 }}>
          <Outlet />
        </main>
      </div>
    </div>
  )
}
