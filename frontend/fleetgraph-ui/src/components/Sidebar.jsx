import React from 'react'
import { NavLink } from 'react-router-dom'

export default function Sidebar() {
  return (
    <aside>
      <nav>
        <ul>
          <li>
            <NavLink to="/home">Home</NavLink>
          </li>
          <li>
            <NavLink to="/signals">Signals</NavLink>
          </li>
          <li>
            <NavLink to="/summary">Summary</NavLink>
          </li>
        </ul>
      </nav>
    </aside>
  )
}
