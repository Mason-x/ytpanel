import test from 'node:test'
import assert from 'node:assert/strict'
import { renderToStaticMarkup } from 'react-dom/server'
import { MemoryRouter } from 'react-router-dom'
import Layout from '../Layout'

test('layout navigation does not render quality checklist link', () => {
  const html = renderToStaticMarkup(
    <MemoryRouter>
      <Layout />
    </MemoryRouter>,
  )

  assert.doesNotMatch(html, /质检清单/)
  assert.doesNotMatch(html, /quality-checklist/)
})
