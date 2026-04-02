# YouTube Reporting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a fully isolated multi-owner YouTube Reporting API subsystem with owner management, per-owner proxy isolation, request logging, raw report storage, derived daily metrics, and a new channel-level Reporting tab without disturbing existing Data API flows.

**Architecture:** Keep the existing `YouTube Data API v3` pipeline unchanged and build a separate Reporting subsystem across SQLite tables, Express routes, queue services, and React settings/channel pages. Every Reporting request is created from owner-scoped credentials and proxy configuration, and raw reports are stored separately from derived daily metrics used by the UI.

**Tech Stack:** React 19, Vite, TypeScript, Express, better-sqlite3, node:test, undici, socks-proxy-agent

---

## File Structure

### Existing files to modify

- `server/src/schema.sql`
  Add Reporting tables and indexes.
- `server/src/db.ts`
  Ensure any new columns/indexes/migrations needed by Reporting tables are initialized safely.
- `server/src/index.ts`
  Mount the new Reporting routes.
- `server/src/services/jobQueue.ts`
  Register and run Reporting sync job types.
- `server/src/services/dailySyncScheduler.ts`
  Add Reporting job enqueue hooks to the existing scheduler flow.
- `src/types.ts`
  Add Owner, binding, usage, log, and reporting view response types.
- `src/lib/api.ts`
  Add frontend API helpers for Reporting settings and channel report endpoints.
- `src/pages/SettingsPage.tsx`
  Add the Owner management backend UI.
- `src/pages/ChannelsPage.tsx`
  Add the new `报表` tab and data loading/rendering.

### New backend files to create

- `server/src/services/reportingOwners.ts`
  Owner CRUD, masking, binding, enable/disable helpers.
- `server/src/services/youtubeReportingClient.ts`
  Owner-scoped OAuth refresh and isolated request client factory.
- `server/src/services/reportingProxyProbe.ts`
  Real proxy connectivity and egress IP checks.
- `server/src/services/youtubeReportingImport.ts`
  CSV parsing and raw report import helpers.
- `server/src/services/youtubeReportingDerive.ts`
  Derived daily metric aggregation and upsert logic.
- `server/src/services/youtubeReportingSync.ts`
  End-to-end sync orchestration for Owner/channel bindings.
- `server/src/routes/reporting.ts`
  Reporting Owner, binding, log, usage, and channel report endpoints.

### New test files to create

- `server/src/services/__tests__/reportingOwners.test.ts`
- `server/src/services/__tests__/youtubeReportingDerive.test.ts`
- `server/src/services/__tests__/youtubeReportingImport.test.ts`
- `server/src/services/__tests__/reportingProxyProbe.test.ts`
- `server/src/routes/__tests__/reportingRouteHelpers.test.ts`
- `src/lib/__tests__/reportingSettingsForm.test.ts`
- `src/components/channels/__tests__/ReportingPanel.test.tsx`

### Optional new frontend component files

- `src/components/settings/ReportingOwnersPanel.tsx`
- `src/components/settings/OwnerEditorModal.tsx`
- `src/components/settings/OwnerBindingsPanel.tsx`
- `src/components/settings/OwnerRequestLogPanel.tsx`
- `src/components/channels/ReportingPanel.tsx`

Keep these components focused. If the existing page files become unwieldy, split view logic into the component files above instead of inflating `SettingsPage.tsx` and `ChannelsPage.tsx`.

## Task 1: Add Reporting schema and core types

**Files:**
- Modify: `server/src/schema.sql`
- Modify: `server/src/db.ts`
- Modify: `src/types.ts`
- Test: `server/src/services/__tests__/reportingOwners.test.ts`

- [ ] **Step 1: Write the failing schema/type test**

```ts
import test from 'node:test'
import assert from 'node:assert/strict'

test('reporting owner rows require owner credentials and binding uniqueness', () => {
  assert.equal(typeof 'placeholder-owner-id', 'string')
})
```

This initial test is intentionally minimal. Expand it immediately to assert the normalized shape returned by the upcoming service helpers for:
- owner masking
- binding uniqueness
- started_at persistence

- [ ] **Step 2: Run the test to verify the Reporting helpers do not exist yet**

Run:

```bash
npx tsx --test server/src/services/__tests__/reportingOwners.test.ts
```

Expected:
- FAIL because the test imports a service file or helper that does not exist yet.

- [ ] **Step 3: Add the SQLite tables to `server/src/schema.sql`**

Create:
- `reporting_owners`
- `reporting_owner_channel_bindings`
- `reporting_request_logs`
- `reporting_job_state`
- `reporting_raw_reports`
- `video_reporting_daily`

Include indexes for:
- `reporting_owner_channel_bindings(owner_id, reporting_enabled)`
- `reporting_request_logs(owner_id, started_at)`
- `reporting_job_state(owner_id, remote_report_id)`
- `video_reporting_daily(channel_id, date)`

Enforce:
- `UNIQUE(channel_id)` on the binding table
- `PRIMARY KEY(date, channel_id, video_id)` on `video_reporting_daily`

- [ ] **Step 4: Add any boot-time migration safety in `server/src/db.ts`**

Implement only the minimum needed:
- create indexes if missing
- ensure legacy DB boot still succeeds
- avoid destructive migration logic

- [ ] **Step 5: Extend shared frontend types in `src/types.ts`**

Add types for:
- `ReportingOwner`
- `ReportingOwnerBinding`
- `ReportingRequestLog`
- `ReportingOwnerUsage`
- `ChannelReportingSummary`
- `ChannelReportingDailyRow`
- `ChannelReportingVideoRow`

- [ ] **Step 6: Expand the test to assert the new normalized types**

Verify:
- masked credential placeholders can be represented
- binding rows include `started_at`
- reporting daily rows carry the five requested metrics

- [ ] **Step 7: Run the test and typecheck the backend**

Run:

```bash
npx tsx --test server/src/services/__tests__/reportingOwners.test.ts
npm run build:server
```

Expected:
- test PASS
- backend TypeScript PASS

- [ ] **Step 8: Commit**

```bash
git add server/src/schema.sql server/src/db.ts src/types.ts server/src/services/__tests__/reportingOwners.test.ts
git commit -m "feat: add reporting schema and shared types"
```

## Task 2: Build Owner storage, masking, and binding services

**Files:**
- Create: `server/src/services/reportingOwners.ts`
- Modify: `server/src/db.ts`
- Test: `server/src/services/__tests__/reportingOwners.test.ts`

- [ ] **Step 1: Write failing service-level tests for owner CRUD helpers**

Cover:
- create owner with raw secrets
- sanitize owner for client responses
- keep masked values when patch payload leaves secrets untouched
- reject a second binding for the same channel

Example:

```ts
test('sanitizeReportingOwner masks client_secret and refresh_token', () => {
  const sanitized = sanitizeReportingOwner({
    owner_id: 'owner-1',
    client_secret: 'secret-value',
    refresh_token: 'refresh-value',
  } as any)

  assert.match(String(sanitized.client_secret || ''), /MASKED/)
  assert.match(String(sanitized.refresh_token || ''), /MASKED/)
})
```

- [ ] **Step 2: Run the tests and verify they fail for missing exports**

Run:

```bash
npx tsx --test server/src/services/__tests__/reportingOwners.test.ts
```

Expected:
- FAIL due to missing `reportingOwners.ts` exports.

- [ ] **Step 3: Implement `server/src/services/reportingOwners.ts`**

Add focused functions:
- `listReportingOwners()`
- `createReportingOwner(input)`
- `updateReportingOwner(ownerId, input)`
- `deleteReportingOwner(ownerId)`
- `listReportingBindings(ownerId)`
- `createReportingBinding(ownerId, input)`
- `updateReportingBinding(bindingId, input)`
- `deleteReportingBinding(bindingId)`
- `sanitizeReportingOwner(owner)`

Use the same masked-placeholder behavior already established in `server/src/routes/settings.ts` for sensitive fields.

- [ ] **Step 4: Add validation rules**

Validate:
- non-empty `name`
- `client_id`, `client_secret`, `refresh_token` preserved or replaced correctly
- `proxy_url` is empty or a valid `http|https|socks|socks5` URL
- binding `started_at` is a valid `YYYY-MM-DD`

- [ ] **Step 5: Re-run tests**

Run:

```bash
npx tsx --test server/src/services/__tests__/reportingOwners.test.ts
```

Expected:
- PASS for masking, update, and binding uniqueness cases.

- [ ] **Step 6: Commit**

```bash
git add server/src/services/reportingOwners.ts server/src/services/__tests__/reportingOwners.test.ts
git commit -m "feat: add reporting owner storage helpers"
```

## Task 3: Add isolated proxy probe and request logging primitives

**Files:**
- Create: `server/src/services/reportingProxyProbe.ts`
- Modify: `server/src/services/reportingOwners.ts`
- Test: `server/src/services/__tests__/reportingProxyProbe.test.ts`

- [ ] **Step 1: Write failing tests for proxy normalization and log payload shape**

Cover:
- direct mode
- HTTP proxy mode
- SOCKS proxy mode
- probe result object includes `egress_ip`, `google_oauth_ok`, `reporting_api_ok`

- [ ] **Step 2: Run the probe tests and verify the file is missing**

Run:

```bash
npx tsx --test server/src/services/__tests__/reportingProxyProbe.test.ts
```

Expected:
- FAIL because `reportingProxyProbe.ts` is missing.

- [ ] **Step 3: Implement `server/src/services/reportingProxyProbe.ts`**

Create helper functions:
- `getProxyMode(proxyUrl)`
- `probeReportingProxy(owner)`
- `insertReportingRequestLog(logInput)`

Use:
- `ProxyAgent` for HTTP/HTTPS proxy transport
- `SocksProxyAgent` where SOCKS is configured

The probe should:
1. detect egress IP
2. test Google OAuth endpoint reachability
3. test Reporting API reachability
4. store one or more request log rows

- [ ] **Step 4: Make the log writer reusable**

Move the SQLite insert logic into a single helper the sync service can reuse later instead of duplicating inserts in routes.

- [ ] **Step 5: Re-run tests**

Run:

```bash
npx tsx --test server/src/services/__tests__/reportingProxyProbe.test.ts
npm run build:server
```

Expected:
- tests PASS
- backend TypeScript PASS

- [ ] **Step 6: Commit**

```bash
git add server/src/services/reportingProxyProbe.ts server/src/services/__tests__/reportingProxyProbe.test.ts server/src/services/reportingOwners.ts
git commit -m "feat: add reporting proxy probe and request logging"
```

## Task 4: Add Owner-scoped Reporting client and CSV import/derive helpers

**Files:**
- Create: `server/src/services/youtubeReportingClient.ts`
- Create: `server/src/services/youtubeReportingImport.ts`
- Create: `server/src/services/youtubeReportingDerive.ts`
- Test: `server/src/services/__tests__/youtubeReportingImport.test.ts`
- Test: `server/src/services/__tests__/youtubeReportingDerive.test.ts`

- [ ] **Step 1: Write failing import tests for the three report families**

Add fixtures inline in the test for:
- `channel_reach_basic_a1`
- `channel_basic_a3`
- `channel_traffic_source_a3`

Assert that parsing extracts:
- `video_id`
- `date`
- metric values relevant to each report type

- [ ] **Step 2: Write failing derive tests for daily upsert behavior**

Cover:
- merge metrics from multiple report types into one `(date, channel_id, video_id)` row
- filter out rows before binding `started_at`
- aggregate traffic source share into JSON percentages

Example:

```ts
test('deriveVideoReportingDaily skips rows before started_at', () => {
  const rows = deriveVideoReportingDaily({
    startedAt: '2026-04-02',
    reachRows: [{ date: '2026-04-01', video_id: 'vid-1', impressions: 100, impressions_ctr: 0.1 }],
    basicRows: [],
    trafficRows: [],
  })

  assert.equal(rows.length, 0)
})
```

- [ ] **Step 3: Run the new tests and verify they fail**

Run:

```bash
npx tsx --test server/src/services/__tests__/youtubeReportingImport.test.ts
npx tsx --test server/src/services/__tests__/youtubeReportingDerive.test.ts
```

Expected:
- both FAIL due to missing services.

- [ ] **Step 4: Implement `server/src/services/youtubeReportingClient.ts`**

Add:
- owner-scoped OAuth refresh helper
- isolated request factory
- no cross-owner session reuse

Return small focused helpers rather than a long-lived singleton.

- [ ] **Step 5: Implement `server/src/services/youtubeReportingImport.ts`**

Add:
- report type detection
- CSV row parsing
- raw file metadata extraction

Keep parser output normalized and free of DB writes.

- [ ] **Step 6: Implement `server/src/services/youtubeReportingDerive.ts`**

Add:
- merge logic for the five required metrics
- `started_at` filtering
- traffic-source percentage aggregation
- SQLite upsert helper for `video_reporting_daily`

- [ ] **Step 7: Re-run focused tests**

Run:

```bash
npx tsx --test server/src/services/__tests__/youtubeReportingImport.test.ts
npx tsx --test server/src/services/__tests__/youtubeReportingDerive.test.ts
```

Expected:
- PASS for parser and derive coverage.

- [ ] **Step 8: Commit**

```bash
git add server/src/services/youtubeReportingClient.ts server/src/services/youtubeReportingImport.ts server/src/services/youtubeReportingDerive.ts server/src/services/__tests__/youtubeReportingImport.test.ts server/src/services/__tests__/youtubeReportingDerive.test.ts
git commit -m "feat: add reporting client import and derive services"
```

## Task 5: Build Reporting sync orchestration and queue integration

**Files:**
- Create: `server/src/services/youtubeReportingSync.ts`
- Modify: `server/src/services/jobQueue.ts`
- Modify: `server/src/services/dailySyncScheduler.ts`
- Test: `server/src/routes/__tests__/reportingRouteHelpers.test.ts`

- [ ] **Step 1: Write failing tests for enqueue and sync selection helpers**

Cover helper behavior for:
- only enabled owners and enabled bindings are scheduled
- one channel cannot enqueue under two owners
- duplicate remote report IDs are skipped

- [ ] **Step 2: Run the tests and verify the helpers are missing**

Run:

```bash
npx tsx --test server/src/routes/__tests__/reportingRouteHelpers.test.ts
```

Expected:
- FAIL because the new orchestration helpers do not exist yet.

- [ ] **Step 3: Implement `server/src/services/youtubeReportingSync.ts`**

Add:
- `enqueueReportingSyncForBinding(bindingId)`
- `enqueueDailyReportingSyncs(dateSource)`
- `syncReportingBinding(bindingId, options)`
- raw report discovery/download/import/derive sequence

Use `reporting_job_state` to ensure:
- duplicate reports are not redownloaded
- imported files can be re-derived without redownload

- [ ] **Step 4: Register a new job type in `server/src/services/jobQueue.ts`**

Add a job type such as:

```ts
type === 'sync_reporting_channel'
```

Payload should include at minimum:
- `binding_id`
- `owner_id`
- `channel_id`

- [ ] **Step 5: Hook daily scheduling into `server/src/services/dailySyncScheduler.ts`**

Do not replace existing behavior. Append Reporting enqueue logic after the existing daily sync enqueue path so failures remain isolated.

- [ ] **Step 6: Run the focused helper test and backend typecheck**

Run:

```bash
npx tsx --test server/src/routes/__tests__/reportingRouteHelpers.test.ts
npm run build:server
```

Expected:
- helper test PASS
- backend TypeScript PASS

- [ ] **Step 7: Commit**

```bash
git add server/src/services/youtubeReportingSync.ts server/src/services/jobQueue.ts server/src/services/dailySyncScheduler.ts server/src/routes/__tests__/reportingRouteHelpers.test.ts
git commit -m "feat: integrate reporting sync with queue and scheduler"
```

## Task 6: Expose Reporting routes and channel report endpoints

**Files:**
- Create: `server/src/routes/reporting.ts`
- Modify: `server/src/index.ts`
- Modify: `server/src/routes/analytics.ts`
- Test: `server/src/routes/__tests__/reportingRouteHelpers.test.ts`

- [ ] **Step 1: Write failing tests for route helper output shapes**

Cover response normalization for:
- owner list item
- owner usage summary
- request log rows
- channel reporting summary

- [ ] **Step 2: Run the route helper test and verify the new route helpers fail**

Run:

```bash
npx tsx --test server/src/routes/__tests__/reportingRouteHelpers.test.ts
```

Expected:
- FAIL because route serializers or helpers are incomplete.

- [ ] **Step 3: Implement `server/src/routes/reporting.ts`**

Add endpoints:
- `GET /api/reporting/owners`
- `POST /api/reporting/owners`
- `PATCH /api/reporting/owners/:ownerId`
- `DELETE /api/reporting/owners/:ownerId`
- `POST /api/reporting/owners/:ownerId/proxy-test`
- `GET /api/reporting/owners/:ownerId/logs`
- `GET /api/reporting/owners/:ownerId/usage`
- `POST /api/reporting/owners/:ownerId/bindings`
- `PATCH /api/reporting/bindings/:bindingId`
- `DELETE /api/reporting/bindings/:bindingId`
- `POST /api/reporting/bindings/:bindingId/sync`

- [ ] **Step 4: Mount the route in `server/src/index.ts`**

Register:

```ts
app.use('/api/reporting', reportingRoutes)
```

- [ ] **Step 5: Extend channel analytics routes or create channel-scoped report readers**

Expose:
- `GET /api/channels/:id/reporting/summary`
- `GET /api/channels/:id/reporting/daily`
- `GET /api/channels/:id/reporting/videos`
- `POST /api/channels/:id/reporting/sync`

Prefer colocating these read endpoints near existing channel analytics routes if the code stays readable. If `server/src/routes/analytics.ts` becomes too large, split channel reporting reads into a new route module and mount it under `/api/channels`.

- [ ] **Step 6: Re-run tests and backend build**

Run:

```bash
npx tsx --test server/src/routes/__tests__/reportingRouteHelpers.test.ts
npm run build:server
```

Expected:
- PASS
- backend TypeScript PASS

- [ ] **Step 7: Commit**

```bash
git add server/src/routes/reporting.ts server/src/index.ts server/src/routes/analytics.ts server/src/routes/__tests__/reportingRouteHelpers.test.ts
git commit -m "feat: add reporting routes and channel report endpoints"
```

## Task 7: Add frontend Reporting settings APIs and form helpers

**Files:**
- Modify: `src/types.ts`
- Modify: `src/lib/api.ts`
- Create: `src/lib/reportingSettingsForm.ts`
- Test: `src/lib/__tests__/reportingSettingsForm.test.ts`

- [ ] **Step 1: Write failing frontend helper tests**

Cover:
- masking behavior for owner secrets
- owner payload generation for create vs update
- binding payload generation with `started_at`

- [ ] **Step 2: Run the frontend helper test and verify it fails**

Run:

```bash
npx tsx --test src/lib/__tests__/reportingSettingsForm.test.ts
```

Expected:
- FAIL because `reportingSettingsForm.ts` does not exist yet.

- [ ] **Step 3: Implement `src/lib/reportingSettingsForm.ts`**

Add helpers similar in style to `src/lib/settingsForm.ts`:
- derive owner editor state
- preserve masked secrets when untouched
- normalize proxy and started date input

- [ ] **Step 4: Extend `src/lib/api.ts`**

Add methods for:
- owner CRUD
- binding CRUD
- proxy test
- owner usage
- owner logs
- channel reporting summary/daily/videos/sync

- [ ] **Step 5: Re-run the helper test**

Run:

```bash
npx tsx --test src/lib/__tests__/reportingSettingsForm.test.ts
```

Expected:
- PASS

- [ ] **Step 6: Commit**

```bash
git add src/types.ts src/lib/api.ts src/lib/reportingSettingsForm.ts src/lib/__tests__/reportingSettingsForm.test.ts
git commit -m "feat: add reporting frontend api helpers"
```

## Task 8: Build the Settings page Owner management UI

**Files:**
- Modify: `src/pages/SettingsPage.tsx`
- Create: `src/components/settings/ReportingOwnersPanel.tsx`
- Create: `src/components/settings/OwnerEditorModal.tsx`
- Create: `src/components/settings/OwnerBindingsPanel.tsx`
- Create: `src/components/settings/OwnerRequestLogPanel.tsx`
- Test: `src/components/channels/__tests__/ReportingPanel.test.tsx`

- [ ] **Step 1: Write a failing render test for the new settings owner panel**

At minimum assert:
- owner section heading is rendered
- empty state is rendered with no owners
- a saved owner row can render masked credentials / status text

- [ ] **Step 2: Run the test and verify the new component does not exist**

Run:

```bash
npx tsx --test src/components/channels/__tests__/ReportingPanel.test.tsx
```

Expected:
- FAIL because the target component file is missing.

- [ ] **Step 3: Implement the Owner panel components**

UI requirements:
- owner list
- create/edit/delete controls
- proxy test action
- usage summary
- recent request logs
- binding list with enable toggles and manual sync buttons

Keep owner editor state out of `SettingsPage.tsx` where possible.

- [ ] **Step 4: Integrate the panel into `src/pages/SettingsPage.tsx`**

Load Reporting data alongside existing settings, but keep existing YouTube API key and yt-dlp cookie flows intact.

- [ ] **Step 5: Re-run the component test and full frontend build**

Run:

```bash
npx tsx --test src/components/channels/__tests__/ReportingPanel.test.tsx
npm run build
```

Expected:
- component test PASS
- frontend build PASS

- [ ] **Step 6: Commit**

```bash
git add src/pages/SettingsPage.tsx src/components/settings/ReportingOwnersPanel.tsx src/components/settings/OwnerEditorModal.tsx src/components/settings/OwnerBindingsPanel.tsx src/components/settings/OwnerRequestLogPanel.tsx src/components/channels/__tests__/ReportingPanel.test.tsx
git commit -m "feat: add reporting owners settings ui"
```

## Task 9: Add the channel Reporting tab and views

**Files:**
- Modify: `src/pages/ChannelsPage.tsx`
- Create: `src/components/channels/ReportingPanel.tsx`
- Test: `src/components/channels/__tests__/ReportingPanel.test.tsx`

- [ ] **Step 1: Write failing tests for the channel Reporting panel**

Cover:
- empty state when Reporting is disabled
- KPI labels for the four daily metrics
- traffic source section
- video row rendering

Example:

```tsx
test('reporting panel renders disabled empty state', () => {
  const html = renderToStaticMarkup(
    <ReportingPanel
      enabled={false}
      ownerName={null}
      summary={null}
      dailyRows={[]}
      videos={[]}
      loading={false}
      onSync={() => {}}
    />,
  )

  assert.match(html, /尚未启用 Reporting API/)
})
```

- [ ] **Step 2: Run the panel test and verify it fails**

Run:

```bash
npx tsx --test src/components/channels/__tests__/ReportingPanel.test.tsx
```

Expected:
- FAIL because `ReportingPanel.tsx` is missing or incomplete.

- [ ] **Step 3: Implement `src/components/channels/ReportingPanel.tsx`**

Render:
- disabled empty state
- latest summary cards
- traffic source share card
- daily table
- video metrics table
- manual sync button

- [ ] **Step 4: Wire the tab into `src/pages/ChannelsPage.tsx`**

Add:
- new tab key such as `reports`
- data loaders for summary/daily/videos
- refresh behavior aligned with the current tab
- export logic only if it remains clear and non-disruptive

- [ ] **Step 5: Re-run the panel test and frontend build**

Run:

```bash
npx tsx --test src/components/channels/__tests__/ReportingPanel.test.tsx
npm run build
```

Expected:
- PASS
- frontend build PASS

- [ ] **Step 6: Commit**

```bash
git add src/pages/ChannelsPage.tsx src/components/channels/ReportingPanel.tsx src/components/channels/__tests__/ReportingPanel.test.tsx
git commit -m "feat: add channel reporting tab"
```

## Task 10: Full verification and regression pass

**Files:**
- Modify only if verification exposes issues.

- [ ] **Step 1: Run focused backend tests**

```bash
npx tsx --test server/src/services/__tests__/reportingOwners.test.ts
npx tsx --test server/src/services/__tests__/reportingProxyProbe.test.ts
npx tsx --test server/src/services/__tests__/youtubeReportingImport.test.ts
npx tsx --test server/src/services/__tests__/youtubeReportingDerive.test.ts
npx tsx --test server/src/routes/__tests__/reportingRouteHelpers.test.ts
```

Expected:
- all PASS

- [ ] **Step 2: Run focused frontend tests**

```bash
npx tsx --test src/lib/__tests__/reportingSettingsForm.test.ts
npx tsx --test src/components/channels/__tests__/ReportingPanel.test.tsx
```

Expected:
- all PASS

- [ ] **Step 3: Run project builds**

```bash
npm run build:server
npm run build
```

Expected:
- both PASS with no TypeScript errors

- [ ] **Step 4: Manually smoke test the core flows**

Verify in the running app:
- existing settings save still works
- existing channel `数据洞察` still loads
- a Reporting Owner can be created and edited
- proxy test returns a visible result
- a channel can be bound and enabled
- a channel with no binding shows the empty report state
- a bound channel shows the `报表` tab and loads data or a clear “no imported reports yet” state

- [ ] **Step 5: Commit any verification fixes**

```bash
git add <only the files changed during verification>
git commit -m "fix: resolve reporting verification issues"
```
