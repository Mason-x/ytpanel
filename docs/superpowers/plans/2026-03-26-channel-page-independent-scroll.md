# Channel Page Independent Scroll Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Lock the channel page to the desktop first viewport and make the tag list, channel list, and channel detail content scroll independently.

**Architecture:** Keep the current React component tree intact and solve the behavior with CSS height-chain fixes plus a minimal class hook if needed. The desktop layout will establish a fixed viewport-bounded shell, while mobile media queries continue to opt out and use natural page flow.

**Tech Stack:** React 19, TypeScript, Vite, CSS

---

## File Map

- Modify: `src/index.css`
  - Own the desktop height chain for `main-container`, `main-content`, `channel-page-shell`, sidebar columns, and detail scroll region.
  - Preserve existing mobile overrides under current media queries.
- Modify: `src/pages/ChannelsPage.tsx`
  - Only if needed to add a dedicated class hook around the empty state or main shell; avoid changing data logic.

## Task 1: Establish a Desktop Viewport-Bounded Height Chain

**Files:**
- Modify: `src/index.css`
- Modify: `src/pages/ChannelsPage.tsx` (only if a new wrapper/class is required)

- [ ] **Step 1: Capture the current desktop behavior to compare against after the CSS change**

Manual check on `/channels` in a desktop-width viewport:
- Scroll the page with a long channel detail list.
- Confirm the current page grows with content and the left sidebar height feels tied to the detail region.
- Note whether the empty-state branch uses the same layout shell as the populated branch.

- [ ] **Step 2: Add the desktop-only height chain in `src/index.css`**

Update these selectors so desktop layout has a stable bounded height:
- `.main-container`
- `.main-content`
- `.channel-page-shell`
- `.channel-main-column`
- `.sidebar`
- `.sidebar-dual-layout`

Implementation rules:
- Use pure CSS and `100dvh`.
- Subtract the sticky navbar height and desktop outer padding from the usable page height.
- Add `min-height: 0` anywhere a flex child must pass height to an inner scroller.
- Keep `overflow: hidden` on the shell-level containers that define scroll boundaries.
- Do not apply this fixed-height behavior inside the existing mobile breakpoints.

- [ ] **Step 3: Keep both channel page branches inside the same bounded shell**

If the empty-state branch in `src/pages/ChannelsPage.tsx` bypasses any class needed for the desktop height chain, add the smallest possible class hook so both:
- the populated page
- the empty state page

inherit the same desktop shell behavior.

- [ ] **Step 4: Verify the desktop shell no longer expands with detail content**

Manual check on `/channels` in desktop width:
- The overall channel page stays inside the first viewport.
- The browser page itself does not become the primary scroller for long channel details.
- The right detail pane still renders the channel header at the top.

## Task 2: Split Sidebar and Detail Regions into Independent Scroll Containers

**Files:**
- Modify: `src/index.css`

- [ ] **Step 1: Refine the sidebar panels so each region owns its own scroll**

Update these selectors:
- `.sidebar-tag-panel`
- `.sidebar-tag-list`
- `.sidebar-channel-panel`
- `.sidebar-channel-list`
- `.sidebar-footer-count`

Implementation rules:
- Tag search stays fixed at the top of the tag panel.
- Tag list remains the only scrollable region in the tag panel.
- Channel search stays fixed at the top of the channel panel.
- Channel footer stays pinned to the bottom of the channel panel.
- Channel list remains the only scrollable region between the search box and footer.

- [ ] **Step 2: Preserve the right-side split of fixed header plus scrollable content**

Confirm and, if necessary, adjust:
- `.channel-main-column`
- `.channel-header`
- `.channel-content-scroll`

Implementation rules:
- Channel header must not scroll with the content list.
- `.channel-content-scroll` must remain the only detail scroller.
- The sticky video toolbar must continue to stick relative to `.channel-content-scroll`.

- [ ] **Step 3: Run a desktop manual verification pass for all three scroll regions**

Manual checks:
- Scroll the tag list without moving the channel list or detail pane.
- Scroll the channel list without moving the tag list or detail pane.
- Scroll the detail pane without moving the two left regions.
- Switch between `视频列表` and `数据洞察` and confirm the detail pane still owns scrolling.
- Confirm the channel header stays visible while the detail content scrolls.

## Task 3: Protect Mobile Behavior and Run Build Verification

**Files:**
- Modify: `src/index.css`

- [ ] **Step 1: Re-check the existing mobile breakpoints**

Review the current `@media (max-width: 1200px)` and `@media (max-width: 760px)` blocks in `src/index.css`.

Ensure they still explicitly restore:
- `height: auto` where needed
- column stacking
- natural page scrolling

- [ ] **Step 2: Build the frontend to catch CSS/TS regressions**

Run: `npm run build`

Expected:
- TypeScript compilation succeeds
- Vite build succeeds
- No new errors from `ChannelsPage.tsx`

- [ ] **Step 3: Run a final mobile smoke check**

Manual check in a narrow/mobile viewport:
- The channel page stacks vertically.
- No section is clipped by a fixed desktop height.
- The page remains scrollable in normal document flow.

- [ ] **Step 4: Commit**

```bash
git add src/index.css src/pages/ChannelsPage.tsx docs/superpowers/specs/2026-03-26-channel-page-independent-scroll-design.md docs/superpowers/plans/2026-03-26-channel-page-independent-scroll.md
git commit -m "fix: decouple channel page desktop scroll regions"
```
