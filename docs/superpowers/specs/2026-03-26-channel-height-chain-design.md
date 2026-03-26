# Channel page height chain (Task 1)

## Context

- Task 1 requires a desktop-only height chain for the channel page, subtracting the sticky navbar (56px) and the desktop outer padding (`var(--space-lg)` top and bottom) from `100dvh`. The goal is to keep the shell bounded by CSS while mobile breakpoints revert to height:auto.
- The shell containers involved are `.main-container`, `.main-content`, `.channel-page-shell`, `.channel-main-column`, `.sidebar`, and `.sidebar-dual-layout`. All of these already participate in flex layouts with scrollable children.

## Requirements

1. Use pure CSS to bound the channel page height chain on desktop without affecting mobile breakpoints.
2. Keep `overflow: hidden` on the shell-level containers that define scroll boundaries.
3. Make sure flex children pass height to inner scrollers by supplying `min-height: 0`.
4. Ensure both the populated and empty channel-page branches use the same bounded shell behavior.

## Proposed solution

1. Move the desktop-specific `height` rules into a `@media (min-width: 901px)` block so only viewports wider than the existing 900px breakpoint receive the `calc(100dvh - 56px - 2 * var(--space-lg))` height on `.main-container`. All other breakpoints keep the current height:auto overrides.
2. Within the same desktop media query, give `.main-content`, `.channel-page-shell`, `.channel-main-column`, `.sidebar`, and `.sidebar-dual-layout` a `height: 100%` that threads the calculated value down the nesting chain without altering their existing `min-height: 0` or `overflow: hidden` rules.
3. Rely on the existing `@media (max-width: 900px)` overrides to reset these selectors back to `height: auto` and `overflow: visible` so the mobile experience keeps flowing height to content naturally.

## Verification

- Run `npm run build` to ensure the CSS change does not break the build.

## Open questions

- Should the desktop-only height chain include any additional offset for other headers or inline controls in the future libraries? As structured today, 56px for the sticky navbar and double padding is the full offset.
