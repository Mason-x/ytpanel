import test from 'node:test';
import assert from 'node:assert/strict';
import { resolveVideoAvailabilityFilter } from '../videoAvailabilityFilter';

test('resolveVideoAvailabilityFilter defaults to available', () => {
  assert.equal(resolveVideoAvailabilityFilter(undefined, 'most_recent'), 'available');
  assert.equal(resolveVideoAvailabilityFilter('', ''), 'available');
});

test('resolveVideoAvailabilityFilter respects explicit availability query', () => {
  assert.equal(resolveVideoAvailabilityFilter('unavailable', 'most_recent'), 'unavailable');
  assert.equal(resolveVideoAvailabilityFilter('available', 'unavailable_recent'), 'available');
});

test('resolveVideoAvailabilityFilter falls back to unavailable for unavailable sort', () => {
  assert.equal(resolveVideoAvailabilityFilter(undefined, 'unavailable_recent'), 'unavailable');
});
