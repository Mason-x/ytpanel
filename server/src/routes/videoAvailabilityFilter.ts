const VALID_AVAILABILITY_VALUES = new Set(['available', 'unavailable']);

export function resolveVideoAvailabilityFilter(rawAvailability: unknown, rawSort: unknown): string {
  const availability = String(rawAvailability || '').trim().toLowerCase();
  if (VALID_AVAILABILITY_VALUES.has(availability)) return availability;

  const sort = String(rawSort || '').trim().toLowerCase();
  if (sort === 'unavailable_recent') return 'unavailable';

  return 'available';
}
