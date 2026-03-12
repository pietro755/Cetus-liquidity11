/**
 * Unit tests for services/monitor.ts – PositionMonitorService
 *
 * Only the pure, synchronous helpers are tested here; network calls are
 * not exercised.
 */

import { PositionMonitorService } from '../services/monitor';

// PositionMonitorService constructor needs an sdkService and config, but
// isPositionInRange is a pure method that doesn't use them.  Pass minimal stubs.
const stubSdkService = {} as any;
const stubConfig = {} as any;

describe('PositionMonitorService.isPositionInRange', () => {
  const svc = new PositionMonitorService(stubSdkService, stubConfig);

  it('returns true when currentTick is exactly at tickLower', () => {
    expect(svc.isPositionInRange(-100, 200, -100)).toBe(true);
  });

  it('returns true when currentTick is exactly at tickUpper', () => {
    expect(svc.isPositionInRange(-100, 200, 200)).toBe(true);
  });

  it('returns true when currentTick is strictly inside the range', () => {
    expect(svc.isPositionInRange(-100, 200, 50)).toBe(true);
  });

  it('returns false when currentTick is below tickLower', () => {
    expect(svc.isPositionInRange(-100, 200, -101)).toBe(false);
  });

  it('returns false when currentTick is above tickUpper', () => {
    expect(svc.isPositionInRange(-100, 200, 201)).toBe(false);
  });

  it('handles a zero-width range (tickLower === tickUpper)', () => {
    expect(svc.isPositionInRange(0, 0, 0)).toBe(true);
    expect(svc.isPositionInRange(0, 0, 1)).toBe(false);
    expect(svc.isPositionInRange(0, 0, -1)).toBe(false);
  });

  it('works with negative tick values', () => {
    expect(svc.isPositionInRange(-500, -100, -300)).toBe(true);
    expect(svc.isPositionInRange(-500, -100, -99)).toBe(false);
    expect(svc.isPositionInRange(-500, -100, -501)).toBe(false);
  });
});
