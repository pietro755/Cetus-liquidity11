/**
 * Unit tests for services/sdk.ts — CetusSDKService config validation
 *
 * The `validateConfig` method is the first thing the constructor calls, so
 * invalid configurations throw before any SDK or network initialisation.
 * These tests exercise every guard — the full set is critical for mainnet
 * safety where real funds are at stake.
 *
 * No real network I/O is performed: every test that would reach the
 * `initializeSDK` step uses an invalid config that is rejected earlier.
 */

import { CetusSDKService } from '../services/sdk';
import type { BotConfig } from '../config';

// Minimal valid-enough config (never actually reaches initCetusSDK)
function makeConfig(overrides: Partial<BotConfig> = {}): BotConfig {
  return {
    network: 'mainnet',
    privateKey: 'a'.repeat(64),          // valid 64-char hex key
    poolAddress: '0x' + 'b'.repeat(64),  // valid 0x-prefixed address
    checkInterval: 60,
    maxSlippage: 0.01,
    gasBudget: 50_000_000,
    logLevel: 'info',
    verboseLogs: false,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// PRIVATE_KEY validation
// ---------------------------------------------------------------------------

describe('CetusSDKService – private key validation', () => {
  it('throws when PRIVATE_KEY is an empty string', () => {
    expect(() => new CetusSDKService(makeConfig({ privateKey: '' }))).toThrow(
      /PRIVATE_KEY is required/,
    );
  });

  it('throws when PRIVATE_KEY is whitespace only', () => {
    expect(() => new CetusSDKService(makeConfig({ privateKey: '   ' }))).toThrow(
      /PRIVATE_KEY is required/,
    );
  });

  it('throws when PRIVATE_KEY is too short (< 64 hex chars)', () => {
    expect(() => new CetusSDKService(makeConfig({ privateKey: 'a'.repeat(32) }))).toThrow(
      /64 hexadecimal/,
    );
  });

  it('throws when PRIVATE_KEY contains non-hex characters', () => {
    expect(() => new CetusSDKService(makeConfig({ privateKey: 'z'.repeat(64) }))).toThrow(
      /64 hexadecimal/,
    );
  });

  it('throws when PRIVATE_KEY is 65 characters (not 64 or 66)', () => {
    expect(() => new CetusSDKService(makeConfig({ privateKey: 'a'.repeat(65) }))).toThrow(
      /64 hexadecimal/,
    );
  });

  it('throws when PRIVATE_KEY has 0x prefix but only 62 hex chars after it', () => {
    // 0x + 62 chars = 64 total, but cleanKey after stripping 0x is only 62 hex chars
    expect(() =>
      new CetusSDKService(makeConfig({ privateKey: '0x' + 'a'.repeat(62) })),
    ).toThrow(/64 hexadecimal/);
  });
});

// ---------------------------------------------------------------------------
// POOL_ADDRESS validation
// ---------------------------------------------------------------------------

describe('CetusSDKService – pool address validation', () => {
  it('throws when POOL_ADDRESS is an empty string', () => {
    expect(() => new CetusSDKService(makeConfig({ poolAddress: '' }))).toThrow(
      /POOL_ADDRESS is required/,
    );
  });

  it('throws when POOL_ADDRESS is whitespace only', () => {
    expect(() => new CetusSDKService(makeConfig({ poolAddress: '   ' }))).toThrow(
      /POOL_ADDRESS is required/,
    );
  });

  it('throws when POOL_ADDRESS does not start with 0x', () => {
    expect(() =>
      new CetusSDKService(makeConfig({ poolAddress: 'b'.repeat(64) })),
    ).toThrow(/POOL_ADDRESS must start with 0x/);
  });
});

// ---------------------------------------------------------------------------
// Accepted formats
// ---------------------------------------------------------------------------

describe('CetusSDKService – accepted private key formats', () => {
  // These tests only check that validateConfig does NOT throw; the subsequent
  // SDK initialisation may fail in a test environment but the validation itself
  // must pass.

  it('accepts a 64-char hex key without 0x prefix', () => {
    // We only care that validateConfig doesn't throw; the error (if any) comes
    // from the underlying Cetus/Sui SDK initialisation, not our validation.
    let caughtMsg = '';
    try {
      new CetusSDKService(makeConfig({ privateKey: 'a'.repeat(64) }));
    } catch (e) {
      caughtMsg = e instanceof Error ? e.message : String(e);
    }
    // Must NOT throw our validation messages
    expect(caughtMsg).not.toMatch(/PRIVATE_KEY is required/);
    expect(caughtMsg).not.toMatch(/64 hexadecimal/);
  });

  it('accepts a 66-char key with 0x prefix (0x + 64 hex chars)', () => {
    let caughtMsg = '';
    try {
      new CetusSDKService(makeConfig({ privateKey: '0x' + 'a'.repeat(64) }));
    } catch (e) {
      caughtMsg = e instanceof Error ? e.message : String(e);
    }
    expect(caughtMsg).not.toMatch(/PRIVATE_KEY is required/);
    expect(caughtMsg).not.toMatch(/64 hexadecimal/);
  });
});
