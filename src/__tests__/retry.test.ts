/**
 * Unit tests for utils/retry.ts
 *
 * Tests isNetworkError pattern matching and retryWithBackoff retry behaviour.
 * No real network calls are made — all async operations are Jest mock functions.
 */

import { isNetworkError, retryWithBackoff } from '../utils/retry';

// ---------------------------------------------------------------------------
// isNetworkError
// ---------------------------------------------------------------------------

describe('isNetworkError', () => {
  const transientMessages = [
    'fetch failed',
    'network error occurred',
    'ECONNREFUSED 127.0.0.1:9000',
    'ECONNRESET by peer',
    'ETIMEDOUT after 30s',
    'ENOTFOUND fullnode.mainnet.sui.io',
    'timeout waiting for response',
    'socket hang up',
    'EAI_AGAIN (DNS temporary failure)',
    'EHOSTUNREACH 192.168.1.1',
    'EPIPE broken pipe',
    'request to https://rpc.example.com failed',
    'getaddrinfo ENOTFOUND',
  ];

  it.each(transientMessages)(
    'returns true for transient message: %s',
    (msg) => {
      expect(isNetworkError(msg)).toBe(true);
    },
  );

  const permanentMessages = [
    'Transaction failed: InvalidSignature',
    'InsufficientFunds',
    'Pool not found: 0xabc',
    'Position liquidity is missing or zero',
    'PRIVATE_KEY must be exactly 64 hexadecimal characters',
    'Unexpected end of JSON input',
    'Cannot read properties of undefined',
  ];

  it.each(permanentMessages)(
    'returns false for non-network message: %s',
    (msg) => {
      expect(isNetworkError(msg)).toBe(false);
    },
  );

  it('is case-insensitive (upper-case FETCH)', () => {
    expect(isNetworkError('FETCH failed')).toBe(true);
  });

  it('returns false for an empty string', () => {
    expect(isNetworkError('')).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// retryWithBackoff — success on first attempt
// ---------------------------------------------------------------------------

describe('retryWithBackoff – success on first attempt', () => {
  it('returns the resolved value immediately', async () => {
    const op = jest.fn().mockResolvedValue('ok');
    await expect(retryWithBackoff(op, 'test-op', 3, 0)).resolves.toBe('ok');
    expect(op).toHaveBeenCalledTimes(1);
  });
});

// ---------------------------------------------------------------------------
// retryWithBackoff — retries on network error then succeeds
// ---------------------------------------------------------------------------

describe('retryWithBackoff – retries on transient network error', () => {
  it('succeeds on the second attempt after one network error', async () => {
    const op = jest.fn()
      .mockRejectedValueOnce(new Error('ECONNRESET'))
      .mockResolvedValueOnce('success');

    await expect(retryWithBackoff(op, 'test-op', 3, 0)).resolves.toBe('success');
    expect(op).toHaveBeenCalledTimes(2);
  });

  it('succeeds on the third attempt after two network errors', async () => {
    const op = jest.fn()
      .mockRejectedValueOnce(new Error('timeout'))
      .mockRejectedValueOnce(new Error('fetch failed'))
      .mockResolvedValueOnce('data');

    await expect(retryWithBackoff(op, 'test-op', 3, 0)).resolves.toBe('data');
    expect(op).toHaveBeenCalledTimes(3);
  });
});

// ---------------------------------------------------------------------------
// retryWithBackoff — does NOT retry on non-network errors
// ---------------------------------------------------------------------------

describe('retryWithBackoff – does not retry on non-network errors', () => {
  it('throws immediately on a permanent error without retrying', async () => {
    const op = jest.fn().mockRejectedValue(new Error('InsufficientFunds'));

    await expect(retryWithBackoff(op, 'test-op', 3, 0)).rejects.toThrow('InsufficientFunds');
    // Called only once — no retry
    expect(op).toHaveBeenCalledTimes(1);
  });
});

// ---------------------------------------------------------------------------
// retryWithBackoff — exhausts all retries and throws
// ---------------------------------------------------------------------------

describe('retryWithBackoff – exhausts retries', () => {
  it('throws the last error after maxRetries network failures', async () => {
    const op = jest.fn().mockRejectedValue(new Error('ECONNREFUSED'));

    await expect(retryWithBackoff(op, 'test-op', 2, 0)).rejects.toThrow('ECONNREFUSED');
    // 1 initial attempt + 2 retries = 3 total calls
    expect(op).toHaveBeenCalledTimes(3);
  });
});
