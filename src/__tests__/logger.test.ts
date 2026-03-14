/**
 * Unit tests for utils/logger.ts — Logger
 *
 * Verifies that the Logger class formats messages correctly and that the
 * error() method always includes the Error message in its output regardless
 * of whether verbose mode is enabled.
 */

import { logger } from '../utils/logger';

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

/** Reset the module registry and return a freshly-instantiated logger. */
function getFreshLogger(): typeof logger {
  jest.resetModules();
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  return require('../utils/logger').logger;
}

// ---------------------------------------------------------------------------
// Logger.error — always includes the Error message
// ---------------------------------------------------------------------------

describe('Logger.error – always includes the Error message', () => {
  let errorSpy: jest.SpyInstance;

  beforeEach(() => {
    errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    errorSpy.mockRestore();
    delete process.env.VERBOSE_LOGS;
    delete process.env.LOG_LEVEL;
  });

  it('includes the error message in non-verbose mode', () => {
    process.env.VERBOSE_LOGS = 'false';
    const freshLogger = getFreshLogger();

    freshLogger.error('Rebalance failed', new Error('something went wrong'));

    expect(errorSpy).toHaveBeenCalledTimes(1);
    const logged = errorSpy.mock.calls[0][0] as string;
    expect(logged).toContain('[ERROR]');
    expect(logged).toContain('Rebalance failed');
    // The actual error message must appear without verbose mode.
    expect(logged).toContain('something went wrong');
  });

  it('includes the error message in verbose mode', () => {
    process.env.VERBOSE_LOGS = 'true';
    const freshLogger = getFreshLogger();

    freshLogger.error('Rebalance failed', new Error('something went wrong'));

    expect(errorSpy).toHaveBeenCalledTimes(1);
    const logged = errorSpy.mock.calls[0][0] as string;
    expect(logged).toContain('[ERROR]');
    expect(logged).toContain('Rebalance failed');
    expect(logged).toContain('something went wrong');
  });

  it('does not append anything extra when no error is provided', () => {
    process.env.VERBOSE_LOGS = 'false';
    const freshLogger = getFreshLogger();

    freshLogger.error('Simple error message');

    expect(errorSpy).toHaveBeenCalledTimes(1);
    const logged = errorSpy.mock.calls[0][0] as string;
    expect(logged).toContain('[ERROR] Simple error message');
    // No extra colon or undefined appended
    expect(logged).not.toMatch(/:\s*$/);
    expect(logged).not.toContain('undefined');
  });
});

// ---------------------------------------------------------------------------
// Logger singleton
// ---------------------------------------------------------------------------

describe('Logger singleton', () => {
  it('exports a logger instance with an error method', () => {
    expect(typeof logger.error).toBe('function');
    expect(typeof logger.warn).toBe('function');
    expect(typeof logger.info).toBe('function');
    expect(typeof logger.debug).toBe('function');
  });
});
