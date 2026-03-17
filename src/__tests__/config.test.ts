/**
 * Unit tests for config/index.ts
 *
 * These tests exercise the environment-variable parsing without touching
 * the network or any external services.
 */

// Keep a snapshot of the real env so we can restore it after each test.
const originalEnv = { ...process.env };

afterEach(() => {
  // Restore env and flush the module cache so loadConfig() sees fresh values.
  process.env = { ...originalEnv };
  jest.resetModules();
});

function loadFresh() {
  // Re-require after resetModules so dotenv / process.env is re-evaluated.
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  return require('../config') as typeof import('../config');
}

const VALID_KEY = 'a'.repeat(64);
const VALID_POOL = '0x' + 'b'.repeat(64);

describe('loadConfig – network', () => {
  it('defaults to mainnet when NETWORK is not set', () => {
    process.env.PRIVATE_KEY = VALID_KEY;
    process.env.POOL_ADDRESS = VALID_POOL;
    delete process.env.NETWORK;

    const { loadConfig } = loadFresh();
    expect(loadConfig().network).toBe('mainnet');
  });

  it('accepts "testnet"', () => {
    process.env.NETWORK = 'testnet';
    process.env.PRIVATE_KEY = VALID_KEY;
    process.env.POOL_ADDRESS = VALID_POOL;

    const { loadConfig } = loadFresh();
    expect(loadConfig().network).toBe('testnet');
  });

  it('accepts explicit "mainnet"', () => {
    process.env.NETWORK = 'mainnet';
    process.env.PRIVATE_KEY = VALID_KEY;
    process.env.POOL_ADDRESS = VALID_POOL;

    const { loadConfig } = loadFresh();
    expect(loadConfig().network).toBe('mainnet');
  });

  it('rejects an unknown network value', () => {
    process.env.NETWORK = 'devnet';
    process.env.PRIVATE_KEY = VALID_KEY;
    process.env.POOL_ADDRESS = VALID_POOL;

    // The module-level `export const config = loadConfig()` runs on require(),
    // so the error is thrown there rather than on an explicit loadConfig() call.
    expect(() => loadFresh()).toThrow(/Invalid NETWORK/);
  });
});

describe('loadConfig – required variables', () => {
  it('throws when PRIVATE_KEY is missing', () => {
    process.env.POOL_ADDRESS = VALID_POOL;
    delete process.env.PRIVATE_KEY;

    // Module-level loadConfig() runs on require(); error is thrown there.
    expect(() => loadFresh()).toThrow(/PRIVATE_KEY/);
  });

  it('throws when POOL_ADDRESS is missing', () => {
    process.env.PRIVATE_KEY = VALID_KEY;
    delete process.env.POOL_ADDRESS;

    expect(() => loadFresh()).toThrow(/POOL_ADDRESS/);
  });
});

describe('loadConfig – optional numeric & boolean variables', () => {
  beforeEach(() => {
    process.env.PRIVATE_KEY = VALID_KEY;
    process.env.POOL_ADDRESS = VALID_POOL;
  });

  it('uses CHECK_INTERVAL from env', () => {
    process.env.CHECK_INTERVAL = '120';
    const { loadConfig } = loadFresh();
    expect(loadConfig().checkInterval).toBe(120);
  });

  it('defaults CHECK_INTERVAL to 60 when not set', () => {
    delete process.env.CHECK_INTERVAL;
    const { loadConfig } = loadFresh();
    expect(loadConfig().checkInterval).toBe(60);
  });

  it('reads MAX_SLIPPAGE as a float', () => {
    process.env.MAX_SLIPPAGE = '0.005';
    const { loadConfig } = loadFresh();
    expect(loadConfig().maxSlippage).toBeCloseTo(0.005);
  });

  it('defaults MAX_SLIPPAGE to 0.01', () => {
    delete process.env.MAX_SLIPPAGE;
    const { loadConfig } = loadFresh();
    expect(loadConfig().maxSlippage).toBe(0.01);
  });

  it('reads GAS_BUDGET as a number', () => {
    process.env.GAS_BUDGET = '100000000';
    const { loadConfig } = loadFresh();
    expect(loadConfig().gasBudget).toBe(100000000);
  });

  it('parses LOWER_TICK and UPPER_TICK when both are set', () => {
    process.env.LOWER_TICK = '-100';
    process.env.UPPER_TICK = '200';
    const { loadConfig } = loadFresh();
    const cfg = loadConfig();
    expect(cfg.lowerTick).toBe(-100);
    expect(cfg.upperTick).toBe(200);
  });

  it('leaves lowerTick / upperTick undefined when not set', () => {
    delete process.env.LOWER_TICK;
    delete process.env.UPPER_TICK;
    const { loadConfig } = loadFresh();
    const cfg = loadConfig();
    expect(cfg.lowerTick).toBeUndefined();
    expect(cfg.upperTick).toBeUndefined();
  });

  it('parses RANGE_WIDTH when set', () => {
    process.env.RANGE_WIDTH = '400';
    const { loadConfig } = loadFresh();
    const cfg = loadConfig();
    expect(cfg.rangeWidth).toBe(400);
  });

  it('leaves rangeWidth undefined when RANGE_WIDTH is not set', () => {
    delete process.env.RANGE_WIDTH;
    const { loadConfig } = loadFresh();
    const cfg = loadConfig();
    expect(cfg.rangeWidth).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// TOKEN_A_AMOUNT / TOKEN_B_AMOUNT validation
// ---------------------------------------------------------------------------

describe('loadConfig – TOKEN_A_AMOUNT / TOKEN_B_AMOUNT validation', () => {
  beforeEach(() => {
    process.env.PRIVATE_KEY = VALID_KEY;
    process.env.POOL_ADDRESS = VALID_POOL;
    delete process.env.TOKEN_A_AMOUNT;
    delete process.env.TOKEN_B_AMOUNT;
  });

  it('accepts a valid positive integer for TOKEN_A_AMOUNT', () => {
    process.env.TOKEN_A_AMOUNT = '1000000';
    const { loadConfig } = loadFresh();
    expect(loadConfig().tokenAAmount).toBe('1000000');
  });

  it('accepts a valid positive integer for TOKEN_B_AMOUNT', () => {
    process.env.TOKEN_B_AMOUNT = '500000';
    const { loadConfig } = loadFresh();
    expect(loadConfig().tokenBAmount).toBe('500000');
  });

  it('leaves tokenAAmount undefined when TOKEN_A_AMOUNT is not set', () => {
    const { loadConfig } = loadFresh();
    expect(loadConfig().tokenAAmount).toBeUndefined();
  });

  it('leaves tokenBAmount undefined when TOKEN_B_AMOUNT is not set', () => {
    const { loadConfig } = loadFresh();
    expect(loadConfig().tokenBAmount).toBeUndefined();
  });

  it('rejects a non-numeric TOKEN_A_AMOUNT', () => {
    process.env.TOKEN_A_AMOUNT = 'abc';
    expect(() => loadFresh()).toThrow(/TOKEN_A_AMOUNT/);
  });

  it('rejects a non-numeric TOKEN_B_AMOUNT', () => {
    process.env.TOKEN_B_AMOUNT = 'xyz';
    expect(() => loadFresh()).toThrow(/TOKEN_B_AMOUNT/);
  });

  it('rejects TOKEN_A_AMOUNT of zero', () => {
    process.env.TOKEN_A_AMOUNT = '0';
    expect(() => loadFresh()).toThrow(/TOKEN_A_AMOUNT/);
  });

  it('rejects TOKEN_B_AMOUNT of zero', () => {
    process.env.TOKEN_B_AMOUNT = '0';
    expect(() => loadFresh()).toThrow(/TOKEN_B_AMOUNT/);
  });

  it('rejects a negative TOKEN_A_AMOUNT', () => {
    process.env.TOKEN_A_AMOUNT = '-500';
    expect(() => loadFresh()).toThrow(/TOKEN_A_AMOUNT/);
  });

  it('rejects a negative TOKEN_B_AMOUNT', () => {
    process.env.TOKEN_B_AMOUNT = '-1';
    expect(() => loadFresh()).toThrow(/TOKEN_B_AMOUNT/);
  });

  it('rejects a decimal TOKEN_A_AMOUNT', () => {
    process.env.TOKEN_A_AMOUNT = '1.5';
    expect(() => loadFresh()).toThrow(/TOKEN_A_AMOUNT/);
  });

  it('rejects a decimal TOKEN_B_AMOUNT', () => {
    process.env.TOKEN_B_AMOUNT = '2.5';
    expect(() => loadFresh()).toThrow(/TOKEN_B_AMOUNT/);
  });
});

describe('loadConfig – maxSlippage safety guard', () => {
  beforeEach(() => {
    process.env.PRIVATE_KEY = VALID_KEY;
    process.env.POOL_ADDRESS = VALID_POOL;
    process.env.NETWORK = 'mainnet';
  });

  it('accepts the default slippage of 0.01 (1 %)', () => {
    delete process.env.MAX_SLIPPAGE;
    const { loadConfig } = loadFresh();
    expect(loadConfig().maxSlippage).toBeCloseTo(0.01);
  });

  it('accepts a custom in-range value such as 0.005 (0.5 %)', () => {
    process.env.MAX_SLIPPAGE = '0.005';
    const { loadConfig } = loadFresh();
    expect(loadConfig().maxSlippage).toBeCloseTo(0.005);
  });

  it('accepts the maximum safe value of 0.99 (just below 100 %)', () => {
    process.env.MAX_SLIPPAGE = '0.99';
    const { loadConfig } = loadFresh();
    expect(loadConfig().maxSlippage).toBeCloseTo(0.99);
  });

  it('rejects a value of 0 (no slippage would always fail on-chain)', () => {
    process.env.MAX_SLIPPAGE = '0';
    // Module-level loadConfig() throws on require()
    expect(() => loadFresh()).toThrow(/MAX_SLIPPAGE/);
  });

  it('rejects a value of 1.0 (100 % slippage — catastrophic on mainnet)', () => {
    process.env.MAX_SLIPPAGE = '1.0';
    expect(() => loadFresh()).toThrow(/MAX_SLIPPAGE/);
  });

  it('rejects a value of 2.0 (200 % slippage — obviously wrong)', () => {
    process.env.MAX_SLIPPAGE = '2.0';
    expect(() => loadFresh()).toThrow(/MAX_SLIPPAGE/);
  });

  it('rejects a negative value', () => {
    process.env.MAX_SLIPPAGE = '-0.5';
    expect(() => loadFresh()).toThrow(/MAX_SLIPPAGE/);
  });
});
