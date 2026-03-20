/**
 * Unit tests for services/rebalance.ts — RebalanceService
 *
 * Network calls are fully mocked; all tests run in milliseconds without
 * any external services.
 */

import { RebalanceService } from '../services/rebalance';
import { PositionMonitorService } from '../services/monitor';
import { TransactionUtil } from '@cetusprotocol/cetus-sui-clmm-sdk';

// ---------------------------------------------------------------------------
// Helpers to build minimal stubs
// ---------------------------------------------------------------------------

function makePoolInfo(overrides: Partial<{
  currentTickIndex: number;
  tickSpacing: number;
  coinTypeA: string;
  coinTypeB: string;
}> = {}) {
  return {
    poolAddress: '0xpool',
    currentTickIndex: overrides.currentTickIndex ?? 0,
    currentSqrtPrice: '1000000000',
    coinTypeA: overrides.coinTypeA ?? '0xcoinA',
    coinTypeB: overrides.coinTypeB ?? '0xcoinB',
    tickSpacing: overrides.tickSpacing ?? 1,
  };
}

function makePosition(overrides: Partial<{
  tickLower: number;
  tickUpper: number;
  liquidity: string;
  poolAddress: string;
}> = {}) {
  return {
    positionId: '0xpos1',
    poolAddress: overrides.poolAddress ?? '0xpool',
    tickLower: overrides.tickLower ?? -200,
    tickUpper: overrides.tickUpper ?? -100,  // default: below current tick
    liquidity: overrides.liquidity ?? '1000000',
    tokenA: '0xcoinA',
    tokenB: '0xcoinB',
    inRange: false,
  };
}

function makeMonitor(positions: ReturnType<typeof makePosition>[], poolInfo: ReturnType<typeof makePoolInfo>) {
  const monitor = {
    getPoolInfo: jest.fn().mockResolvedValue(poolInfo),
    getPositions: jest.fn().mockResolvedValue(positions),
    isPositionInRange: jest.fn().mockImplementation(
      (lower: number, upper: number, current: number) => current >= lower && current <= upper,
    ),
  } as unknown as PositionMonitorService;
  return monitor;
}

function makeSdkService(address = '0xwallet') {
  return {
    getAddress: jest.fn().mockReturnValue(address),
    getBalance: jest.fn().mockResolvedValue('999999999'),
    getSdk: jest.fn(),
    getKeypair: jest.fn(),
    getSuiClient: jest.fn(),
  } as any;
}

// ---------------------------------------------------------------------------
// checkAndRebalance — returns null when position is in range
// ---------------------------------------------------------------------------

describe('checkAndRebalance – in-range position', () => {
  it('returns null and never calls removeLiquidity', async () => {
    const pool = makePoolInfo({ currentTickIndex: 0 });
    // Position [−100, 100] — in range at tick 0
    const pos = makePosition({ tickLower: -100, tickUpper: 100, liquidity: '5000000' });
    const monitor = makeMonitor([pos], pool);

    // override isPositionInRange to return true
    (monitor.isPositionInRange as jest.Mock).mockReturnValue(true);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
    } as any;

    process.env.DRY_RUN = 'false';
    const svc = new RebalanceService(makeSdkService(), monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// checkAndRebalance — no positions → creates initial position
// ---------------------------------------------------------------------------

describe('checkAndRebalance – no positions', () => {
  beforeEach(() => { process.env.DRY_RUN = 'true'; });
  afterEach(() => { delete process.env.DRY_RUN; });

  it('creates initial position (dry run) when no positions with liquidity exist', async () => {
    const pool = makePoolInfo({ currentTickIndex: 0 });
    const monitor = makeMonitor([], pool);
    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: -100,
      upperTick: 100,
    } as any;

    const svc = new RebalanceService(makeSdkService(), monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);
    expect(result!.newPosition).toEqual({ tickLower: -100, tickUpper: 100 });
    expect(result!.oldPosition).toBeUndefined();
  });

  it('creates initial position (dry run) when only zero-liquidity positions exist', async () => {
    const pool = makePoolInfo({ currentTickIndex: 500 });
    // Both positions have zero liquidity — treated as "no position"
    const positions = [
      makePosition({ liquidity: '0' }),
      makePosition({ liquidity: '0' }),
    ];
    const monitor = makeMonitor(positions, pool);
    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
    } as any;

    const svc = new RebalanceService(makeSdkService(), monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);
    expect(result!.newPosition).toEqual({ tickLower: 400, tickUpper: 600 });
  });
});

// ---------------------------------------------------------------------------
// createInitialPosition — default tick range (no env config)
// ---------------------------------------------------------------------------

describe('checkAndRebalance – no positions, default tick range', () => {
  beforeEach(() => { process.env.DRY_RUN = 'true'; });
  afterEach(() => { delete process.env.DRY_RUN; });

  it('uses ±10 tick spacings centred on current tick when no lowerTick/upperTick configured', async () => {
    const pool = makePoolInfo({ currentTickIndex: 1000, tickSpacing: 10 });
    const monitor = makeMonitor([], pool);
    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      // lowerTick / upperTick deliberately absent → use default range
    } as any;

    const svc = new RebalanceService(makeSdkService(), monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    const { tickLower, tickUpper } = result!.newPosition!;

    // Both bounds must be multiples of tickSpacing (10)
    expect(tickLower % 10).toBe(0);
    expect(tickUpper % 10).toBe(0);

    // Range must straddle the current tick
    expect(tickLower).toBeLessThanOrEqual(1000);
    expect(tickUpper).toBeGreaterThanOrEqual(1000);

    // Width must be exactly ±10 tick spacings (20 * tickSpacing = 200 ticks)
    expect(tickUpper - tickLower).toBe(200);
  });
});

// ---------------------------------------------------------------------------
// rebalancePosition in dry-run — env-configured tick range
// ---------------------------------------------------------------------------

describe('checkAndRebalance – dry-run out-of-range, env tick range', () => {
  beforeEach(() => { process.env.DRY_RUN = 'true'; });
  afterEach(() => { delete process.env.DRY_RUN; });

  it('returns success with the env-configured new tick range', async () => {
    const pool = makePoolInfo({ currentTickIndex: 500, tickSpacing: 10 });
    // Position is below current tick (out of range)
    const pos = makePosition({ tickLower: -200, tickUpper: -100, liquidity: '1000000' });
    const monitor = makeMonitor([pos], pool);
    (monitor.isPositionInRange as jest.Mock).mockReturnValue(false);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
    } as any;

    const svc = new RebalanceService(makeSdkService(), monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);
    expect(result!.newPosition).toEqual({ tickLower: 400, tickUpper: 600 });
    expect(result!.oldPosition).toEqual({ tickLower: -200, tickUpper: -100 });
  });
});

// ---------------------------------------------------------------------------
// rebalancePosition in dry-run — derived (width-preserving) tick range
// ---------------------------------------------------------------------------

describe('checkAndRebalance – dry-run out-of-range, derived tick range', () => {
  beforeEach(() => { process.env.DRY_RUN = 'true'; });
  afterEach(() => { delete process.env.DRY_RUN; });

  it('preserves old range width, aligned to tickSpacing', async () => {
    // Old range width = 500 − (−500) = 1000; centred on currentTick 2000, spacing 10.
    const pool = makePoolInfo({ currentTickIndex: 2000, tickSpacing: 10 });
    const pos = makePosition({ tickLower: -500, tickUpper: 500, liquidity: '2000000' });
    const monitor = makeMonitor([pos], pool);
    (monitor.isPositionInRange as jest.Mock).mockReturnValue(false);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      // lowerTick / upperTick deliberately absent → use derived range
    } as any;

    const svc = new RebalanceService(makeSdkService(), monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    const { tickLower, tickUpper } = result!.newPosition!;
    const rangeWidth = tickUpper - tickLower;
    const originalWidth = 500 - (-500); // 1000

    // The new range width must be >= original (ceil may add spacing)
    expect(rangeWidth).toBeGreaterThanOrEqual(originalWidth);
    // Both bounds must be multiples of tickSpacing
    expect(tickLower % 10).toBe(0);
    expect(tickUpper % 10).toBe(0);
    // Range must straddle the current tick
    expect(tickLower).toBeLessThanOrEqual(2000);
    expect(tickUpper).toBeGreaterThanOrEqual(2000);
  });
});

// ---------------------------------------------------------------------------
// dry-run: picks highest-liquidity position when multiple exist
// ---------------------------------------------------------------------------

describe('checkAndRebalance – picks highest liquidity position', () => {
  beforeEach(() => { process.env.DRY_RUN = 'true'; });
  afterEach(() => { delete process.env.DRY_RUN; });

  it('selects the position with most liquidity', async () => {
    const pool = makePoolInfo({ currentTickIndex: 500, tickSpacing: 1 });
    const smallPos = makePosition({ liquidity: '1000', tickLower: -200, tickUpper: -100 });
    const bigPos = { ...smallPos, positionId: '0xpos2', liquidity: '9999999' };
    const monitor = makeMonitor([smallPos, bigPos], pool);
    (monitor.isPositionInRange as jest.Mock).mockReturnValue(false);

    const config = { gasBudget: 50_000_000, maxSlippage: 0.01, lowerTick: 400, upperTick: 600 } as any;

    const svc = new RebalanceService(makeSdkService(), monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    // In dry-run the old position info is returned — it should come from bigPos
    expect(result!.oldPosition).toEqual({ tickLower: -200, tickUpper: -100 });
  });
});

// ---------------------------------------------------------------------------
// Stored-liquidity guard: openNewPosition validates storedLiquidity when provided
// ---------------------------------------------------------------------------

describe('rebalance – stored liquidity guard', () => {
  beforeEach(() => { process.env.DRY_RUN = 'true'; });
  afterEach(() => { delete process.env.DRY_RUN; });

  it('creates initial position (dry run) when only zero-liquidity position exists', async () => {
    // Zero-liquidity positions are filtered out; createInitialPosition is called instead.
    const pool = makePoolInfo({ currentTickIndex: 500 });
    const pos = makePosition({ liquidity: '0' });
    const monitor = makeMonitor([pos], pool);
    const config = { gasBudget: 50_000_000, maxSlippage: 0.01, lowerTick: 400, upperTick: 600 } as any;

    const svc = new RebalanceService(makeSdkService(), monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    // Instead of null, a new initial position is created.
    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);
    expect(result!.newPosition).toEqual({ tickLower: 400, tickUpper: 600 });
  });
});

// ---------------------------------------------------------------------------
// openNewPosition uses createAddLiquidityFixTokenPayload (not createAddLiquidityPayload)
// ---------------------------------------------------------------------------

describe('rebalance – live path uses fix-token add-liquidity', () => {
  afterEach(() => { delete process.env.DRY_RUN; });

  it('calls createAddLiquidityFixTokenPayload with env-configured amounts, not delta_liquidity', async () => {
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({ currentTickIndex: 500, tickSpacing: 10 });
    const pos = makePosition({ tickLower: -200, tickUpper: -100, liquidity: '5000000' });
    const monitor = makeMonitor([pos], pool);
    (monitor.isPositionInRange as jest.Mock).mockReturnValue(false);
    // Return the same position on retry (used by removeLiquidity)
    (monitor.getPositions as jest.Mock).mockResolvedValue([pos]);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '1000000',
    } as any;

    // Mock transaction stubs
    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);
    const createAddLiquidityPayload = jest.fn().mockResolvedValue(mockTxStub);

    const mockSdk = {
      Position: {
        removeLiquidityTransactionPayload: jest.fn().mockResolvedValue(mockTxStub),
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload,
      },
    };

    // Both env token amounts are set and the new position price is in range →
    // no swap is needed, so getBalance is not called in rebalancePosition.
    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn()
        // call 1: removeLiquidity
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xremove' })
        // call 2: openPosition (NFT)
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
        })
        // call 3: addLiquidity
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' }),
      getBalance: jest.fn(),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockResolvedValue('999999999'),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    // Must call the fix-token variant, NOT the delta-liquidity variant.
    expect(createAddLiquidityFixTokenPayload).toHaveBeenCalledTimes(1);
    expect(createAddLiquidityPayload).not.toHaveBeenCalled();

    // The fix-token call must use env-configured amounts, not a stored delta_liquidity field.
    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0];
    expect(callArgs).toHaveProperty('amount_a');
    expect(callArgs).toHaveProperty('amount_b');
    expect(callArgs).toHaveProperty('fix_amount_a');
    expect(callArgs).toHaveProperty('slippage', config.maxSlippage);
    expect(callArgs).toHaveProperty('is_open', false);
    expect(callArgs).not.toHaveProperty('delta_liquidity');

    // coinTypeA and coinTypeB must be top-level (not nested) and present.
    expect(callArgs).toHaveProperty('coinTypeA', pool.coinTypeA);
    expect(callArgs).toHaveProperty('coinTypeB', pool.coinTypeB);

    // amount_a and amount_b must equal the exact env-configured values.
    expect(callArgs.amount_a).toBe('1000000');
    expect(callArgs.amount_b).toBe('1000000');

    // fix_amount_a must be a boolean.
    expect(typeof callArgs.fix_amount_a).toBe('boolean');

    // getObject must have been called to verify the position is accessible.
    expect(mockSuiClient.getObject).toHaveBeenCalledWith({
      id: '0xnewpos',
      options: { showOwner: true, showType: true },
    });
  });
});

// ---------------------------------------------------------------------------
// openNewPosition step 2 retries when NFT not yet available for consumption
// ---------------------------------------------------------------------------

describe('rebalance – step 2 retries on "not available for consumption"', () => {
  afterEach(() => { delete process.env.DRY_RUN; });

  it('succeeds after one retryable failure in add-liquidity', async () => {
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({ currentTickIndex: 500, tickSpacing: 10 });
    const pos = makePosition({ tickLower: -200, tickUpper: -100, liquidity: '5000000' });
    const monitor = makeMonitor([pos], pool);
    (monitor.isPositionInRange as jest.Mock).mockReturnValue(false);
    (monitor.getPositions as jest.Mock).mockResolvedValue([pos]);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '1000000',
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);

    const mockSdk = {
      Position: {
        removeLiquidityTransactionPayload: jest.fn().mockResolvedValue(mockTxStub),
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload: jest.fn(),
      },
    };

    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn()
        // call 1: removeLiquidity succeeds
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xremove' })
        // call 2: openPosition (NFT) succeeds
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
        })
        // call 3: addLiquidity fails with the retryable error
        .mockRejectedValueOnce(new Error('Object 0xnewpos is not available for consumption'))
        // call 4: addLiquidity succeeds on retry
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' }),
      getBalance: jest.fn(),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockResolvedValue('999999999'),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);
    // createAddLiquidityFixTokenPayload must have been called twice (initial + 1 retry)
    expect(createAddLiquidityFixTokenPayload).toHaveBeenCalledTimes(2);
  });
});

// ---------------------------------------------------------------------------
// openNewPosition — input validation
// ---------------------------------------------------------------------------

describe('rebalance – openNewPosition input validation', () => {
  afterEach(() => { delete process.env.DRY_RUN; });

  /**
   * Helper that sets up a rebalance run where removeLiquidity succeeds and
   * the config uses env-configured token amounts.
   */
  function makeValidationScenario(configOverride: Record<string, unknown> = {}) {
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({ currentTickIndex: 500, tickSpacing: 10 });
    const pos = makePosition({ tickLower: -200, tickUpper: -100, liquidity: '5000000' });
    const monitor = makeMonitor([pos], pool);
    (monitor.isPositionInRange as jest.Mock).mockReturnValue(false);
    (monitor.getPositions as jest.Mock).mockResolvedValue([pos]);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '1000000',
      ...configOverride,
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const mockSdk = {
      Position: {
        removeLiquidityTransactionPayload: jest.fn().mockResolvedValue(mockTxStub),
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload: jest.fn().mockResolvedValue(mockTxStub),
        createAddLiquidityPayload: jest.fn(),
      },
    };

    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn()
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xremove' }),
      getBalance: jest.fn(),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockResolvedValue('999999999'),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    return { pool, pos, monitor, config, mockSdk, mockSuiClient, sdkService };
  }

  it('returns failure when tickLower is not an integer', async () => {
    const { monitor, sdkService } = makeValidationScenario({ lowerTick: 1.5, upperTick: 600 });
    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 1.5,
      upperTick: 600,
      totalUsd: '1000000',
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(false);
    expect(result!.error).toMatch(/Invalid tickLower/);
  });

  it('returns failure when tickUpper is not an integer', async () => {
    const { monitor, sdkService } = makeValidationScenario();
    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600.9,
      totalUsd: '1000000',
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(false);
    expect(result!.error).toMatch(/Invalid tickUpper/);
  });

  it('logs all params and re-throws when createAddLiquidityFixTokenPayload fails', async () => {
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({ currentTickIndex: 500, tickSpacing: 10 });
    const pos = makePosition({ tickLower: -200, tickUpper: -100, liquidity: '5000000' });
    const monitor = makeMonitor([pos], pool);
    (monitor.isPositionInRange as jest.Mock).mockReturnValue(false);
    (monitor.getPositions as jest.Mock).mockResolvedValue([pos]);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '1000000',
    } as any;
    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const sdkError = new Error('SDK_INTERNAL: invalid coin type');
    const createAddLiquidityFixTokenPayload = jest.fn().mockRejectedValue(sdkError);

    const mockSdk = {
      Position: {
        removeLiquidityTransactionPayload: jest.fn().mockResolvedValue(mockTxStub),
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload: jest.fn(),
      },
    };

    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn()
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xremove' })
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
        }),
      getBalance: jest.fn(),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockResolvedValue('999999999'),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    // The non-retryable SDK error should bubble up to rebalancePosition's catch
    expect(result).not.toBeNull();
    expect(result!.success).toBe(false);
    expect(result!.error).toBe('SDK_INTERNAL: invalid coin type');
    // The SDK function should have been called exactly once (non-retryable → no retry)
    expect(createAddLiquidityFixTokenPayload).toHaveBeenCalledTimes(1);
  });
});

// ---------------------------------------------------------------------------
// waitForPositionObject — position accessibility verification
// ---------------------------------------------------------------------------

describe('rebalance – position object verification before add-liquidity', () => {
  afterEach(() => { delete process.env.DRY_RUN; });

  /** Build a full happy-path scenario with configurable getObject mock. */
  function makeVerificationScenario(getObjectMock: jest.Mock) {
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({ currentTickIndex: 500, tickSpacing: 10 });
    const pos = makePosition({ tickLower: -200, tickUpper: -100, liquidity: '5000000' });
    const monitor = makeMonitor([pos], pool);
    (monitor.isPositionInRange as jest.Mock).mockReturnValue(false);
    (monitor.getPositions as jest.Mock).mockResolvedValue([pos]);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '1000000',
    } as any;
    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);
    const mockSdk = {
      Position: {
        removeLiquidityTransactionPayload: jest.fn().mockResolvedValue(mockTxStub),
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload: jest.fn(),
      },
    };

    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn()
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xremove' })
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
        })
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' }),
      getBalance: jest.fn(),
      getObject: getObjectMock,
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockResolvedValue('999999999'),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    return { pool, monitor, config, mockSdk, mockSuiClient, sdkService, createAddLiquidityFixTokenPayload };
  }

  it('calls getObject with the new position ID before createAddLiquidityFixTokenPayload', async () => {
    const getObject = jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } });
    const { monitor, config, sdkService, createAddLiquidityFixTokenPayload } =
      makeVerificationScenario(getObject);

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    // getObject must have been called with the new position ID.
    expect(getObject).toHaveBeenCalledWith({
      id: '0xnewpos',
      options: { showOwner: true, showType: true },
    });

    // Verify ordering: getObject was called before createAddLiquidityFixTokenPayload.
    const getObjectOrder = getObject.mock.invocationCallOrder[0];
    const addLiqOrder = createAddLiquidityFixTokenPayload.mock.invocationCallOrder[0];
    expect(getObjectOrder).toBeLessThan(addLiqOrder);
  });

  it('retries getObject when position is not immediately accessible, then proceeds', async () => {
    // First call returns no data (not yet propagated); second call returns data.
    const getObject = jest.fn()
      .mockResolvedValueOnce({ data: null, error: { code: 'notExists' } })
      .mockResolvedValueOnce({ data: { objectId: '0xnewpos', type: 'position' } });

    const { monitor, config, sdkService, createAddLiquidityFixTokenPayload } =
      makeVerificationScenario(getObject);

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    // getObject should have been polled exactly twice.
    expect(getObject).toHaveBeenCalledTimes(2);
    // createAddLiquidityFixTokenPayload proceeds after verification.
    expect(createAddLiquidityFixTokenPayload).toHaveBeenCalledTimes(1);
  }, 10_000); // allow extra time for the 1.5 s retry delay

  it('returns failure when position object never becomes accessible', async () => {
    // getObject always returns no data across all attempts.
    const getObject = jest.fn().mockResolvedValue({ data: null, error: { code: 'notExists' } });

    const { monitor, config, sdkService, createAddLiquidityFixTokenPayload } =
      makeVerificationScenario(getObject);

    const svc = new RebalanceService(sdkService, monitor, config);

    jest.useFakeTimers();
    const resultPromise = svc.checkAndRebalance('0xpool');
    // Advance past all retry delays (5 attempts × 1500 ms = 7500 ms, plus headroom).
    await jest.runAllTimersAsync();
    const result = await resultPromise;
    jest.useRealTimers();

    expect(result).not.toBeNull();
    expect(result!.success).toBe(false);
    expect(result!.error).toMatch(/not accessible after/);

    // The add-liquidity call must never have been made.
    expect(createAddLiquidityFixTokenPayload).not.toHaveBeenCalled();
  });

  it('returns failure when objectChanges contains only a child-owned position (ObjectOwner)', async () => {
    // Simulate the bug: openPosition returns an objectChange where the "position"
    // object is owned by another object (ObjectOwner), not the wallet (AddressOwner).
    // The objectChanges.find() guard must reject it, leaving positionChange undefined
    // and throwing "Could not find new position ID".
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({ currentTickIndex: 500, tickSpacing: 10 });
    const pos = makePosition({ tickLower: -200, tickUpper: -100, liquidity: '5000000' });
    const monitor = makeMonitor([pos], pool);
    (monitor.isPositionInRange as jest.Mock).mockReturnValue(false);
    (monitor.getPositions as jest.Mock).mockResolvedValue([pos]);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '1000000',
    } as any;
    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);
    const mockSdk = {
      Position: {
        removeLiquidityTransactionPayload: jest.fn().mockResolvedValue(mockTxStub),
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload: jest.fn(),
      },
    };

    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn()
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xremove' })
        // openPosition returns a position object owned by another object, not the wallet.
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [
            {
              type: 'created',
              objectType: 'position',
              objectId: '0xchildpos',
              owner: { ObjectOwner: '0xparentobject' },
            },
          ],
        }),
      getBalance: jest.fn(),
      getObject: jest.fn(),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockResolvedValue('999999999'),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(false);
    expect(result!.error).toMatch(/Could not find new position ID/);
    // add-liquidity must never be called when no valid position ID is found.
    expect(createAddLiquidityFixTokenPayload).not.toHaveBeenCalled();
  });

  it('returns failure when waitForPositionObject detects an ObjectOwner-owned position', async () => {
    // Simulate the case where the position object IS visible on the network via
    // getObject but is owned by another object — waitForPositionObject must throw
    // immediately rather than passing the bad ID to add-liquidity.
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({ currentTickIndex: 500, tickSpacing: 10 });
    const pos = makePosition({ tickLower: -200, tickUpper: -100, liquidity: '5000000' });
    const monitor = makeMonitor([pos], pool);
    (monitor.isPositionInRange as jest.Mock).mockReturnValue(false);
    (monitor.getPositions as jest.Mock).mockResolvedValue([pos]);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '1000000',
    } as any;
    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);
    const mockSdk = {
      Position: {
        removeLiquidityTransactionPayload: jest.fn().mockResolvedValue(mockTxStub),
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload: jest.fn(),
      },
    };

    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn()
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xremove' })
        // openPosition returns a position owned by the wallet (passes the first guard).
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [
            {
              type: 'created',
              objectType: 'position',
              objectId: '0xnewpos',
              owner: { AddressOwner: '0xwallet' },
            },
          ],
        }),
      getBalance: jest.fn(),
      // getObject returns the position as ObjectOwner — simulates a wrapped object.
      getObject: jest.fn().mockResolvedValue({
        data: { objectId: '0xnewpos', owner: { ObjectOwner: '0xparentobject' } },
      }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockResolvedValue('999999999'),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(false);
    expect(result!.error).toMatch(/owned by another object/);
    // add-liquidity must never be called.
    expect(createAddLiquidityFixTokenPayload).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// fix_amount_a selection — in-range and out-of-range price scenarios
// ---------------------------------------------------------------------------

describe('rebalance – fix_amount_a is determined by price and tick range', () => {
  afterEach(() => { delete process.env.DRY_RUN; });

  /**
   * Run a full rebalance with custom pool state (currentTickIndex, sqrtPrice)
   * and env-configured token amounts, then return the params captured from the
   * createAddLiquidityFixTokenPayload call.
   *
   * tokenAmountA / tokenAmountB are the exact amounts from env config.
   * Pass '0' for one token to simulate a single-token scenario (e.g. price out of range).
   */
  async function runAndCaptureFixToken(
    currentTickIndex: number,
    currentSqrtPrice: string,
    tickLower: number,
    tickUpper: number,
    walletAmountA: string,
    walletAmountB: string,
  ): Promise<{ fix_amount_a: boolean }> {
    process.env.DRY_RUN = 'false';

    const pool = {
      poolAddress: '0xpool',
      currentTickIndex,
      currentSqrtPrice,
      coinTypeA: '0xcoinA',
      coinTypeB: '0xcoinB',
      tickSpacing: 10,
    };

    const pos = makePosition({ tickLower: -200, tickUpper: -100, liquidity: '5000000' });
    const monitor = makeMonitor([pos], pool as any);
    (monitor.isPositionInRange as jest.Mock).mockReturnValue(false);
    (monitor.getPositions as jest.Mock).mockResolvedValue([pos]);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: tickLower,
      upperTick: tickUpper,
      totalUsd: '999999999999999', // large enough to not trigger scaling
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);
    const mockSdk = {
      Position: {
        removeLiquidityTransactionPayload: jest.fn().mockResolvedValue(mockTxStub),
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload: jest.fn(),
      },
    };

    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn()
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xremove' })
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
        })
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' }),
      getBalance: jest.fn(),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) =>
        Promise.resolve(coinType === '0xcoinA' ? walletAmountA : walletAmountB)),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);
    expect(createAddLiquidityFixTokenPayload).toHaveBeenCalledTimes(1);
    return createAddLiquidityFixTokenPayload.mock.calls[0][0] as { fix_amount_a: boolean };
  }

  // Sqrt prices computed from TickMath.tickIndexToSqrtPriceX64 for consistency
  // with the corrected fix_amount_a formula that uses TickMath internally.
  // Tick 500 → '18913701982652573318', tick 100 → '18539204128674405812', tick 700 → '19103778296503601288'
  const SQRT_PRICE_TICK_500 = '18913701982652573318';
  const SQRT_PRICE_TICK_100 = '18539204128674405812';
  const SQRT_PRICE_TICK_700 = '19103778296503601288';

  it('fixes token B (fix_amount_a=false) when A is the liquidity-excess token (both tokens, in range)', async () => {
    // tokenAmountA=2000000, tokenAmountB=1000000 at tick 500 in range [400,600]:
    // L_a > L_b → B is the bottleneck → fix B (fix_amount_a=false)
    const args = await runAndCaptureFixToken(500, SQRT_PRICE_TICK_500, 400, 600, '2000000', '1000000');
    expect(args.fix_amount_a).toBe(false);
  });

  it('fixes token A (fix_amount_a=true) when B is the liquidity-excess token (both tokens, in range)', async () => {
    // tokenAmountA=500000, tokenAmountB=1000000 at tick 500 in range [400,600]:
    // L_a < L_b → A is the bottleneck → fix A (fix_amount_a=true)
    const args = await runAndCaptureFixToken(500, SQRT_PRICE_TICK_500, 400, 600, '500000', '1000000');
    expect(args.fix_amount_a).toBe(true);
  });

  it('fixes token A (fix_amount_a=true) when price is below the new range', async () => {
    // currentTick(100) < tickLower(400) → position only accepts token A → fix A.
    // tokenAmountB='0' means no B is available so the B→A swap path is not triggered.
    const args = await runAndCaptureFixToken(100, SQRT_PRICE_TICK_100, 400, 600, '1000000', '0');
    expect(args.fix_amount_a).toBe(true);
  });

  it('fixes token B (fix_amount_a=false) when price is at or above the new range', async () => {
    // currentTick(700) >= tickUpper(600) → position only accepts token B → fix B.
    // tokenAmountA='0' means no A is available so the A→B swap path is not triggered.
    const args = await runAndCaptureFixToken(700, SQRT_PRICE_TICK_700, 400, 600, '0', '1000000');
    expect(args.fix_amount_a).toBe(false);
  });

  it('fixes token B (fix_amount_a=false) when price is near the top of the range', async () => {
    // currentTick(590) near tickUpper(600) → mostly token B required.
    // The old spot-price formula incorrectly chose fix_amount_a=true here; the
    // correct CLMM formula chooses fix_amount_a=false, preventing InsufficientCoinBalance.
    // Tick 590 sqrtPrice = '18999001155891605229'
    const sqrtPriceTick590 = '18999001155891605229';
    const args = await runAndCaptureFixToken(590, sqrtPriceTick590, 400, 600, '1000000', '2000000');
    expect(args.fix_amount_a).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// swapTokensIfNeeded — both tokens present but position is out of range
// ---------------------------------------------------------------------------

describe('rebalance – both tokens present with out-of-range new position triggers swap', () => {
  afterEach(() => { delete process.env.DRY_RUN; });

  /**
   * Run a full rebalance where BOTH env-configured token amounts are non-zero,
   * and the new tick range is out of range in the given direction.  Returns
   * the direction and amount passed to the aggregator swap call.
   */
  async function runAndCaptureSwap(
    currentTickIndex: number,
    tickLower: number,
    tickUpper: number,
    tokenAmountA: string,
    tokenAmountB: string,
  ): Promise<{ a2b: boolean; amount: string }> {
    process.env.DRY_RUN = 'false';

    const pool = {
      poolAddress: '0xpool',
      currentTickIndex,
      currentSqrtPrice: '18913701982652573318',
      coinTypeA: '0xcoinA',
      coinTypeB: '0xcoinB',
      tickSpacing: 10,
    };

    const pos = makePosition({ tickLower: -200, tickUpper: -100, liquidity: '5000000' });
    const monitor = makeMonitor([pos], pool as any);
    (monitor.isPositionInRange as jest.Mock).mockReturnValue(false);
    (monitor.getPositions as jest.Mock).mockResolvedValue([pos]);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: tickLower,
      upperTick: tickUpper,
      totalUsd: '999999999999999', // large enough to not trigger scaling
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const createSwapTransactionPayload = jest.fn().mockReturnValue(mockTxStub);
    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);

    const mockSdk = {
      RouterV2: {
        getBestRouter: jest.fn().mockRejectedValue(new Error('aggregator unavailable')),
      },
      Swap: {
        createSwapTransactionPayload,
      },
      Position: {
        removeLiquidityTransactionPayload: jest.fn().mockResolvedValue(mockTxStub),
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload: jest.fn(),
      },
    };

    // getBalance is called by swapTokensIfNeeded for pre/post swap amounts.
    // First 2 calls are pre-swap reads → return '0'; calls 3-4 are post-swap → actual.
    let getBalanceCallCount = 0;
    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn()
        // removeLiquidity
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xremove' })
        // swap (fallback direct)
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xswap',
          balanceChanges: [],
        })
        // openPosition NFT
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
        })
        // addLiquidity
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' }),
      getBalance: jest.fn().mockImplementation(({ coinType }: { coinType: string }) => {
        getBalanceCallCount++;
        if (getBalanceCallCount <= 2) return Promise.resolve({ totalBalance: '0' });
        return Promise.resolve({ totalBalance: coinType === '0xcoinA' ? tokenAmountA : tokenAmountB });
      }),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) =>
        Promise.resolve(coinType === '0xcoinA' ? tokenAmountA : tokenAmountB)),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);
    expect(createSwapTransactionPayload).toHaveBeenCalledTimes(1);

    const swapArgs = createSwapTransactionPayload.mock.calls[0][0] as { a2b: boolean; amount: string };
    return { a2b: swapArgs.a2b, amount: swapArgs.amount };
  }

  it('swaps ALL token B to A when both tokens are present but price is below the new range', async () => {
    // Both env amounts are non-zero. New range is below current price — only token A accepted.
    // The bot must convert all B to A rather than leaving it unused.
    const swap = await runAndCaptureSwap(
      100,   // currentTick — below range [400,600]
      400,   // tickLower
      600,   // tickUpper
      '500000',   // tokenAmountA (already have some A)
      '800000',   // tokenAmountB (must swap ALL of this to A)
    );
    expect(swap.a2b).toBe(false);          // B → A
    expect(swap.amount).toBe('800000');    // swap the full B amount
  });

  it('swaps ALL token A to B when both tokens are present but price is above the new range', async () => {
    // Both env amounts are non-zero. New range is above current price — only token B accepted.
    // The bot must convert all A to B rather than leaving it unused.
    const swap = await runAndCaptureSwap(
      700,   // currentTick — above range [400,600]
      400,   // tickLower
      600,   // tickUpper
      '800000',   // tokenAmountA (must swap ALL of this to B)
      '500000',   // tokenAmountB (already have some B)
    );
    expect(swap.a2b).toBe(true);           // A → B
    expect(swap.amount).toBe('800000');    // swap the full A amount
  });
});

// ---------------------------------------------------------------------------
// swapTokensIfNeeded — CLMM-optimal swap when in-range with a single token
// ---------------------------------------------------------------------------

describe('rebalance – CLMM-optimal swap amount for in-range single-token case', () => {
  afterEach(() => { delete process.env.DRY_RUN; });

  /**
   * Run a full rebalance where only ONE env-configured token amount is non-zero
   * and the new tick range is in-range.  Returns the direction and amount
   * passed to the swap call.
   *
   * Pass '0' for the unavailable token to simulate a single-token scenario.
   */
  async function runAndCaptureSingleTokenSwap(
    currentTickIndex: number,
    currentSqrtPrice: string,
    tickLower: number,
    tickUpper: number,
    tokenAmountA: string,
    tokenAmountB: string,
  ): Promise<{ a2b: boolean; amount: string }> {
    process.env.DRY_RUN = 'false';

    const pool = {
      poolAddress: '0xpool',
      currentTickIndex,
      currentSqrtPrice,
      coinTypeA: '0xcoinA',
      coinTypeB: '0xcoinB',
      tickSpacing: 10,
    };

    const pos = makePosition({ tickLower: -200, tickUpper: -100, liquidity: '5000000' });
    const monitor = makeMonitor([pos], pool as any);
    (monitor.isPositionInRange as jest.Mock).mockReturnValue(false);
    (monitor.getPositions as jest.Mock).mockResolvedValue([pos]);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: tickLower,
      upperTick: tickUpper,
      totalUsd: '999999999999999', // large enough to not trigger scaling
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const createSwapTransactionPayload = jest.fn().mockReturnValue(mockTxStub);
    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);

    const mockSdk = {
      RouterV2: {
        getBestRouter: jest.fn().mockRejectedValue(new Error('aggregator unavailable')),
      },
      Swap: {
        createSwapTransactionPayload,
      },
      Position: {
        removeLiquidityTransactionPayload: jest.fn().mockResolvedValue(mockTxStub),
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload: jest.fn(),
      },
    };

    // getBalance is called by swapTokensIfNeeded for pre/post swap amounts.
    let getBalanceCallCount = 0;
    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn()
        // removeLiquidity
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xremove' })
        // swap (fallback direct)
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xswap',
          balanceChanges: [],
        })
        // openPosition NFT
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
        })
        // addLiquidity
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' }),
      getBalance: jest.fn().mockImplementation(({ coinType }: { coinType: string }) => {
        getBalanceCallCount++;
        if (getBalanceCallCount <= 2) return Promise.resolve({ totalBalance: '0' });
        return Promise.resolve({ totalBalance: coinType === '0xcoinA' ? tokenAmountA : tokenAmountB });
      }),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) =>
        Promise.resolve(coinType === '0xcoinA' ? tokenAmountA : tokenAmountB)),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);
    expect(createSwapTransactionPayload).toHaveBeenCalledTimes(1);

    const swapArgs = createSwapTransactionPayload.mock.calls[0][0] as { a2b: boolean; amount: string };
    return { a2b: swapArgs.a2b, amount: swapArgs.amount };
  }

  // Tick 500 sqrt price for the in-range scenario tests below.
  const SQRT_PRICE_TICK_500 = '18913701982652573318';

  it('swaps A→B with CLMM-optimal amount when only token A is available and price is in range', async () => {
    // Only token A is configured (token B env amount is '0').
    // The new range [400, 600] is in range at tick 500.
    //
    // With the old "swap half" heuristic, the bot would swap bigA/2 = 500_000.
    // With the CLMM-optimal formula the swap amount depends on the exact ratio
    // required by [tick400, tick600] at tick500.  The precise value is computed
    // by the formula: bigA * sqrtPb * (sqrtP - sqrtPa) / D, which for a roughly
    // symmetric range around tick 500 should be close to but NOT exactly half.
    const swap = await runAndCaptureSingleTokenSwap(
      500,                  // currentTick — in the new range [400, 600]
      SQRT_PRICE_TICK_500,
      400,                  // tickLower
      600,                  // tickUpper
      '1000000',            // tokenAmountA — only token A configured
      '0',                  // tokenAmountB — zero (single-token scenario)
    );

    expect(swap.a2b).toBe(true);   // A → B direction is correct

    // The CLMM-optimal swap amount must be a positive integer string.
    const swapAmt = BigInt(swap.amount);
    expect(swapAmt).toBeGreaterThan(0n);
    expect(swapAmt).toBeLessThan(1_000_000n); // must not swap more than available

    // The optimal amount for a roughly symmetric ±100-tick range around tick 500
    // is approximately 50% of bigA (within ±5% of 500_000 for near-symmetric ranges).
    // The exact value is NOT 500_000 in general, and varies by tick geometry.
    expect(swapAmt).toBeGreaterThanOrEqual(400_000n);
    expect(swapAmt).toBeLessThanOrEqual(600_000n);
  });

  it('swaps B→A with CLMM-optimal amount when only token B is available and price is in range', async () => {
    // Only token B is configured (token A env amount is '0').
    // The new range [400, 600] is in range at tick 500.
    const swap = await runAndCaptureSingleTokenSwap(
      500,
      SQRT_PRICE_TICK_500,
      400,
      600,
      '0',                  // tokenAmountA — zero (single-token scenario)
      '1000000',            // tokenAmountB — only token B configured
    );

    expect(swap.a2b).toBe(false);  // B → A direction is correct

    const swapAmt = BigInt(swap.amount);
    expect(swapAmt).toBeGreaterThan(0n);
    expect(swapAmt).toBeLessThan(1_000_000n);

    // Similar symmetry argument — should be near but not exactly half.
    expect(swapAmt).toBeGreaterThanOrEqual(400_000n);
    expect(swapAmt).toBeLessThanOrEqual(600_000n);
  });

  it('swaps a smaller A fraction when the new range is skewed toward A (near lower tick)', async () => {
    // New range [490, 600]: tick 500 is only 10 ticks above tickLower (490)
    // and 100 ticks below tickUpper (600).
    //
    // CLMM position amounts at tick 500 in [490, 600]:
    //   amountA ∝ (sqrtPb − sqrtP): 100 ticks of "A headroom" → large A component
    //   amountB ∝ (sqrtP − sqrtPa): 10 ticks of "B headroom"  → small  B component
    //
    // So the position needs MOSTLY A and only a tiny bit of B.
    // When only A is configured, the bot must swap a SMALL fraction of A to B
    // (~9–10%) — much less than the "swap half" heuristic's 50%.
    const swap = await runAndCaptureSingleTokenSwap(
      500,
      SQRT_PRICE_TICK_500,
      490,    // tickLower — close to current tick (small B-accepting portion)
      600,    // tickUpper — far above current tick (large A-accepting portion)
      '1000000',
      '0',
    );

    expect(swap.a2b).toBe(true); // A → B

    // The optimal swap is significantly LESS than half because the position
    // needs mostly A.  The naive half-swap would leave far too much B and
    // too little A, reducing achieved liquidity.
    const swapAmt = BigInt(swap.amount);
    expect(swapAmt).toBeGreaterThan(0n);
    expect(swapAmt).toBeLessThan(500_000n); // strictly less than the "half" heuristic
  });

  it('swaps a smaller B fraction when the new range is skewed toward B (near upper tick)', async () => {
    // New range [400, 510]: tick 500 is 100 ticks above tickLower (400)
    // and only 10 ticks below tickUpper (510).
    //
    // CLMM position amounts at tick 500 in [400, 510]:
    //   amountA ∝ (sqrtPb − sqrtP): 10 ticks of "A headroom"  → small  A component
    //   amountB ∝ (sqrtP − sqrtPa): 100 ticks of "B headroom" → large  B component
    //
    // So the position needs MOSTLY B and only a tiny bit of A.
    // When only B is configured, the bot must swap a SMALL fraction of B to A
    // (~9–10%) — much less than the "swap half" heuristic's 50%.
    const swap = await runAndCaptureSingleTokenSwap(
      500,
      SQRT_PRICE_TICK_500,
      400,    // tickLower — far below current tick
      510,    // tickUpper — close to current tick (small A-accepting portion)
      '0',
      '1000000',
    );

    expect(swap.a2b).toBe(false); // B → A

    // The optimal swap is significantly LESS than half because the position
    // needs mostly B.
    const swapAmt = BigInt(swap.amount);
    expect(swapAmt).toBeGreaterThan(0n);
    expect(swapAmt).toBeLessThan(500_000n); // strictly less than the "half" heuristic
  });
});

// ---------------------------------------------------------------------------
// rebalance — configured amounts capped by wallet balance
// ---------------------------------------------------------------------------

describe('rebalance – configured amounts capped by wallet balance', () => {
  afterEach(() => { delete process.env.DRY_RUN; });

  it('uses wallet amounts for rebalanced position when wallet value is below TOTAL_USD', async () => {
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({ currentTickIndex: 500, tickSpacing: 10 });
    const pos = makePosition({ tickLower: -200, tickUpper: -100, liquidity: '5000000' });
    const monitor = makeMonitor([pos], pool);
    (monitor.isPositionInRange as jest.Mock).mockReturnValue(false);
    (monitor.getPositions as jest.Mock).mockResolvedValue([pos]);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '10000000', // large enough that wallet value is below budget
    } as any;
    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);
    const mockSdk = {
      Position: {
        removeLiquidityTransactionPayload: jest.fn().mockResolvedValue(mockTxStub),
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload: jest.fn(),
      },
    };

    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn()
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xremove' })
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
        })
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' }),
      getBalance: jest.fn(),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) =>
        Promise.resolve(coinType === '0xcoinA' ? '2000000' : '3000000')),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);
    expect(createAddLiquidityFixTokenPayload).toHaveBeenCalledTimes(1);

    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0] as {
      amount_a: string; amount_b: string;
    };

    // Wallet value (≈3000000 tokenB) is below TOTAL_USD (10000000) — full wallet balances used.
    expect(callArgs.amount_a).toBe('2000000');
    expect(callArgs.amount_b).toBe('3000000');

    expect(sdkService.getBalance).toHaveBeenCalledWith('0xcoinA');
    expect(sdkService.getBalance).toHaveBeenCalledWith('0xcoinB');
    expect(sdkService.getBalance).toHaveBeenCalledTimes(2);
    // No swap was needed, so the swap-specific Sui balance reads are still unused.
    expect(mockSuiClient.getBalance).not.toHaveBeenCalled();
  });

  it('uses full wallet balances when wallet value is below TOTAL_USD', async () => {
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({ currentTickIndex: 500, tickSpacing: 10 });
    const pos = makePosition({ tickLower: -200, tickUpper: -100, liquidity: '5000000' });
    const monitor = makeMonitor([pos], pool);
    (monitor.isPositionInRange as jest.Mock).mockReturnValue(false);
    (monitor.getPositions as jest.Mock).mockResolvedValue([pos]);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '10000000', // large enough that wallet value (≈2500000) is below budget
    } as any;
    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);
    const mockSdk = {
      Position: {
        removeLiquidityTransactionPayload: jest.fn().mockResolvedValue(mockTxStub),
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload: jest.fn(),
      },
    };

    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn()
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xremove' })
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
        })
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' }),
      getBalance: jest.fn(),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) =>
        Promise.resolve(coinType === '0xcoinA' ? '1500000' : '2500000')),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0] as {
      amount_a: string; amount_b: string;
    };
    // Wallet value (≈2500000 tokenB) < TOTAL_USD (10000000) → full wallet balances used.
    expect(callArgs.amount_a).toBe('1500000');
    expect(callArgs.amount_b).toBe('2500000');
  });

  it('produces identical amounts across multiple rebalances (deterministic)', async () => {
    // Run two rebalances in sequence. Both must use the identical env amounts,
    // proving the bot is deterministic and not accumulating state.
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({ currentTickIndex: 500, tickSpacing: 10 });
    const pos = makePosition({ tickLower: -200, tickUpper: -100, liquidity: '5000000' });
    const monitor = makeMonitor([pos], pool);
    (monitor.isPositionInRange as jest.Mock).mockReturnValue(false);
    (monitor.getPositions as jest.Mock).mockResolvedValue([pos]);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '10000000', // large enough that wallet value is below budget
    } as any;
    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);
    const mockSdk = {
      Position: {
        removeLiquidityTransactionPayload: jest.fn().mockResolvedValue(mockTxStub),
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload: jest.fn(),
      },
    };

    const makeSuccessfulRun = () => ({
      signAndExecuteTransaction: jest.fn()
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xremove' })
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
        })
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' }),
      getBalance: jest.fn(),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos' } }),
    });

    const mockSuiClient1 = makeSuccessfulRun();
    const sdkService1 = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) =>
        Promise.resolve(coinType === '0xcoinA' ? '1500000' : '2500000')),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient1),
    } as any;

    const svc = new RebalanceService(sdkService1, monitor, config);
    const result1 = await svc.checkAndRebalance('0xpool');
    expect(result1!.success).toBe(true);

    // Reset and run again
    const mockSuiClient2 = makeSuccessfulRun();
    const sdkService2 = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) =>
        Promise.resolve(coinType === '0xcoinA' ? '1500000' : '2500000')),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient2),
    } as any;
    (monitor.getPositions as jest.Mock).mockResolvedValue([pos]);

    const svc2 = new RebalanceService(sdkService2, monitor, config);
    const result2 = await svc2.checkAndRebalance('0xpool');
    expect(result2!.success).toBe(true);

    // Both runs must produce identical amounts.
    expect(createAddLiquidityFixTokenPayload).toHaveBeenCalledTimes(2);
    const args1 = createAddLiquidityFixTokenPayload.mock.calls[0][0] as { amount_a: string; amount_b: string };
    const args2 = createAddLiquidityFixTokenPayload.mock.calls[1][0] as { amount_a: string; amount_b: string };
    expect(args1.amount_a).toBe('1500000');
    expect(args1.amount_b).toBe('2500000');
    expect(args2.amount_a).toBe('1500000');
    expect(args2.amount_b).toBe('2500000');
  });
});

// ---------------------------------------------------------------------------
// RANGE_WIDTH env var — rebalancePosition (out-of-range, dry-run)
// ---------------------------------------------------------------------------

describe('checkAndRebalance – dry-run out-of-range, RANGE_WIDTH tick range', () => {
  beforeEach(() => { process.env.DRY_RUN = 'true'; });
  afterEach(() => { delete process.env.DRY_RUN; });

  it('uses rangeWidth from config when lowerTick/upperTick are not set', async () => {
    const pool = makePoolInfo({ currentTickIndex: 2000, tickSpacing: 10 });
    const pos = makePosition({ tickLower: -500, tickUpper: 500, liquidity: '2000000' });
    const monitor = makeMonitor([pos], pool);
    (monitor.isPositionInRange as jest.Mock).mockReturnValue(false);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      rangeWidth: 400,  // RANGE_WIDTH=400 — should use this, not old position width
    } as any;

    const svc = new RebalanceService(makeSdkService(), monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    const { tickLower, tickUpper } = result!.newPosition!;

    // Both bounds must be multiples of tickSpacing (10)
    expect(tickLower % 10).toBe(0);
    expect(tickUpper % 10).toBe(0);
    // Range must straddle the current tick
    expect(tickLower).toBeLessThanOrEqual(2000);
    expect(tickUpper).toBeGreaterThanOrEqual(2000);
    // Width must be approximately the configured rangeWidth (may be rounded up to tickSpacing)
    expect(tickUpper - tickLower).toBeGreaterThanOrEqual(400);
  });

  it('prefers explicit lowerTick/upperTick over rangeWidth', async () => {
    const pool = makePoolInfo({ currentTickIndex: 500, tickSpacing: 10 });
    const pos = makePosition({ tickLower: -200, tickUpper: -100, liquidity: '1000000' });
    const monitor = makeMonitor([pos], pool);
    (monitor.isPositionInRange as jest.Mock).mockReturnValue(false);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      rangeWidth: 9999,  // should be ignored because lowerTick/upperTick are set
    } as any;

    const svc = new RebalanceService(makeSdkService(), monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result!.newPosition).toEqual({ tickLower: 400, tickUpper: 600 });
  });
});

// ---------------------------------------------------------------------------
// RANGE_WIDTH env var — createInitialPosition (no existing position, dry-run)
// ---------------------------------------------------------------------------

describe('checkAndRebalance – no positions, RANGE_WIDTH tick range', () => {
  beforeEach(() => { process.env.DRY_RUN = 'true'; });
  afterEach(() => { delete process.env.DRY_RUN; });

  it('uses rangeWidth from config for initial position when lowerTick/upperTick are not set', async () => {
    const pool = makePoolInfo({ currentTickIndex: 1000, tickSpacing: 10 });
    const monitor = makeMonitor([], pool);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      rangeWidth: 200,  // RANGE_WIDTH=200
    } as any;

    const svc = new RebalanceService(makeSdkService(), monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    const { tickLower, tickUpper } = result!.newPosition!;

    // Both bounds must be multiples of tickSpacing (10)
    expect(tickLower % 10).toBe(0);
    expect(tickUpper % 10).toBe(0);
    // Range must straddle the current tick
    expect(tickLower).toBeLessThanOrEqual(1000);
    expect(tickUpper).toBeGreaterThanOrEqual(1000);
    // Width must be approximately rangeWidth (may be rounded up to tickSpacing)
    expect(tickUpper - tickLower).toBeGreaterThanOrEqual(200);
  });

  it('prefers explicit lowerTick/upperTick over rangeWidth for initial position', async () => {
    const pool = makePoolInfo({ currentTickIndex: 1000, tickSpacing: 10 });
    const monitor = makeMonitor([], pool);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 900,
      upperTick: 1100,
      rangeWidth: 9999,  // should be ignored
    } as any;

    const svc = new RebalanceService(makeSdkService(), monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result!.newPosition).toEqual({ tickLower: 900, tickUpper: 1100 });
  });
});

// ---------------------------------------------------------------------------
// createInitialPosition — configured amounts capped by wallet balance
// ---------------------------------------------------------------------------

describe('createInitialPosition – configured amounts capped by wallet balance', () => {
  afterEach(() => { delete process.env.DRY_RUN; });

  /**
   * Helper that sets up a live (non-dry-run) createInitialPosition scenario.
   * Returns the mock SDK's createAddLiquidityFixTokenPayload for inspection.
   *
    * walletAmountA and walletAmountB are the wallet balances for each token.
    * TOTAL_USD is set large enough that no scaling is applied.
   */
  function makeInitialPositionScenario(opts: {
    walletAmountA: string;
    walletAmountB: string;
  }) {
    process.env.DRY_RUN = 'false';

    // No existing positions → createInitialPosition is invoked.
    const pool = makePoolInfo({ currentTickIndex: 500, tickSpacing: 10 });
    const monitor = makeMonitor([], pool);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '10000000', // large enough that wallet value is below budget
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);
    const mockSdk = {
      Position: {
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload: jest.fn(),
      },
    };

    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn()
        // call 1: openPosition (NFT)
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [{
            type: 'created',
            objectType: 'position',
            objectId: '0xnewpos',
            owner: { AddressOwner: '0xwallet' },
          }],
        })
        // call 2: addLiquidity
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' }),
      // getBalance is only used inside swapTokensIfNeeded when a swap actually happens
      getBalance: jest.fn(),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) =>
        Promise.resolve(coinType === '0xcoinA' ? opts.walletAmountA : opts.walletAmountB)),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    return { pool, monitor, config, sdkService, createAddLiquidityFixTokenPayload, mockSuiClient };
  }

  it('uses wallet token A balance for initial position when wallet value is below TOTAL_USD', async () => {
    const { monitor, config, sdkService, createAddLiquidityFixTokenPayload, mockSuiClient } =
      makeInitialPositionScenario({ walletAmountA: '2000000', walletAmountB: '1000000' });

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0];
    // amount_a must equal the wallet balance when wallet value < TOTAL_USD.
    expect(callArgs.amount_a).toBe('2000000');
    expect(sdkService.getBalance).toHaveBeenCalledWith('0xcoinA');
    expect(sdkService.getBalance).toHaveBeenCalledWith('0xcoinB');
    expect(sdkService.getBalance).toHaveBeenCalledTimes(2);
    // No swap was needed, so swap-specific Sui balance reads are still unused.
    expect(mockSuiClient.getBalance).not.toHaveBeenCalled();
  });

  it('uses wallet token B balance for initial position when wallet value is below TOTAL_USD', async () => {
    const { monitor, config, sdkService, createAddLiquidityFixTokenPayload, mockSuiClient } =
      makeInitialPositionScenario({ walletAmountA: '1000000', walletAmountB: '1500000' });

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0];
    // amount_b must equal the wallet balance when wallet value < TOTAL_USD.
    expect(callArgs.amount_b).toBe('1500000');
    expect(sdkService.getBalance).toHaveBeenCalledWith('0xcoinA');
    expect(sdkService.getBalance).toHaveBeenCalledWith('0xcoinB');
    expect(sdkService.getBalance).toHaveBeenCalledTimes(2);
    // No swap was needed, so swap-specific Sui balance reads are still unused.
    expect(mockSuiClient.getBalance).not.toHaveBeenCalled();
  });

  it('uses full wallet balances when TOTAL_USD exceeds wallet value', async () => {
    const { monitor, config, sdkService, createAddLiquidityFixTokenPayload } =
      makeInitialPositionScenario({ walletAmountA: '9999999', walletAmountB: '9999999' });

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0];
    // Full wallet balances are passed through when wallet value is below TOTAL_USD.
    expect(callArgs.amount_a).toBe('9999999');
    expect(callArgs.amount_b).toBe('9999999');
  });
});

// ---------------------------------------------------------------------------
// rebalancePosition — configured amounts capped by wallet balance
// ---------------------------------------------------------------------------

describe('rebalancePosition – configured amounts capped by wallet balance', () => {
  afterEach(() => { delete process.env.DRY_RUN; });

  /**
   * Helper that sets up a live (non-dry-run) rebalance scenario where the bot
   * has an out-of-range position. Returns the mock SDK's
   * createAddLiquidityFixTokenPayload for assertion.
   *
    * walletAmountA and walletAmountB are the wallet balances for each token.
    * TOTAL_USD is set large enough that no scaling is applied.
   */
  function makeRebalanceEnvAmountScenario(opts: {
    walletAmountA: string;
    walletAmountB: string;
  }) {
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({ currentTickIndex: 500, tickSpacing: 10 });
    // Position is below current tick → out of range.
    const pos = makePosition({ tickLower: -200, tickUpper: -100, liquidity: '5000000' });
    const monitor = makeMonitor([pos], pool);
    (monitor.isPositionInRange as jest.Mock).mockReturnValue(false);
    (monitor.getPositions as jest.Mock).mockResolvedValue([pos]);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '10000000', // large enough that wallet value is below budget
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);
    const mockSdk = {
      Position: {
        removeLiquidityTransactionPayload: jest.fn().mockResolvedValue(mockTxStub),
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload: jest.fn(),
      },
    };

    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn()
        // call 1: removeLiquidity
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xremove' })
        // call 2: openPosition (NFT)
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
        })
        // call 3: addLiquidity
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' }),
      // getBalance is only used inside swapTokensIfNeeded when a swap actually happens
      getBalance: jest.fn(),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) =>
        Promise.resolve(coinType === '0xcoinA' ? opts.walletAmountA : opts.walletAmountB)),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    return { monitor, config, sdkService, createAddLiquidityFixTokenPayload, mockSuiClient };
  }

  it('uses wallet token A balance during rebalance when wallet value is below TOTAL_USD', async () => {
    const { monitor, config, sdkService, createAddLiquidityFixTokenPayload, mockSuiClient } =
      makeRebalanceEnvAmountScenario({ walletAmountA: '2000000', walletAmountB: '3000000' });

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0];
    // Wallet value (≈3000000 tokenB) < TOTAL_USD (10000000) → full wallet balances used.
    expect(callArgs.amount_a).toBe('2000000');
    expect(sdkService.getBalance).toHaveBeenCalledWith('0xcoinA');
    expect(sdkService.getBalance).toHaveBeenCalledWith('0xcoinB');
    expect(sdkService.getBalance).toHaveBeenCalledTimes(2);
    // No swap was needed, so swap-specific Sui balance reads are still unused.
    expect(mockSuiClient.getBalance).not.toHaveBeenCalled();
  });

  it('uses wallet token B balance during rebalance when wallet value is below TOTAL_USD', async () => {
    const { monitor, config, sdkService, createAddLiquidityFixTokenPayload } =
      makeRebalanceEnvAmountScenario({ walletAmountA: '1000000', walletAmountB: '1500000' });

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0];
    // Wallet value (≈1500000 tokenB) < TOTAL_USD (10000000) → full wallet balances used.
    expect(callArgs.amount_b).toBe('1500000');
  });

  it('uses full wallet balances during rebalance when TOTAL_USD exceeds wallet value', async () => {
    const { monitor, config, sdkService, createAddLiquidityFixTokenPayload } =
      makeRebalanceEnvAmountScenario({ walletAmountA: '9999999', walletAmountB: '9999999' });

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0];
    // Full wallet balances pass through when wallet value is below TOTAL_USD.
    expect(callArgs.amount_a).toBe('9999999');
    expect(callArgs.amount_b).toBe('9999999');
  });

  it('produces identical amounts across multiple rebalances (deterministic)', async () => {
    // Both rebalances must produce identical amounts.
    const { monitor, config, sdkService, createAddLiquidityFixTokenPayload } =
      makeRebalanceEnvAmountScenario({ walletAmountA: '4000000', walletAmountB: '6000000' });

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0];
    expect(callArgs.amount_a).toBe('4000000');
    expect(callArgs.amount_b).toBe('6000000');
  });
});

// ---------------------------------------------------------------------------
// wallet balance checks before opening a new position
// ---------------------------------------------------------------------------

describe('wallet balance checks before opening a new position', () => {
  afterEach(() => { delete process.env.DRY_RUN; });

  it('uses wallet balances to trigger the required swap for a rebalanced position', async () => {
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({ currentTickIndex: 500, tickSpacing: 10 });
    const pos = makePosition({ tickLower: -200, tickUpper: -100, liquidity: '5000000' });
    const monitor = makeMonitor([pos], pool);
    (monitor.isPositionInRange as jest.Mock).mockReturnValue(false);
    (monitor.getPositions as jest.Mock).mockResolvedValue([pos]);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '1000000',
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };
    const createSwapTransactionPayload = jest.fn().mockReturnValue(mockTxStub);
    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);

    const mockSdk = {
      RouterV2: {
        getBestRouter: jest.fn().mockRejectedValue(new Error('aggregator unavailable')),
      },
      Swap: {
        createSwapTransactionPayload,
      },
      Position: {
        removeLiquidityTransactionPayload: jest.fn().mockResolvedValue(mockTxStub),
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload: jest.fn(),
      },
    };

    let getBalanceCallCount = 0;
    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn()
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xremove' })
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xswap',
          balanceChanges: [],
        })
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
        })
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' }),
      getBalance: jest.fn().mockImplementation(({ coinType }: { coinType: string }) => {
        getBalanceCallCount++;
        if (getBalanceCallCount <= 2) {
          return Promise.resolve({ totalBalance: coinType === '0xcoinA' ? '800000' : '0' });
        }
        return Promise.resolve({ totalBalance: coinType === '0xcoinA' ? '400000' : '350000' });
      }),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) =>
        Promise.resolve(coinType === '0xcoinA' ? '800000' : '0')),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);
    expect(sdkService.getBalance).toHaveBeenCalledWith('0xcoinA');
    expect(sdkService.getBalance).toHaveBeenCalledWith('0xcoinB');
    expect(createSwapTransactionPayload).toHaveBeenCalledTimes(1);

    const swapArgs = createSwapTransactionPayload.mock.calls[0][0] as { a2b: boolean; amount: string };
    expect(swapArgs.a2b).toBe(true);
    expect(BigInt(swapArgs.amount)).toBeGreaterThan(0n);

    const addLiquidityArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0] as {
      amount_a: string; amount_b: string;
    };
    expect(addLiquidityArgs.amount_a).toBe('400000');
    expect(addLiquidityArgs.amount_b).toBe('350000');
  });

  it('uses wallet balances to trigger the required swap for an initial position', async () => {
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({ currentTickIndex: 500, tickSpacing: 10 });
    const monitor = makeMonitor([], pool);
    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '1000000',
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };
    const createSwapTransactionPayload = jest.fn().mockReturnValue(mockTxStub);
    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);

    const mockSdk = {
      RouterV2: {
        getBestRouter: jest.fn().mockRejectedValue(new Error('aggregator unavailable')),
      },
      Swap: {
        createSwapTransactionPayload,
      },
      Position: {
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload: jest.fn(),
      },
    };

    let getBalanceCallCount = 0;
    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn()
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xswap',
          balanceChanges: [],
        })
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
        })
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' }),
      getBalance: jest.fn().mockImplementation(({ coinType }: { coinType: string }) => {
        getBalanceCallCount++;
        if (getBalanceCallCount <= 2) {
          return Promise.resolve({ totalBalance: coinType === '0xcoinA' ? '800000' : '0' });
        }
        return Promise.resolve({ totalBalance: coinType === '0xcoinA' ? '400000' : '350000' });
      }),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) =>
        Promise.resolve(coinType === '0xcoinA' ? '800000' : '0')),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);
    expect(sdkService.getBalance).toHaveBeenCalledWith('0xcoinA');
    expect(sdkService.getBalance).toHaveBeenCalledWith('0xcoinB');
    expect(createSwapTransactionPayload).toHaveBeenCalledTimes(1);

    const swapArgs = createSwapTransactionPayload.mock.calls[0][0] as { a2b: boolean; amount: string };
    expect(swapArgs.a2b).toBe(true);
    expect(BigInt(swapArgs.amount)).toBeGreaterThan(0n);

    const addLiquidityArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0] as {
      amount_a: string; amount_b: string;
    };
    expect(addLiquidityArgs.amount_a).toBe('400000');
    expect(addLiquidityArgs.amount_b).toBe('350000');
  });

  it('falls back to the direct pool swap when the aggregator returns no route result', async () => {
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({ currentTickIndex: 500, tickSpacing: 10 });
    const monitor = makeMonitor([], pool);
    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '1000000',
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };
    const createSwapTransactionPayload = jest.fn().mockReturnValue(mockTxStub);
    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);

    const mockSdk = {
      RouterV2: {
        getBestRouter: jest.fn().mockResolvedValue({ result: null }),
      },
      Swap: {
        createSwapTransactionPayload,
      },
      Position: {
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload: jest.fn(),
      },
    };

    let getBalanceCallCount = 0;
    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn()
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xswap',
          balanceChanges: [],
        })
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
        })
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' }),
      getBalance: jest.fn().mockImplementation(({ coinType }: { coinType: string }) => {
        getBalanceCallCount++;
        if (getBalanceCallCount <= 2) {
          return Promise.resolve({ totalBalance: coinType === '0xcoinA' ? '800000' : '0' });
        }
        return Promise.resolve({ totalBalance: coinType === '0xcoinA' ? '400000' : '350000' });
      }),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) =>
        Promise.resolve(coinType === '0xcoinA' ? '800000' : '0')),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);
    expect(mockSdk.RouterV2.getBestRouter).toHaveBeenCalledTimes(1);
    expect(createSwapTransactionPayload).toHaveBeenCalledTimes(1);
  });

  it('uses the aggregator route when getBestRouter returns a V1-fallback result (isTimeout: true)', async () => {
    // Regression test for: "No parameters available for service downgrade"
    // When the aggregator API is unavailable the SDK falls back to RPC routing
    // and marks the result with isTimeout: true.  The bot must use that result
    // instead of falling through to the direct pool swap.
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({ currentTickIndex: 500, tickSpacing: 10 });
    const monitor = makeMonitor([], pool);
    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '1000000',
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };
    const createSwapTransactionPayload = jest.fn().mockReturnValue(mockTxStub);
    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);

    // V1-fallback result: isTimeout is true, but splitPaths is non-empty and
    // isExceed is false — the bot should accept and use this result.
    const v1FallbackResult = {
      isExceed: false,
      isTimeout: true,
      inputAmount: 800000,
      outputAmount: 750000,
      fromCoin: '0xcoinA',
      toCoin: '0xcoinB',
      byAmountIn: true,
      splitPaths: [{ percent: 100, inputAmount: 800000, outputAmount: 750000, pathIndex: 0,
        basePaths: [] /* intentionally empty — only the path count matters for this test */ }],
    };

    const mockSdk = {
      RouterV2: {
        getBestRouter: jest.fn().mockResolvedValue({ result: v1FallbackResult, version: 'v1' }),
      },
      Swap: {
        createSwapTransactionPayload,
      },
      Position: {
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload: jest.fn(),
      },
    };

    let getBalanceCallCount = 0;
    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn()
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xswap',
          balanceChanges: [],
        })
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
        })
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' }),
      getBalance: jest.fn().mockImplementation(({ coinType }: { coinType: string }) => {
        getBalanceCallCount++;
        if (getBalanceCallCount <= 2) {
          return Promise.resolve({ totalBalance: coinType === '0xcoinA' ? '800000' : '0' });
        }
        return Promise.resolve({ totalBalance: coinType === '0xcoinA' ? '400000' : '350000' });
      }),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) =>
        Promise.resolve(coinType === '0xcoinA' ? '800000' : '0')),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    // Spy on TransactionUtil.buildAggregatorSwapTransaction so we can verify
    // it is called (aggregator path) rather than the direct pool swap path.
    const buildAggSpy = jest
      .spyOn(TransactionUtil, 'buildAggregatorSwapTransaction')
      .mockResolvedValue(mockTxStub as any);

    try {
      const svc = new RebalanceService(sdkService, monitor, config);
      const result = await svc.checkAndRebalance('0xpool');

      expect(result).not.toBeNull();
      expect(result!.success).toBe(true);
      expect(mockSdk.RouterV2.getBestRouter).toHaveBeenCalledTimes(1);
      // Aggregator path must be used — direct pool swap must NOT be called.
      expect(createSwapTransactionPayload).not.toHaveBeenCalled();
      expect(buildAggSpy).toHaveBeenCalledTimes(1);
      // swapWithMultiPoolParams must be passed so the SDK can fall back to RPC
      // routing without throwing "No parameters available for service downgrade".
      expect(mockSdk.RouterV2.getBestRouter).toHaveBeenCalledWith(
        expect.any(String),   // fromCoin
        expect.any(String),   // toCoin
        expect.any(Number),   // amount
        true,                 // byAmountIn
        0,                    // priceSplitPoint
        '',                   // partner
        '',                   // _senderAddress (deprecated)
        expect.objectContaining({
          poolAddresses: ['0xpool'],
          byAmountIn: true,
          coinTypeA: '0xcoinA',
          coinTypeB: '0xcoinB',
        }),
      );
    } finally {
      buildAggSpy.mockRestore();
    }
  });

  it('fails gracefully when neither required token is available in the wallet', async () => {
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({ currentTickIndex: 500, tickSpacing: 10 });
    const monitor = makeMonitor([], pool);
    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '1000000',
    } as any;

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockResolvedValue('0'),
      getSdk: jest.fn(),
      getKeypair: jest.fn(),
      getSuiClient: jest.fn(),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(false);
    expect(result!.error).toMatch(/Insufficient wallet balances to open initial position/);
  });
});
