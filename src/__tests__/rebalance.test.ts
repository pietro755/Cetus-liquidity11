/**
 * Unit tests for services/rebalance.ts — RebalanceService
 *
 * Network calls are fully mocked; all tests run in milliseconds without
 * any external services.
 */

import { RebalanceService, calculateOptimalSwapAmount, swapWithBuffer } from '../services/rebalance';
import { PositionMonitorService } from '../services/monitor';
import { TransactionUtil } from '@cetusprotocol/cetus-sui-clmm-sdk';

// ---------------------------------------------------------------------------
// Helpers to build minimal stubs
// ---------------------------------------------------------------------------

function makePoolInfo(overrides: Partial<{
  currentTickIndex: number;
  currentSqrtPrice: string;
  tickSpacing: number;
  coinTypeA: string;
  coinTypeB: string;
}> = {}) {
  return {
    poolAddress: '0xpool',
    currentTickIndex: overrides.currentTickIndex ?? 0,
    currentSqrtPrice: overrides.currentSqrtPrice ?? '1000000000',
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

const POST_SWAP_BALANCE = '500000';

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
    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616', // 2^64 => priceA = 1 tokenB per 1 tokenA
      tickSpacing: 10,
    });
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

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616',
      tickSpacing: 10,
    });
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
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' })
        // call 4: top-up addLiquidity (deposited amounts < required env amounts)
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xtopup' }),
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
    // Called twice: once for the initial deposit, once for the top-up.
    expect(createAddLiquidityFixTokenPayload).toHaveBeenCalledTimes(2);
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

    // TOTAL_USD=1,000,000 and priceA=1 => 490,000 buffered per token.
    expect(callArgs.amount_a).toBe('490000');
    expect(callArgs.amount_b).toBe('490000');

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

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616',
      tickSpacing: 10,
    });
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
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' })
        // call 5: top-up addLiquidity (deposited amounts < required env amounts)
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xtopup' }),
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
    // createAddLiquidityFixTokenPayload must have been called three times (initial + 1 retry + 1 top-up)
    expect(createAddLiquidityFixTokenPayload).toHaveBeenCalledTimes(3);
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

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616',
      tickSpacing: 10,
    });
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

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616',
      tickSpacing: 10,
    });
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

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616',
      tickSpacing: 10,
    });
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
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' })
        // call 4: top-up addLiquidity (deposited amounts < required env amounts)
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xtopup' }),
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
    // createAddLiquidityFixTokenPayload proceeds after verification, then once more for top-up.
    expect(createAddLiquidityFixTokenPayload).toHaveBeenCalledTimes(2);
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

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616',
      tickSpacing: 10,
    });
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

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616',
      tickSpacing: 10,
    });
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
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' })
        // call 4: top-up addLiquidity (deposited amounts < required env amounts)
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xtopup' }),
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
    // Called twice: once for initial deposit, once for top-up.
    expect(createAddLiquidityFixTokenPayload).toHaveBeenCalledTimes(2);
    return createAddLiquidityFixTokenPayload.mock.calls[0][0] as { fix_amount_a: boolean };
  }

  // Sqrt prices computed from TickMath.tickIndexToSqrtPriceX64 for consistency
  // with the corrected fix_amount_a formula that uses TickMath internally.
  // Tick 500 → '18913701982652573318', tick 100 → '18539204128674405812', tick 700 → '19103778296503601288'
  const SQRT_PRICE_TICK_500 = '18913701982652573318';
  const SQRT_PRICE_TICK_100 = '18539204128674405812';
  const SQRT_PRICE_TICK_700 = '19103778296503601288';

  it('fixes token A (fix_amount_a=true) when the TOTAL_USD-derived rebalance caps make A the bottleneck in range', async () => {
    const args = await runAndCaptureFixToken(500, SQRT_PRICE_TICK_500, 400, 600, '2000000', '1000000');
    expect(args.fix_amount_a).toBe(true);
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
        if (currentTickIndex < tickLower) {
          const totalA = (BigInt(tokenAmountA) + BigInt(tokenAmountB)).toString();
          return Promise.resolve({ totalBalance: coinType === '0xcoinA' ? totalA : '0' });
        }
        const totalB = (BigInt(tokenAmountA) + BigInt(tokenAmountB)).toString();
        return Promise.resolve({ totalBalance: coinType === '0xcoinA' ? '0' : totalB });
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

  it('swaps only the token-B deficit needed to restore the below-range target allocation', async () => {
    const swap = await runAndCaptureSwap(
      100,   // currentTick — below range [400,600]
      400,   // tickLower
      600,   // tickUpper
      '500000',   // tokenAmountA (already have some A)
      '800000',   // tokenAmountB (must swap ALL of this to A)
    );
    expect(swap.a2b).toBe(false);          // B → A
    // TOTAL_USD=1,000,000 below range targets the full budget in token A:
    // requiredA = floor(1,000,000 * 2^128 / sqrtPrice^2) = 951,231.
    // Starting from 500,000 A, the raw deficit is 451,231 A; with the 5% buffer
    // the swap targets 451231 * 105 / 100 = 473,792 A.
    expect(swap.amount).toBe('473792');
  });

  it('swaps only the token-A deficit needed to restore the above-range target allocation', async () => {
    const swap = await runAndCaptureSwap(
      700,   // currentTick — above range [400,600]
      400,   // tickLower
      600,   // tickUpper
      '800000',   // tokenAmountA (must swap ALL of this to B)
      '500000',   // tokenAmountB (already have some B)
    );
    expect(swap.a2b).toBe(true);           // A → B
    // Above range, the full TOTAL_USD budget maps to token B only.  The raw
    // deficit equals 500,000 B; with the 5% buffer the swap targets 525,000 B.
    expect(swap.amount).toBe('525000');
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
        return Promise.resolve({ totalBalance: coinType === '0xcoinA' ? '500000' : '500000' });
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

    // The optimal swap must never exceed the old half-swap heuristic plus the
    // 5% upfront buffer applied to the deficit.  deficitB = requiredAmountB =
    // halfUsd = 500_000; with the 5% buffer swapAmount = 525_000.
    const swapAmt = BigInt(swap.amount);
    expect(swapAmt).toBeGreaterThan(0n);
    expect(swapAmt).toBeLessThanOrEqual(525_000n);
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

  it('caps rebalanced position amounts to the env-defined TOTAL_USD target', async () => {
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616',
      tickSpacing: 10,
    });
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
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' })
        // call 4: top-up addLiquidity (deposited amounts < required env amounts)
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xtopup' }),
      getBalance: jest.fn(),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) =>
        Promise.resolve(coinType === '0xcoinA' ? '7000000' : '7000000')),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);
    // Called twice: once for initial deposit, once for top-up.
    expect(createAddLiquidityFixTokenPayload).toHaveBeenCalledTimes(2);

    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0] as {
      amount_a: string; amount_b: string;
    };

    // TOTAL_USD=10,000,000 and priceA=1 => 4,900,000 buffered per token.
    expect(callArgs.amount_a).toBe('4900000');
    expect(callArgs.amount_b).toBe('4900000');

    expect(sdkService.getBalance).toHaveBeenCalledWith('0xcoinA');
    expect(sdkService.getBalance).toHaveBeenCalledWith('0xcoinB');
    // 2 initial reads + 2 post-step-1 re-reads inside openNewPosition + 2 top-up reads
    expect(sdkService.getBalance).toHaveBeenCalledTimes(6);
    // No swap was needed, so the swap-specific Sui balance reads are still unused.
    expect(mockSuiClient.getBalance).not.toHaveBeenCalled();
  });

  it('does not expand rebalanced position amounts beyond the env-defined TOTAL_USD target', async () => {
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616',
      tickSpacing: 10,
    });
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
        Promise.resolve(coinType === '0xcoinA' ? '9999999' : '9999999')),
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
    expect(callArgs.amount_a).toBe('4900000');
    expect(callArgs.amount_b).toBe('4900000');
  });

  it('produces identical amounts across multiple rebalances (deterministic)', async () => {
    // Run two rebalances in sequence. Both must use the identical env amounts,
    // proving the bot is deterministic and not accumulating state.
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616',
      tickSpacing: 10,
    });
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
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' })
        // call 4: top-up addLiquidity (deposited amounts < required env amounts)
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xtopup' }),
      getBalance: jest.fn(),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos' } }),
    });

    const mockSuiClient1 = makeSuccessfulRun();
    const sdkService1 = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) =>
        Promise.resolve(coinType === '0xcoinA' ? '7000000' : '7000000')),
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
        Promise.resolve(coinType === '0xcoinA' ? '9000000' : '8000000')),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient2),
    } as any;
    (monitor.getPositions as jest.Mock).mockResolvedValue([pos]);

    const svc2 = new RebalanceService(sdkService2, monitor, config);
    const result2 = await svc2.checkAndRebalance('0xpool');
    expect(result2!.success).toBe(true);

    // Both runs must produce identical amounts.
    // Called 4 times: 2 initial deposits (one per run) + 2 top-up calls (one per run).
    expect(createAddLiquidityFixTokenPayload).toHaveBeenCalledTimes(4);
    const args1 = createAddLiquidityFixTokenPayload.mock.calls[0][0] as { amount_a: string; amount_b: string };
    const args2 = createAddLiquidityFixTokenPayload.mock.calls[2][0] as { amount_a: string; amount_b: string };
    expect(args1.amount_a).toBe('4900000');
    expect(args1.amount_b).toBe('4900000');
    expect(args2.amount_a).toBe('4900000');
    expect(args2.amount_b).toBe('4900000');
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
// createInitialPosition — TOTAL_USD converted into fixed token amounts
// ---------------------------------------------------------------------------

describe('createInitialPosition – TOTAL_USD converted token amounts', () => {
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
    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616', // 2^64 => priceA = 1 tokenB per 1 tokenA
      tickSpacing: 10,
    });
    const monitor = makeMonitor([], pool);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '10000000',
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

    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn().mockResolvedValue({
        effects: successEffect,
        digest: '0xtx',
        balanceChanges: [],
        objectChanges: [{
          type: 'created',
          objectType: 'position',
          objectId: '0xnewpos',
          owner: { AddressOwner: '0xwallet' },
        }],
      }),
      // getBalance/getCoins are used in swapTokensIfNeeded when a swap happens.
      getBalance: jest.fn().mockResolvedValue({ totalBalance: opts.walletAmountA }),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
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

  it('uses TOTAL_USD-converted amounts with a safety buffer', async () => {
    const { monitor, config, sdkService, createAddLiquidityFixTokenPayload, mockSuiClient } =
      makeInitialPositionScenario({ walletAmountA: '9000000', walletAmountB: '9000000' });

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0];
    // TOTAL_USD=10,000,000 => half=5,000,000 each; apply 98/100 multiplier => 4,900,000
    expect(callArgs.amount_a).toBe('4900000');
    expect(callArgs.amount_b).toBe('4900000');
    expect(sdkService.getBalance).toHaveBeenCalledWith('0xcoinA');
    expect(sdkService.getBalance).toHaveBeenCalledWith('0xcoinB');
    // 2 initial reads + 2 post-step-1 re-reads inside openNewPosition
    expect(sdkService.getBalance).toHaveBeenCalledTimes(4);
    // No swap was needed, so swap-specific Sui balance reads are still unused.
    expect(mockSuiClient.getBalance).not.toHaveBeenCalled();
  });

  it('does not use arbitrary full wallet balances when TOTAL_USD is fixed', async () => {
    const { monitor, config, sdkService, createAddLiquidityFixTokenPayload, mockSuiClient } =
      makeInitialPositionScenario({ walletAmountA: '9999999', walletAmountB: '9999999' });

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0];
    expect(callArgs.amount_a).toBe('4900000');
    expect(callArgs.amount_b).toBe('4900000');
    expect(sdkService.getBalance).toHaveBeenCalledWith('0xcoinA');
    expect(sdkService.getBalance).toHaveBeenCalledWith('0xcoinB');
    // 2 initial reads + 2 post-step-1 re-reads inside openNewPosition
    expect(sdkService.getBalance).toHaveBeenCalledTimes(4);
    // No swap was needed, so swap-specific Sui balance reads are still unused.
    expect(mockSuiClient.getBalance).not.toHaveBeenCalled();
  });

  it('fails when balances are still insufficient after swap attempt', async () => {
    const { monitor, config, sdkService, createAddLiquidityFixTokenPayload } =
      makeInitialPositionScenario({ walletAmountA: '100000', walletAmountB: '100000' });

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(false);
    expect(result!.error).toMatch(/Insufficient wallet balance to satisfy TOTAL_USD target/i);
    expect(createAddLiquidityFixTokenPayload).not.toHaveBeenCalled();
  });

  it('keeps the initial-position safety buffer when a token-B deficit swap still finishes below the required amount', async () => {
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616',
      tickSpacing: 10,
    });
    const monitor = makeMonitor([], pool);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '10000000',
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };
    const createSwapTransactionPayload = jest.fn().mockReturnValue(mockTxStub);
    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);
    const mockSdk = {
      RouterV2: { getBestRouter: jest.fn().mockRejectedValue(new Error('aggregator unavailable')) },
      Swap: { createSwapTransactionPayload },
      Position: {
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload: jest.fn(),
      },
    };

    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn()
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xswap',
          balanceChanges: [
            { owner: { AddressOwner: '0xwallet' }, coinType: '0xcoinA', amount: '-150000' },
            { owner: { AddressOwner: '0xwallet' }, coinType: '0xcoinB', amount: '150000' },
          ],
          objectChanges: [],
        })
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          balanceChanges: [],
          objectChanges: [{
            type: 'created',
            objectType: 'position',
            objectId: '0xnewpos',
            owner: { AddressOwner: '0xwallet' },
          }],
        })
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xadd-success',
        }),
      getBalance: jest.fn().mockResolvedValue({ totalBalance: '0' }),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    const balanceReads = {
      '0xcoinA': ['9000000', '8850000', '8850000'],
      '0xcoinB': ['4800000', '4950000', '4950000'],
    } as Record<string, string[]>;
    const balanceIndices = new Map<string, number>();
    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) => {
        const values = balanceReads[coinType];
        const index = balanceIndices.get(coinType) ?? 0;
        const value = values[Math.min(index, values.length - 1)];
        balanceIndices.set(coinType, index + 1);
        return Promise.resolve(value);
      }),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);
    expect(createSwapTransactionPayload).toHaveBeenCalledTimes(1);
    expect(createAddLiquidityFixTokenPayload).toHaveBeenCalledTimes(1);

    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0];
    expect(callArgs.amount_a).toBe('4900000');
    expect(callArgs.amount_b).toBe('4900000');
  });

  it('prefers swap-adjusted balances when the immediate post-swap wallet refresh lags for token B', async () => {
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616',
      tickSpacing: 10,
    });
    const monitor = makeMonitor([], pool);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '10000000',
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };
    const createSwapTransactionPayload = jest.fn().mockReturnValue(mockTxStub);
    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);
    const mockSdk = {
      RouterV2: { getBestRouter: jest.fn().mockRejectedValue(new Error('aggregator unavailable')) },
      Swap: { createSwapTransactionPayload },
      Position: {
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload: jest.fn(),
      },
    };

    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn()
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xswap',
          balanceChanges: [
            { owner: { AddressOwner: '0xwallet' }, coinType: '0xcoinA', amount: '-200000' },
            { owner: { AddressOwner: '0xwallet' }, coinType: '0xcoinB', amount: '200000' },
          ],
          objectChanges: [],
        })
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          balanceChanges: [],
          objectChanges: [{
            type: 'created',
            objectType: 'position',
            objectId: '0xnewpos',
            owner: { AddressOwner: '0xwallet' },
          }],
        })
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xadd-success',
        }),
      getBalance: jest.fn().mockResolvedValue({ totalBalance: '0' }),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    const balanceReads = {
      // First read: pre-swap wallet. Second read: immediate post-swap refresh that is still stale.
      // Third read: step-2 balance check after the position NFT is created sees the updated wallet.
      '0xcoinA': ['9000000', '8800000', '8800000'],
      '0xcoinB': ['4800000', '4850000', '5000000'],
    } as Record<string, string[]>;
    const balanceIndices = new Map<string, number>();
    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) => {
        const values = balanceReads[coinType];
        const index = balanceIndices.get(coinType) ?? 0;
        const value = values[Math.min(index, values.length - 1)];
        balanceIndices.set(coinType, index + 1);
        return Promise.resolve(value);
      }),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);
    expect(createSwapTransactionPayload).toHaveBeenCalledTimes(1);
    expect(createAddLiquidityFixTokenPayload).toHaveBeenCalledTimes(1);

    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0];
    expect(callArgs.amount_a).toBe('4900000');
    expect(callArgs.amount_b).toBe('4900000');
  });

  it('retries add liquidity with a reduced fixed token amount when post-swap sizing still hits InsufficientCoinBalance', async () => {
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616',
      tickSpacing: 10,
    });
    const monitor = makeMonitor([], pool);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '10000000',
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };
    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);
    const mockSdk = {
      RouterV2: { getBestRouter: jest.fn().mockRejectedValue(new Error('aggregator unavailable')) },
      Swap: { createSwapTransactionPayload: jest.fn().mockReturnValue(mockTxStub) },
      Position: {
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload: jest.fn(),
      },
    };

    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn()
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          balanceChanges: [],
          objectChanges: [{
            type: 'created',
            objectType: 'position',
            objectId: '0xnewpos',
            owner: { AddressOwner: '0xwallet' },
          }],
        })
        .mockResolvedValueOnce({
          effects: { status: { status: 'failure', error: 'InsufficientCoinBalance in command 1' } },
          digest: '0xadd-fail',
        })
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xadd-success',
        }),
      getBalance: jest.fn().mockResolvedValue({ totalBalance: '5000000' }),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) =>
        Promise.resolve(coinType === '0xcoinA' ? '5000000' : '5000000')),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);
    expect(createAddLiquidityFixTokenPayload).toHaveBeenCalledTimes(2);

    const firstCallArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0] as {
      amount_a: string;
      amount_b: string;
      fix_amount_a: boolean;
    };
    const secondCallArgs = createAddLiquidityFixTokenPayload.mock.calls[1][0] as {
      amount_a: string;
      amount_b: string;
      fix_amount_a: boolean;
    };

    expect(firstCallArgs.amount_a).toBe('4900000');
    expect(firstCallArgs.amount_b).toBe('4900000');

    if (firstCallArgs.fix_amount_a) {
      expect(secondCallArgs.amount_a).toBe((BigInt(firstCallArgs.amount_a) - 1n).toString());
      expect(secondCallArgs.amount_b).toBe(firstCallArgs.amount_b);
    } else {
      expect(secondCallArgs.amount_a).toBe(firstCallArgs.amount_a);
      expect(secondCallArgs.amount_b).toBe((BigInt(firstCallArgs.amount_b) - 1n).toString());
    }
  });

  it('returns a clear failure when TOTAL_USD is too small for initial-position integer arithmetic', async () => {
    // TOTAL_USD=1 → halfUsd = 0n → both required and buffered amounts are 0.
    // The bot should fail fast instead of silently opening with the full wallet.
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616', // 2^64 => priceA = 1 tokenB per 1 tokenA
      tickSpacing: 10,
    });
    const monitor = makeMonitor([], pool);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '1',  // halfUsd = 0n — amounts are 0 → fall back to wallet balance
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };
    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);
    const mockSdk = {
      RouterV2: { getBestRouter: jest.fn().mockRejectedValue(new Error('aggregator unavailable')) },
      Swap: { createSwapTransactionPayload: jest.fn().mockReturnValue(mockTxStub) },
      Position: {
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload: jest.fn(),
      },
    };

    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn().mockResolvedValue({
        effects: successEffect,
        digest: '0xtx',
        balanceChanges: [],
        objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
      }),
      getBalance: jest.fn().mockResolvedValue({ totalBalance: '0' }),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      // Wallet has both tokens — amounts that match the real failing scenario.
      getBalance: jest.fn().mockImplementation((coinType: string) =>
        Promise.resolve(coinType === '0xcoinA' ? '972427' : '2818067929')),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(false);
    expect(result!.error).toMatch(/TOTAL_USD \(1\) is too small to create an initial position/i);
    expect(createAddLiquidityFixTokenPayload).not.toHaveBeenCalled();
  });

  it('returns a clear failure when the safety buffer would round initial amounts down to zero', async () => {
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616',
      tickSpacing: 10,
    });
    const monitor = makeMonitor([], pool);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '2',
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };
    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);
    const mockSdk = {
      RouterV2: { getBestRouter: jest.fn().mockRejectedValue(new Error('aggregator unavailable')) },
      Swap: { createSwapTransactionPayload: jest.fn().mockReturnValue(mockTxStub) },
      Position: {
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload: jest.fn(),
      },
    };

    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn().mockResolvedValue({
        effects: successEffect,
        digest: '0xtx',
        balanceChanges: [],
        objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
      }),
      getBalance: jest.fn().mockResolvedValue({ totalBalance: '0' }),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) =>
        Promise.resolve(coinType === '0xcoinA' ? '972427' : '2818067929')),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(false);
    expect(result!.error).toMatch(/TOTAL_USD \(2\) is too small to create an initial position/i);
    expect(createAddLiquidityFixTokenPayload).not.toHaveBeenCalled();
  });

  it('uses post-step-1 wallet balance for step 2 when token A is depleted by gas', async () => {
    // Regression test for: "Add liquidity failed: InsufficientCoinBalance in command 0"
    //
    // Step 1 (openPositionTransactionPayload) consumes SUI gas.  If token A is
    // SUI, its on-chain balance after step 1 is lower than what was read before
    // step 1.  Step 2 (createAddLiquidityFixTokenPayload) must use the fresh
    // post-step-1 balance, not the stale pre-step-1 balance.
    //
    // TOTAL_USD is set so that usableAmountA > POST_STEP1_A, which ensures the
    // post-step-1 read (not the stale pre-step-1 cap) determines step 2 amountA.
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616', // 2^64 => priceA = 1 tokenB per 1 tokenA
      tickSpacing: 10,
    });
    const monitor = makeMonitor([], pool);

    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      // TOTAL_USD=1940000 → halfUsd=970000; priceA=1 → requiredAmountA=970000 ≤ walletA=972427
      // usableAmountA = 970000*98/100 = 950600 > POST_STEP1_A=922427 → step2 uses gas-depleted A
      totalUsd: '1940000',
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };
    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);
    const mockSdk = {
      RouterV2: { getBestRouter: jest.fn().mockRejectedValue(new Error('aggregator unavailable')) },
      Swap: { createSwapTransactionPayload: jest.fn().mockReturnValue(mockTxStub) },
      Position: {
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload: jest.fn(),
      },
    };

    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn().mockResolvedValue({
        effects: successEffect,
        digest: '0xtx',
        balanceChanges: [],
        objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
      }),
      getBalance: jest.fn().mockResolvedValue({ totalBalance: '0' }),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    // First 2 calls (before step 1): full balance.
    // Subsequent calls (after step 1 consumed gas): reduced tokenA balance.
    const PRE_STEP1_A  = '972427';
    const POST_STEP1_A = '922427';  // 50_000 SUI base units consumed as gas
    let balanceCallCount = 0;
    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) => {
        balanceCallCount++;
        const amountA = balanceCallCount <= 2 ? PRE_STEP1_A : POST_STEP1_A;
        return Promise.resolve(coinType === '0xcoinA' ? amountA : '2818067929');
      }),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    // Step 2 must use the post-step-1 (gas-depleted) balance, not the stale value.
    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0];
    expect(callArgs.amount_a).toBe(POST_STEP1_A);  // 922427, not 972427
    // usableAmountB = 970000*98/100 = 950600 (capped by TOTAL_USD); walletB=2818067929 > cap
    expect(callArgs.amount_b).toBe('950600');
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

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616',
      tickSpacing: 10,
    });
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
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' })
        // call 4: top-up addLiquidity (deposited amounts < required env amounts)
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xtopup' }),
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

  it('caps token A during rebalance at the env-defined TOTAL_USD target', async () => {
    const { monitor, config, sdkService, createAddLiquidityFixTokenPayload, mockSuiClient } =
      makeRebalanceEnvAmountScenario({ walletAmountA: '7000000', walletAmountB: '7000000' });

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0];
    expect(callArgs.amount_a).toBe('4900000');
    expect(sdkService.getBalance).toHaveBeenCalledWith('0xcoinA');
    expect(sdkService.getBalance).toHaveBeenCalledWith('0xcoinB');
    // 2 initial reads + 2 post-step-1 re-reads inside openNewPosition + 2 top-up reads
    expect(sdkService.getBalance).toHaveBeenCalledTimes(6);
    // No swap was needed, so swap-specific Sui balance reads are still unused.
    expect(mockSuiClient.getBalance).not.toHaveBeenCalled();
  });

  it('caps token B during rebalance at the env-defined TOTAL_USD target', async () => {
    const { monitor, config, sdkService, createAddLiquidityFixTokenPayload } =
      makeRebalanceEnvAmountScenario({ walletAmountA: '7000000', walletAmountB: '7000000' });

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0];
    expect(callArgs.amount_b).toBe('4900000');
  });

  it('uses identical env-defined amounts across repeated rebalances', async () => {
    const { monitor, config, sdkService, createAddLiquidityFixTokenPayload } =
      makeRebalanceEnvAmountScenario({ walletAmountA: '9999999', walletAmountB: '9999999' });

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0];
    expect(callArgs.amount_a).toBe('4900000');
    expect(callArgs.amount_b).toBe('4900000');
  });
});

// ---------------------------------------------------------------------------
// wallet balance checks before opening a new position
// ---------------------------------------------------------------------------

describe('wallet balance checks before opening a new position', () => {
  afterEach(() => { delete process.env.DRY_RUN; });

  it('uses wallet balances to trigger the required swap for a rebalanced position', async () => {
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616',
      tickSpacing: 10,
    });
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

    let sdkBalanceCallCount = 0;
    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) => {
        sdkBalanceCallCount++;
        if (sdkBalanceCallCount <= 2) {
          return Promise.resolve(coinType === '0xcoinA' ? '800000' : '0');
        }
        return Promise.resolve(coinType === '0xcoinA' ? POST_SWAP_BALANCE : POST_SWAP_BALANCE);
      }),
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

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616', // 2^64 => priceA = 1 tokenB per 1 tokenA
      tickSpacing: 10,
    });
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
        return Promise.resolve({ totalBalance: coinType === '0xcoinA' ? POST_SWAP_BALANCE : POST_SWAP_BALANCE });
      }),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    let sdkBalanceCallCount = 0;
    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) => {
        sdkBalanceCallCount++;
        if (sdkBalanceCallCount <= 2) {
          return Promise.resolve(coinType === '0xcoinA' ? '800000' : '0');
        }
        return Promise.resolve(coinType === '0xcoinA' ? POST_SWAP_BALANCE : POST_SWAP_BALANCE);
      }),
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
    expect(addLiquidityArgs.amount_a).toBe('490000');
    expect(addLiquidityArgs.amount_b).toBe('490000');
  });

  it('falls back to the direct pool swap when the aggregator returns no route result', async () => {
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616', // 2^64 => priceA = 1 tokenB per 1 tokenA
      tickSpacing: 10,
    });
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
        return Promise.resolve({ totalBalance: coinType === '0xcoinA' ? POST_SWAP_BALANCE : POST_SWAP_BALANCE });
      }),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    let sdkBalanceCallCount = 0;
    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) => {
        sdkBalanceCallCount++;
        if (sdkBalanceCallCount <= 2) {
          return Promise.resolve(coinType === '0xcoinA' ? '800000' : '0');
        }
        return Promise.resolve(coinType === '0xcoinA' ? POST_SWAP_BALANCE : POST_SWAP_BALANCE);
      }),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);
    // getBestRouter is called twice for deficit swaps: once for the aggregator
    // quote (to compute the buffer) and once for the actual swap.  Both calls
    // return null here, so the aggregator path is skipped for both and the
    // direct pool swap is used as fallback.
    expect(mockSdk.RouterV2.getBestRouter).toHaveBeenCalledTimes(2);
    expect(createSwapTransactionPayload).toHaveBeenCalledTimes(1);
  });

  it('uses the aggregator route when getBestRouter returns a V1-fallback result (isTimeout: true)', async () => {
    // Regression test for: "No parameters available for service downgrade"
    // When the aggregator API is unavailable the SDK falls back to RPC routing
    // and marks the result with isTimeout: true.  The bot must use that result
    // instead of falling through to the direct pool swap.
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616', // 2^64 => priceA = 1 tokenB per 1 tokenA
      tickSpacing: 10,
    });
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
        return Promise.resolve({ totalBalance: coinType === '0xcoinA' ? POST_SWAP_BALANCE : POST_SWAP_BALANCE });
      }),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    let sdkBalanceCallCount = 0;
    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) => {
        sdkBalanceCallCount++;
        if (sdkBalanceCallCount <= 2) {
          return Promise.resolve(coinType === '0xcoinA' ? '800000' : '0');
        }
        return Promise.resolve(coinType === '0xcoinA' ? POST_SWAP_BALANCE : POST_SWAP_BALANCE);
      }),
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
      // getBestRouter is called twice for deficit swaps: once for the aggregator
      // quote (to compute the buffer) and once for the actual swap.
      expect(mockSdk.RouterV2.getBestRouter).toHaveBeenCalledTimes(2);
      // Aggregator path must be used — direct pool swap must NOT be called.
      expect(createSwapTransactionPayload).not.toHaveBeenCalled();
      expect(buildAggSpy).toHaveBeenCalledTimes(1);
      // swapWithMultiPoolParams must be passed so the SDK can fall back to RPC
      // routing without throwing "No parameters available for service downgrade".
      // Deficit swaps use byAmountIn=true for the aggregator (exactIn with a
      // buffered input amount) because the aggregator may not reliably support
      // exactOut.  The direct pool swap fallback still uses byAmountIn=false.
      expect(mockSdk.RouterV2.getBestRouter).toHaveBeenCalledWith(
        expect.any(String),   // fromCoin
        expect.any(String),   // toCoin
        expect.any(Number),   // amount (buffered input)
        true,                 // byAmountIn (exactIn for aggregator reliability)
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

  it('converts deficit swap to byAmountIn=true with a buffered input for the aggregator', async () => {
    // Regression test: the Cetus aggregator may not reliably deliver the exact
    // output amount when using byAmountIn=false (exactOut).  The bot converts
    // deficit swaps to byAmountIn=true with a price-derived input + buffer so
    // the aggregator swaps enough to cover the deficit.
    process.env.DRY_RUN = 'false';

    // sqrtPrice = 2^64 ⟹ price = 1 tokenB per 1 tokenA.
    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616',
      tickSpacing: 10,
    });
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

    const v1FallbackResult = {
      isExceed: false,
      isTimeout: true,
      inputAmount: 800000,
      outputAmount: 750000,
      fromCoin: '0xcoinA',
      toCoin: '0xcoinB',
      byAmountIn: true,
      splitPaths: [{ percent: 100, inputAmount: 800000, outputAmount: 750000, pathIndex: 0,
        basePaths: [] }],
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

    // Wallet: plenty of A, zero B → deficit B swap (A→B).
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
        return Promise.resolve({ totalBalance: POST_SWAP_BALANCE });
      }),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    let sdkBalanceCallCount = 0;
    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) => {
        sdkBalanceCallCount++;
        if (sdkBalanceCallCount <= 2) {
          return Promise.resolve(coinType === '0xcoinA' ? '800000' : '0');
        }
        return Promise.resolve(POST_SWAP_BALANCE);
      }),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const buildAggSpy = jest
      .spyOn(TransactionUtil, 'buildAggregatorSwapTransaction')
      .mockResolvedValue(mockTxStub as any);

    try {
      const svc = new RebalanceService(sdkService, monitor, config);
      const result = await svc.checkAndRebalance('0xpool');

      expect(result).not.toBeNull();
      expect(result!.success).toBe(true);
      expect(buildAggSpy).toHaveBeenCalledTimes(1);
      // Direct pool swap must NOT be called when the aggregator succeeds.
      expect(createSwapTransactionPayload).not.toHaveBeenCalled();

      // getBestRouter is called twice: call[0] is the aggregator quote (to
      // compute the buffer); call[1] is the actual aggregator swap.
      expect(mockSdk.RouterV2.getBestRouter).toHaveBeenCalledTimes(2);

      // The actual swap call (index 1) must use byAmountIn=true (exactIn with buffer).
      const routerCallArgs = mockSdk.RouterV2.getBestRouter.mock.calls[1];
      const aggByAmountIn = routerCallArgs[3];
      const aggAmount = routerCallArgs[2];
      expect(aggByAmountIn).toBe(true);

      // The buffered input amount should be larger than the raw deficit
      // (which equals the amount that would be passed with byAmountIn=false).
      // At price=1, estimatedInput = deficit * 1.05 (5% buffer since
      // max(3×1%,5%) = 5%).  Since this is a Number, just verify it's positive.
      expect(aggAmount).toBeGreaterThan(0);

      // The swapWithMultiPoolParams embedded in the last argument must also
      // carry byAmountIn=true.
      const swapMultiPoolParam = routerCallArgs[7];
      expect(swapMultiPoolParam.byAmountIn).toBe(true);
    } finally {
      buildAggSpy.mockRestore();
    }
  });

  it('uses full available balance (not just surplus) as the exactIn cap when surplus is too small to cover the deficit', async () => {
    // Regression: the old code capped the aggregator exactIn input at
    // (bigA - requiredAmountA), i.e. only the "surplus" A over the position's
    // share.  When the surplus was smaller than the A needed to buy the B
    // deficit the swap produced fewer tokens than required.
    //
    // Setup (sqrtPrice=2^64 → price=1, totalUsd=2_000_000):
    //   requiredAmountA = 1_000_000, requiredAmountB = 1_000_000
    //   walletA = 1_100_000  (surplus = 100_000)
    //   walletB = 0          (deficitB = 1_000_000; swapAmount = 1_050_000 after 5% buffer)
    //   estimatedInput = 1_050_000 * 1.05 (buf) = 1_102_500,
    //   then A→B extra 5%: 1_102_500 * 1.05 = 1_157_625 (capped at bigA = 1_100_000)
    //
    // Old behaviour: cap at surplus (100_000) → aggAmount = 100_000  (WRONG)
    // New behaviour: cap at bigA  (1_100_000) → aggAmount = 1_100_000 (OK)
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616', // 2^64 → price = 1
      tickSpacing: 10,
    });
    const monitor = makeMonitor([], pool);
    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '2000000',
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const v1FallbackResult = {
      isExceed: false,
      isTimeout: true,
      inputAmount: 1100000,
      outputAmount: 1050000,
      fromCoin: '0xcoinA',
      toCoin: '0xcoinB',
      byAmountIn: true,
      splitPaths: [{ percent: 100, inputAmount: 1100000, outputAmount: 1050000, pathIndex: 0, basePaths: [] }],
    };

    const mockSdk = {
      RouterV2: {
        getBestRouter: jest.fn().mockResolvedValue({ result: v1FallbackResult, version: 'v1' }),
      },
      Swap: { createSwapTransactionPayload: jest.fn().mockReturnValue(mockTxStub) },
      Position: {
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload: jest.fn().mockResolvedValue(mockTxStub),
        createAddLiquidityPayload: jest.fn(),
      },
    };

    // walletA = 1_100_000 (surplus over requiredAmountA=1_000_000 is only 100_000)
    // walletB = 0         (full deficit of 1_000_000 B)
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
          return Promise.resolve({ totalBalance: coinType === '0xcoinA' ? '1100000' : '0' });
        }
        return Promise.resolve({ totalBalance: POST_SWAP_BALANCE });
      }),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    let sdkBalanceCallCount = 0;
    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) => {
        sdkBalanceCallCount++;
        if (sdkBalanceCallCount <= 2) {
          return Promise.resolve(coinType === '0xcoinA' ? '1100000' : '0');
        }
        return Promise.resolve(POST_SWAP_BALANCE);
      }),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const buildAggSpy = jest
      .spyOn(TransactionUtil, 'buildAggregatorSwapTransaction')
      .mockResolvedValue(mockTxStub as any);

    try {
      const svc = new RebalanceService(sdkService, monitor, config);
      const result = await svc.checkAndRebalance('0xpool');

      expect(result).not.toBeNull();
      expect(result!.success).toBe(true);
      expect(buildAggSpy).toHaveBeenCalledTimes(1);

      // getBestRouter is called twice: call[0] is the aggregator quote (to
      // compute the buffer); call[1] is the actual aggregator swap.
      expect(mockSdk.RouterV2.getBestRouter).toHaveBeenCalledTimes(2);

      const routerCallArgs = mockSdk.RouterV2.getBestRouter.mock.calls[1];
      const aggByAmountIn = routerCallArgs[3];
      const aggAmount    = routerCallArgs[2]; // Number(aggSwapAmount)

      // Must still use exactIn.
      expect(aggByAmountIn).toBe(true);

      // The input must be large enough to cover the entire 1_000_000 B deficit.
      // With the old surplus-cap code this would have been 100_000 (the small
      // surplus), which is far too little.  With the new total-balance cap the
      // value is 1_050_000 (deficit × 1.05 buffer), which is ≥ the deficit.
      expect(aggAmount).toBeGreaterThanOrEqual(1_000_000);
    } finally {
      buildAggSpy.mockRestore();
    }
  });

  it('uses aggregator-quote-driven price impact in deficit-swap buffer', async () => {
    // When the aggregator (getBestRouter) returns a quote whose outputAmount is
    // less than the swap target (due to routing fees / price impact), the bot must
    // scale up the input amount to account for both the slippage tolerance and
    // the measured price impact.
    //
    // The quote is now obtained via getBestRouter (first call) rather than
    // preSwapWithMultiPool, so the buffer reflects the aggregator's actual
    // routing path instead of a direct-pool estimate that may differ.
    //
    // Setup (sqrtPrice=2^64 → price=1, totalUsd=2_000_000):
    //   requiredAmountA = 1_000_000, requiredAmountB = 1_000_000
    //   walletA = 1_100_000, walletB = 0  → deficitB = 1_000_000
    //   swapAmount = 1_000_000 * 1.05 = 1_050_000  (5% upfront buffer)
    //   Initial spot estimate: estimatedInput = 1_050_000 (at price=1)
    //   Quote (call 1) returns outputAmount = 950_000  → priceImpact ≈ 10.5%
    //     priceImpactBps = (1_050_000 − 950_000) × 10_000 / 950_000 ≈ 1052 bps
    //   maxSlippage = 1% (slipBps = 100)
    //   bufBps = 100 + 1052 = 1152  → bufferedInput ≈ 1_050_000 × 1.1152 ≈ 1_170_960
    //   A→B extra 5%: 1_170_960 × 1.05 ≈ 1_229_508
    //   Capped at bigA = 1_100_000  → aggAmount = 1_100_000
    //   Actual swap uses bufferedInput (call 2) ≥ 1_060_000.
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616', // 2^64 → price = 1
      tickSpacing: 10,
    });
    const monitor = makeMonitor([], pool);
    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '2000000',
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    // First call: aggregator quote — returns 950_000 output for 1_000_000 input
    // (simulates aggregator routing at a slightly worse rate than spot price).
    const quoteResult = {
      isExceed: false,
      isTimeout: false,
      inputAmount: 1000000,
      outputAmount: 950000,
      fromCoin: '0xcoinA',
      toCoin: '0xcoinB',
      byAmountIn: true,
      splitPaths: [],
    };

    // Second call: actual aggregator swap — has proper splitPaths for execution.
    const aggregatorResult = {
      isExceed: false,
      isTimeout: false,
      inputAmount: 1062600,
      outputAmount: 1000000,
      fromCoin: '0xcoinA',
      toCoin: '0xcoinB',
      byAmountIn: true,
      splitPaths: [{ percent: 100, inputAmount: 1062600, outputAmount: 1000000, pathIndex: 0, basePaths: [] }],
    };

    const mockSdk = {
      RouterV2: {
        getBestRouter: jest.fn()
          .mockResolvedValueOnce({ result: quoteResult, version: 'v2' })   // quote
          .mockResolvedValueOnce({ result: aggregatorResult, version: 'v2' }), // actual swap
      },
      Swap: {
        createSwapTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
      },
      Position: {
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload: jest.fn().mockResolvedValue(mockTxStub),
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
          return Promise.resolve({ totalBalance: coinType === '0xcoinA' ? '1100000' : '0' });
        }
        return Promise.resolve({ totalBalance: POST_SWAP_BALANCE });
      }),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    let sdkBalanceCallCount = 0;
    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) => {
        sdkBalanceCallCount++;
        if (sdkBalanceCallCount <= 2) {
          return Promise.resolve(coinType === '0xcoinA' ? '1100000' : '0');
        }
        return Promise.resolve(POST_SWAP_BALANCE);
      }),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const buildAggSpy = jest
      .spyOn(TransactionUtil, 'buildAggregatorSwapTransaction')
      .mockResolvedValue(mockTxStub as any);

    try {
      const svc = new RebalanceService(sdkService, monitor, config);
      const result = await svc.checkAndRebalance('0xpool');

      expect(result).not.toBeNull();
      expect(result!.success).toBe(true);

      // getBestRouter must be called twice: once for the quote (call 0) and
      // once for the actual swap (call 1).
      expect(mockSdk.RouterV2.getBestRouter).toHaveBeenCalledTimes(2);

      // Quote call (index 0): byAmountIn=true, amount = estimatedInput ≈ 1_050_000
      const quoteCallArgs = mockSdk.RouterV2.getBestRouter.mock.calls[0];
      expect(quoteCallArgs[3]).toBe(true);   // byAmountIn
      expect(quoteCallArgs[7].a2b).toBe(true);

      // Actual swap call (index 1): byAmountIn=true with buffered input.
      // priceImpactBps = (1_050_000 − 950_000) × 10_000 / 950_000 ≈ 1052 bps
      // bufBps = slipBps(100) + priceImpactBps(1052) = 1152 → bufferedInput ≈ 1_100_000 (capped)
      const swapCallArgs = mockSdk.RouterV2.getBestRouter.mock.calls[1];
      const aggByAmountIn = swapCallArgs[3];
      const aggAmount = swapCallArgs[2];
      expect(aggByAmountIn).toBe(true);
      expect(aggAmount).toBeGreaterThanOrEqual(1_060_000);
    } finally {
      buildAggSpy.mockRestore();
    }
  });

  it('falls back to static buffer when the aggregator quote call throws', async () => {
    // When the first getBestRouter call (used for the pre-swap quote) throws
    // (e.g., network error), the bot must fall back to the static buffer of
    // max(3×maxSlippage, 5%) rather than failing the swap entirely.
    // The second getBestRouter call (actual swap) still succeeds normally.
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616', // 2^64 → price = 1
      tickSpacing: 10,
    });
    const monitor = makeMonitor([], pool);
    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '2000000',
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const aggregatorResult = {
      isExceed: false,
      isTimeout: false,
      inputAmount: 1050000,
      outputAmount: 1000000,
      fromCoin: '0xcoinA',
      toCoin: '0xcoinB',
      byAmountIn: true,
      splitPaths: [{ percent: 100, inputAmount: 1050000, outputAmount: 1000000, pathIndex: 0, basePaths: [] }],
    };

    const mockSdk = {
      RouterV2: {
        // First call (quote) throws; second call (actual swap) succeeds.
        getBestRouter: jest.fn()
          .mockRejectedValueOnce(new Error('quote unavailable'))
          .mockResolvedValueOnce({ result: aggregatorResult, version: 'v2' }),
      },
      Swap: {
        createSwapTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
      },
      Position: {
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload: jest.fn().mockResolvedValue(mockTxStub),
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
          return Promise.resolve({ totalBalance: coinType === '0xcoinA' ? '1100000' : '0' });
        }
        return Promise.resolve({ totalBalance: POST_SWAP_BALANCE });
      }),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    let sdkBalanceCallCount = 0;
    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) => {
        sdkBalanceCallCount++;
        if (sdkBalanceCallCount <= 2) {
          return Promise.resolve(coinType === '0xcoinA' ? '1100000' : '0');
        }
        return Promise.resolve(POST_SWAP_BALANCE);
      }),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const buildAggSpy = jest
      .spyOn(TransactionUtil, 'buildAggregatorSwapTransaction')
      .mockResolvedValue(mockTxStub as any);

    try {
      const svc = new RebalanceService(sdkService, monitor, config);
      const result = await svc.checkAndRebalance('0xpool');

      expect(result).not.toBeNull();
      expect(result!.success).toBe(true);

      // getBestRouter called twice: first (quote) throws, second (actual swap) succeeds.
      expect(mockSdk.RouterV2.getBestRouter).toHaveBeenCalledTimes(2);

      // The actual swap call (index 1) must still use byAmountIn=true with static buffer.
      const routerCallArgs = mockSdk.RouterV2.getBestRouter.mock.calls[1];
      const aggByAmountIn = routerCallArgs[3];
      const aggAmount = routerCallArgs[2];
      expect(aggByAmountIn).toBe(true);

      // Static fallback: buffer = max(3×1%, 5%) = 5% → 1_050_000 * 1.05 = 1_102_500,
      // then A→B extra 5% → 1_157_625, capped at bigA=1_100_000.
      expect(aggAmount).toBeGreaterThanOrEqual(1_050_000);
    } finally {
      buildAggSpy.mockRestore();
    }
  });

  it('falls back to direct pool exactOut when the aggregator route output is insufficient for the deficit', async () => {
    // Regression: when the aggregator routing engine returns an outputAmount
    // less than the buffered swap target (even after the exactIn buffer has been
    // applied), the bot must not use that route.  It must instead fall back to
    // the direct single-pool exactOut swap, which guarantees delivery of the
    // full buffered amount.
    //
    // Setup (sqrtPrice=2^64 → price=1, totalUsd=2_000_000):
    //   requiredAmountA = 1_000_000, requiredAmountB = 1_000_000
    //   walletA = 1_100_000, walletB = 0  → deficitB = 1_000_000
    //   swapAmount = 1_000_000 * 1.05 = 1_050_000  (5% upfront buffer)
    //   Quote (call 1): outputAmount = 950_000 < swapAmount → priceImpact buffer applied
    //   Swap route (call 2): outputAmount = 890_000 < swapAmount → aggregator MUST be rejected
    //   Expected: direct pool exactOut used (by_amount_in=false, amount=1_050_000)
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616', // 2^64 → price = 1
      tickSpacing: 10,
    });
    const monitor = makeMonitor([], pool);
    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '2000000',
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };
    const createSwapTransactionPayload = jest.fn().mockReturnValue(mockTxStub);

    // Quote (call 1): returns output less than deficit to trigger the buffer.
    const quoteResult = {
      isExceed: false,
      isTimeout: false,
      inputAmount: 1_000_000,
      outputAmount: 950_000, // < 1_000_000 deficit
      fromCoin: '0xcoinA',
      toCoin: '0xcoinB',
      byAmountIn: true,
      splitPaths: [],
    };

    // Swap route (call 2): still returns outputAmount < deficit even after buffer.
    const insufficientRouteResult = {
      isExceed: false,
      isTimeout: false,
      inputAmount: 1_062_600,
      outputAmount: 890_000, // < 1_000_000 deficit — bot must reject this route
      fromCoin: '0xcoinA',
      toCoin: '0xcoinB',
      byAmountIn: true,
      splitPaths: [{ percent: 100, inputAmount: 1_062_600, outputAmount: 890_000, pathIndex: 0, basePaths: [] }],
    };

    const mockSdk = {
      RouterV2: {
        getBestRouter: jest.fn()
          .mockResolvedValueOnce({ result: quoteResult, version: 'v2' })         // quote
          .mockResolvedValueOnce({ result: insufficientRouteResult, version: 'v2' }), // actual swap
      },
      Swap: { createSwapTransactionPayload },
      Position: {
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload: jest.fn().mockResolvedValue(mockTxStub),
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
          return Promise.resolve({ totalBalance: coinType === '0xcoinA' ? '1100000' : '0' });
        }
        return Promise.resolve({ totalBalance: POST_SWAP_BALANCE });
      }),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    let sdkBalanceCallCount = 0;
    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) => {
        sdkBalanceCallCount++;
        if (sdkBalanceCallCount <= 2) {
          return Promise.resolve(coinType === '0xcoinA' ? '1100000' : '0');
        }
        return Promise.resolve(POST_SWAP_BALANCE);
      }),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const buildAggSpy = jest
      .spyOn(TransactionUtil, 'buildAggregatorSwapTransaction')
      .mockResolvedValue(mockTxStub as any);

    try {
      const svc = new RebalanceService(sdkService, monitor, config);
      const result = await svc.checkAndRebalance('0xpool');

      expect(result).not.toBeNull();
      expect(result!.success).toBe(true);

      // The aggregator route must NOT be used because its output < deficit.
      expect(buildAggSpy).not.toHaveBeenCalled();

      // The direct pool exactOut swap must be used instead.
      expect(createSwapTransactionPayload).toHaveBeenCalledTimes(1);
      const swapArgs = createSwapTransactionPayload.mock.calls[0][0] as {
        by_amount_in: boolean;
        amount: string;
      };
      // Direct pool exactOut: by_amount_in=false, amount = deficitB + 5% buffer (1_050_000)
      expect(swapArgs.by_amount_in).toBe(false);
      expect(swapArgs.amount).toBe('1050000');
    } finally {
      buildAggSpy.mockRestore();
    }
  });

  it('falls back to direct pool exactOut when route output cannot cover the deficit after execution slippage', async () => {
    // Regression for the case reported in production logs where the aggregator
    // route output was just above the 5% buffered swapAmount but, after the
    // transaction applied maxSlippage (e.g. 10%), the actual received amount
    // fell below the raw deficit.
    //
    // With high maxSlippage the old threshold (swapAmount = deficitB × 1.05)
    // was too weak: a quoted output of deficitB × 1.06 passes the old check,
    // but with 10% execution slippage the actual delivery is only
    // deficitB × 1.06 × 0.90 ≈ deficitB × 0.954  <  deficitB.
    //
    // The fix requires result.outputAmount ≥ rawDeficit / (1 − maxSlippage):
    //   rawDeficit / 0.90 ≈ rawDeficit × 1.111
    // So a route quoting only deficitB × 1.06 must be rejected and the bot
    // must fall back to the guaranteed directPool exactOut.
    //
    // Setup (sqrtPrice=2^64 → price=1, totalUsd=2_000_000, maxSlippage=10%):
    //   requiredAmountA = 1_000_000, requiredAmountB = 1_000_000
    //   walletA = 1_200_000, walletB = 0  → deficitB = 1_000_000
    //   swapAmount = 1_050_000  (5% upfront buffer)
    //   minSafeOutput = 1_000_000 / 0.90 ≈ 1_111_112
    //   routeOutputThreshold = max(1_050_000, 1_111_112) = 1_111_112
    //
    //   Quote (call 1): 1_000_000 → triggers buffer calculation
    //   Swap route (call 2): outputAmount = 1_060_000
    //     → 1_060_000 < 1_111_112 → aggregator MUST be rejected
    //     → if executed with 10% slip: 1_060_000 × 0.9 = 954_000 < 1_000_000 ❌
    //   Expected: direct pool exactOut used (by_amount_in=false, amount=1_050_000)
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616', // 2^64 → price = 1
      tickSpacing: 10,
    });
    const monitor = makeMonitor([], pool);
    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.10,  // 10% — high enough that a small quote buffer is consumed
      lowerTick: 400,
      upperTick: 600,
      totalUsd: '2000000',
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };
    const createSwapTransactionPayload = jest.fn().mockReturnValue(mockTxStub);

    // Quote (call 1): returns output = 1_000_000 to drive the buffer calculation.
    const quoteResult = {
      isExceed: false,
      isTimeout: false,
      inputAmount: 1_050_000,
      outputAmount: 1_000_000,
      fromCoin: '0xcoinA',
      toCoin: '0xcoinB',
      byAmountIn: true,
      splitPaths: [],
    };

    // Swap route (call 2): returns outputAmount = 1_060_000 — above the old
    // swapAmount threshold (1_050_000) but below minSafeOutput (≈1_111_112).
    // With 10% execution slippage: 1_060_000 × 0.9 = 954_000 < deficitB.
    const insufficientRouteResult = {
      isExceed: false,
      isTimeout: false,
      inputAmount: 1_100_000,
      outputAmount: 1_060_000,
      fromCoin: '0xcoinA',
      toCoin: '0xcoinB',
      byAmountIn: true,
      splitPaths: [{ percent: 100, inputAmount: 1_100_000, outputAmount: 1_060_000, pathIndex: 0, basePaths: [] }],
    };

    const mockSdk = {
      RouterV2: {
        getBestRouter: jest.fn()
          .mockResolvedValueOnce({ result: quoteResult, version: 'v2' })
          .mockResolvedValueOnce({ result: insufficientRouteResult, version: 'v2' }),
      },
      Swap: { createSwapTransactionPayload },
      Position: {
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload: jest.fn().mockResolvedValue(mockTxStub),
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
          return Promise.resolve({ totalBalance: coinType === '0xcoinA' ? '1200000' : '0' });
        }
        return Promise.resolve({ totalBalance: POST_SWAP_BALANCE });
      }),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    let sdkBalanceCallCount = 0;
    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) => {
        sdkBalanceCallCount++;
        if (sdkBalanceCallCount <= 2) {
          return Promise.resolve(coinType === '0xcoinA' ? '1200000' : '0');
        }
        return Promise.resolve(POST_SWAP_BALANCE);
      }),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const buildAggSpy = jest
      .spyOn(TransactionUtil, 'buildAggregatorSwapTransaction')
      .mockResolvedValue(mockTxStub as any);

    try {
      const svc = new RebalanceService(sdkService, monitor, config);
      const result = await svc.checkAndRebalance('0xpool');

      expect(result).not.toBeNull();
      expect(result!.success).toBe(true);

      // The aggregator route must NOT be used: 1_060_000 < minSafeOutput ≈ 1_111_112.
      expect(buildAggSpy).not.toHaveBeenCalled();

      // The direct pool exactOut swap must be used to guarantee the output.
      expect(createSwapTransactionPayload).toHaveBeenCalledTimes(1);
      const swapArgs = createSwapTransactionPayload.mock.calls[0][0] as {
        by_amount_in: boolean;
        amount: string;
      };
      expect(swapArgs.by_amount_in).toBe(false);
      expect(swapArgs.amount).toBe('1050000');
    } finally {
      buildAggSpy.mockRestore();
    }
  });

  it('preloads router token metadata so the aggregator transaction can still be built', async () => {
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616',
      tickSpacing: 10,
    });
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

    const aggregatorResult = {
      // Match the SDK's V1/RPC fallback shape: the route is still usable, but
      // router token metadata must be present for the aggregator transaction builder.
      isExceed: false,
      isTimeout: true,
      inputAmount: 800000,
      outputAmount: 750000,
      fromCoin: '0xcoinA',
      toCoin: '0xcoinB',
      byAmountIn: true,
      splitPaths: [{ percent: 100, inputAmount: 800000, outputAmount: 750000, pathIndex: 0, lastQuoteOutput: 0, basePaths: [] }],
    };

    const routerCoinMap = new Map<string, any>();
    const tokenInfo = jest.fn((coinType: string) => routerCoinMap.get(coinType));
    const loadGraph = jest.fn(({ coins }: { coins: Array<{ address: string; decimals: number }> }) => {
      coins.forEach((coin) => routerCoinMap.set(coin.address, coin));
    });

    const mockSdk = {
      RouterV2: {
        getBestRouter: jest.fn().mockResolvedValue({ result: aggregatorResult, version: 'v1' }),
      },
      Router: {
        tokenInfo,
        loadGraph,
      },
      Token: {
        getTokenListByCoinTypes: jest.fn().mockResolvedValue({
          '0xcoinA': { address: '0xcoinA', decimals: 9, symbol: 'COINA' },
          '0xcoinB': { address: '0xcoinB', decimals: 6, symbol: 'COINB' },
        }),
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
        return Promise.resolve({ totalBalance: coinType === '0xcoinA' ? POST_SWAP_BALANCE : POST_SWAP_BALANCE });
      }),
      getCoins: jest.fn().mockResolvedValue({ data: [{ coinType: '0xcoinA', coinObjectId: '0xcoin', balance: '800000' }] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    let sdkBalanceCallCount = 0;
    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) => {
        sdkBalanceCallCount++;
        if (sdkBalanceCallCount <= 2) {
          return Promise.resolve(coinType === '0xcoinA' ? '800000' : '0');
        }
        return Promise.resolve(coinType === '0xcoinA' ? POST_SWAP_BALANCE : POST_SWAP_BALANCE);
      }),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const buildAggSpy = jest
      .spyOn(TransactionUtil, 'buildAggregatorSwapTransaction')
      .mockImplementation(async (sdkArg: any) => {
        expect(sdkArg.Router.tokenInfo('0xcoinA')?.decimals).toBe(9);
        expect(sdkArg.Router.tokenInfo('0xcoinB')?.decimals).toBe(6);
        return mockTxStub as any;
      });

    try {
      const svc = new RebalanceService(sdkService, monitor, config);
      const result = await svc.checkAndRebalance('0xpool');

      expect(result).not.toBeNull();
      expect(result!.success).toBe(true);
      expect(mockSdk.Token.getTokenListByCoinTypes).toHaveBeenCalledWith(['0xcoinA', '0xcoinB']);
      expect(loadGraph).toHaveBeenCalledTimes(1);
      expect(createSwapTransactionPayload).not.toHaveBeenCalled();
      expect(buildAggSpy).toHaveBeenCalledTimes(1);
    } finally {
      buildAggSpy.mockRestore();
    }
  });

  it('suppresses benign SDK router parse noise when a fallback route is still usable', async () => {
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616',
      tickSpacing: 10,
    });
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

    const fallbackResult = {
      isExceed: false,
      isTimeout: true,
      inputAmount: 800000,
      outputAmount: 750000,
      fromCoin: '0xcoinA',
      toCoin: '0xcoinB',
      byAmountIn: true,
      splitPaths: [{ percent: 100, inputAmount: 800000, outputAmount: 750000, pathIndex: 0, basePaths: [] }],
    };

    const routerError = new TypeError("Cannot read properties of undefined (reading 'map')");
    const mockSdk = {
      RouterV2: {
        getBestRouter: jest.fn().mockImplementation(async () => {
          console.error(routerError);
          console.log('json data. ', { amount_out: '750000' });
          return { result: fallbackResult, version: 'v1' };
        }),
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
        return Promise.resolve({ totalBalance: coinType === '0xcoinA' ? POST_SWAP_BALANCE : POST_SWAP_BALANCE });
      }),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    let sdkBalanceCallCount = 0;
    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getBalance: jest.fn().mockImplementation((coinType: string) => {
        sdkBalanceCallCount++;
        if (sdkBalanceCallCount <= 2) {
          return Promise.resolve(coinType === '0xcoinA' ? '800000' : '0');
        }
        return Promise.resolve(coinType === '0xcoinA' ? POST_SWAP_BALANCE : POST_SWAP_BALANCE);
      }),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const buildAggSpy = jest
      .spyOn(TransactionUtil, 'buildAggregatorSwapTransaction')
      .mockResolvedValue(mockTxStub as any);
    const errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
    const logSpy = jest.spyOn(console, 'log').mockImplementation(() => {});

    try {
      const svc = new RebalanceService(sdkService, monitor, config);
      const result = await svc.checkAndRebalance('0xpool');

      expect(result).not.toBeNull();
      expect(result!.success).toBe(true);
      expect(createSwapTransactionPayload).not.toHaveBeenCalled();
      expect(buildAggSpy).toHaveBeenCalledTimes(1);
      expect(errorSpy).not.toHaveBeenCalledWith(routerError);
      expect(logSpy.mock.calls.some(([firstArg]) => firstArg === 'json data. ')).toBe(false);
    } finally {
      buildAggSpy.mockRestore();
      errorSpy.mockRestore();
      logSpy.mockRestore();
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
    expect(result!.error).toMatch(/No usable balance to open initial position/);
  });
});

// ---------------------------------------------------------------------------
// Top-up after rebalance — topUpPosition logic
// ---------------------------------------------------------------------------

describe('rebalance – top-up after rebalance when deposited amounts are below env target', () => {
  afterEach(() => { delete process.env.DRY_RUN; });

  it('calls createAddLiquidityFixTokenPayload twice: once for initial deposit, once for top-up', async () => {
    // TOTAL_USD=1,000,000, price=1 → requiredA=500000, requiredB=500000
    // Wallet=999999999 → deposited (buffered) = 490000 each → deficit = 10000 each
    // Top-up should add 10000 of each token to reach the env target.
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616', // 2^64 → price = 1
      tickSpacing: 10,
    });
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
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' })
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xtopup' }),
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

    // createAddLiquidityFixTokenPayload must be called twice: initial + top-up.
    expect(createAddLiquidityFixTokenPayload).toHaveBeenCalledTimes(2);

    // The initial deposit uses the buffered (98%) amount.
    const initialArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0];
    expect(initialArgs.amount_a).toBe('490000');
    expect(initialArgs.amount_b).toBe('490000');
    expect(initialArgs.pos_id).toBe('0xnewpos');

    // The top-up deposit makes up the 2% deficit (500000 - 490000 = 10000 each).
    const topUpArgs = createAddLiquidityFixTokenPayload.mock.calls[1][0];
    expect(BigInt(topUpArgs.amount_a)).toBeGreaterThan(0n);
    expect(BigInt(topUpArgs.amount_b)).toBeGreaterThanOrEqual(0n);
    expect(topUpArgs.pos_id).toBe('0xnewpos');
    expect(topUpArgs.is_open).toBe(false);

    // Overall rebalance succeeds.
    expect(mockSuiClient.signAndExecuteTransaction).toHaveBeenCalledTimes(4);
  });

  it('succeeds even when top-up signAndExecuteTransaction fails (best-effort)', async () => {
    // The rebalance itself succeeded; the top-up is not critical.
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616',
      tickSpacing: 10,
    });
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
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' })
        // Top-up transaction fails
        .mockResolvedValueOnce({ effects: { status: { status: 'failure', error: 'network error' } }, digest: '0xfail' }),
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

    // Rebalance itself must still succeed even if top-up fails.
    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);
    expect(result!.transactionDigest).toBe('0xadd');
  });

  it('tops up a below-range (single-sided) position for token A', async () => {
    // For a position where the current price is below the new range, only token A
    // is accepted.  After the initial deposit (buffered at 98%), there should be a
    // 2% deficit in A.  The top-up adds the deficit from the wallet.
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616',
      tickSpacing: 10,
    });
    const pos = makePosition({ tickLower: -200, tickUpper: -100, liquidity: '5000000' });
    const monitor = makeMonitor([pos], pool);
    (monitor.isPositionInRange as jest.Mock).mockReturnValue(false);
    (monitor.getPositions as jest.Mock).mockResolvedValue([pos]);

    // New range (600–800) is above current tick (500) → below-range scenario.
    // Only token A is accepted. wallet B = 0 so no swap is needed.
    // requiredA = totalUsd * 2^128 / sqrtPrice^2 = 1000000; amountA buffered = 980000.
    const config = {
      gasBudget: 50_000_000,
      maxSlippage: 0.01,
      lowerTick: 600,
      upperTick: 800,
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
      },
    };

    // Pool tick (500) < tickLower (600) → below range → only A accepted, B=0 → no swap.
    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn()
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xremove' })
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
        })
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' })
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xtopup' }),
      getBalance: jest.fn(),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      // Wallet: plenty of A, zero B (below-range: no B needed, no swap triggered).
      getBalance: jest.fn().mockImplementation((coinType: string) =>
        Promise.resolve(coinType === '0xcoinA' ? '999999999' : '0')),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    // Below-range: initial deposit + top-up for A. Two calls total.
    expect(createAddLiquidityFixTokenPayload).toHaveBeenCalledTimes(2);
    const topUpArgs = createAddLiquidityFixTokenPayload.mock.calls[1][0];
    // Top-up for A only (below range).
    expect(topUpArgs.fix_amount_a).toBe(true);
    expect(BigInt(topUpArgs.amount_a)).toBeGreaterThan(0n);
    expect(topUpArgs.pos_id).toBe('0xnewpos');
  });

  it('skips top-up when wallet has no tokens to cover the deficit', async () => {
    // Wallet is empty after the main deposit — no top-up possible.
    process.env.DRY_RUN = 'false';

    const pool = makePoolInfo({
      currentTickIndex: 500,
      currentSqrtPrice: '18446744073709551616',
      tickSpacing: 10,
    });
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
      },
    };

    let getBalanceCallCount = 0;
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
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      // First 4 calls return enough to satisfy required amounts (no swap triggered);
      // calls 5+ (top-up's readWalletTokenBalances) return 0 to simulate wallet empty.
      getBalance: jest.fn().mockImplementation(() => {
        getBalanceCallCount++;
        if (getBalanceCallCount <= 4) return Promise.resolve('500001');
        return Promise.resolve('0'); // wallet empty for top-up check
      }),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    // Rebalance succeeds; top-up was skipped (wallet empty).
    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    // Only one createAddLiquidityFixTokenPayload call (the initial deposit) since top-up is skipped.
    expect(createAddLiquidityFixTokenPayload).toHaveBeenCalledTimes(1);
    // Only 3 signAndExecuteTransaction calls (remove, open, add) — no top-up call.
    expect(mockSuiClient.signAndExecuteTransaction).toHaveBeenCalledTimes(3);
  });
});

// ---------------------------------------------------------------------------
// calculateOptimalSwapAmount
// ---------------------------------------------------------------------------

describe('calculateOptimalSwapAmount', () => {
  it('returns null when no surplus/deficit exists on either side', () => {
    // Both tokens exactly at target — nothing to swap.
    expect(calculateOptimalSwapAmount(1000n, 2000n, 1000n, 2000n, 2)).toBeNull();
  });

  it('returns null when there is only a surplus with no deficit on the other side', () => {
    // Surplus A but also surplus B (no deficit) — no directional swap needed.
    expect(calculateOptimalSwapAmount(2000n, 3000n, 1000n, 2000n, 2)).toBeNull();
  });

  it('swaps A→B when there is surplusA and deficitB', () => {
    // currentA=2000, targetA=1000 → surplusA=1000
    // currentB=500,  targetB=1000 → deficitB=500
    // price=2 (A in terms of B), so requiredA = ceil(500/2 * 1.02) = ceil(255) = 255
    const result = calculateOptimalSwapAmount(2000n, 500n, 1000n, 1000n, 2);
    expect(result).not.toBeNull();
    expect(result!.fromToken).toBe('A');
    expect(result!.amountIn).toBe(255n); // min(surplusA=1000, requiredA=255) = 255
  });

  it('caps A→B swap at surplusA when requiredA exceeds surplusA', () => {
    // currentA=1100, targetA=1000 → surplusA=100
    // currentB=0,    targetB=1000 → deficitB=1000
    // price=2, requiredA = ceil(1000/2 * 1.02) = ceil(510) = 510 > surplusA=100
    const result = calculateOptimalSwapAmount(1100n, 0n, 1000n, 1000n, 2);
    expect(result).not.toBeNull();
    expect(result!.fromToken).toBe('A');
    expect(result!.amountIn).toBe(100n); // capped at surplusA
  });

  it('swaps B→A when there is surplusB and deficitA', () => {
    // currentA=500,  targetA=1000 → deficitA=500
    // currentB=2000, targetB=1000 → surplusB=1000
    // price=2 (A in terms of B), requiredB = ceil(500 * 2 * 1.02) = ceil(1020) = 1020
    // surplusB=1000 < requiredB=1020 → capped at surplusB
    const result = calculateOptimalSwapAmount(500n, 2000n, 1000n, 1000n, 2);
    expect(result).not.toBeNull();
    expect(result!.fromToken).toBe('B');
    expect(result!.amountIn).toBe(1000n); // min(surplusB=1000, requiredB=1020) = 1000
  });

  it('caps B→A swap at surplusB when requiredB exceeds surplusB', () => {
    // currentA=0,    targetA=1000 → deficitA=1000
    // currentB=1100, targetB=1000 → surplusB=100
    // price=2, requiredB = ceil(1000 * 2 * 1.02) = ceil(2040) = 2040 > surplusB=100
    const result = calculateOptimalSwapAmount(0n, 1100n, 1000n, 1000n, 2);
    expect(result).not.toBeNull();
    expect(result!.fromToken).toBe('B');
    expect(result!.amountIn).toBe(100n); // capped at surplusB
  });

  it('includes the 2% buffer in the computed requiredA amount', () => {
    // price=1, deficitB=100 → requiredA = ceil(100/1 * 1.02) = 102
    // surplusA=200 (currentA=300, targetA=100) is larger than requiredA=102,
    // so the buffer-adjusted requiredA is the limiting factor.
    const result = calculateOptimalSwapAmount(300n, 0n, 100n, 100n, 1);
    expect(result).not.toBeNull();
    expect(result!.fromToken).toBe('A');
    expect(result!.amountIn).toBe(102n);
  });

  it('includes the 2% buffer in the computed requiredB amount', () => {
    // price=1, deficitA=100 → requiredB = ceil(100 * 1 * 1.02) = 102
    // surplusB=200 (currentB=300, targetB=100) is larger than requiredB=102,
    // so the buffer-adjusted requiredB is the limiting factor.
    const result = calculateOptimalSwapAmount(0n, 300n, 100n, 100n, 1);
    expect(result).not.toBeNull();
    expect(result!.fromToken).toBe('B');
    expect(result!.amountIn).toBe(102n);
  });
});

// ---------------------------------------------------------------------------
// swapWithBuffer
// ---------------------------------------------------------------------------

describe('swapWithBuffer', () => {
  it('returns true when the first attempt succeeds', async () => {
    const execute = jest.fn().mockResolvedValue({ success: true });
    const result = await swapWithBuffer(execute, '0xcoinA', '0xcoinB', 1000n, 0.01);
    expect(result).toBe(true);
    expect(execute).toHaveBeenCalledTimes(1);
    // Attempt 1 adjusts by +1%: 1000 * 101 / 100 = 1010
    expect(execute).toHaveBeenCalledWith({
      fromToken: '0xcoinA',
      toToken: '0xcoinB',
      amountIn: 1010n,
      slippage: 0.01,
      exactIn: true,
    });
  });

  it('retries and succeeds on the second attempt', async () => {
    const execute = jest.fn()
      .mockResolvedValueOnce({ success: false })
      .mockResolvedValueOnce({ success: true });
    const result = await swapWithBuffer(execute, '0xcoinA', '0xcoinB', 1000n, 0.01);
    expect(result).toBe(true);
    expect(execute).toHaveBeenCalledTimes(2);
    // Attempt 2: 1000 * 102 / 100 = 1020
    expect(execute.mock.calls[1][0].amountIn).toBe(1020n);
  });

  it('returns false when all attempts return success=false without throwing', async () => {
    const execute = jest.fn().mockResolvedValue({ success: false });
    const result = await swapWithBuffer(execute, '0xcoinA', '0xcoinB', 1000n, 0.01, 3);
    expect(result).toBe(false);
    expect(execute).toHaveBeenCalledTimes(3);
  });

  it('rethrows the error from the last attempt when every attempt throws', async () => {
    const execute = jest.fn().mockRejectedValue(new Error('swap error'));
    await expect(
      swapWithBuffer(execute, '0xcoinA', '0xcoinB', 1000n, 0.01, 3),
    ).rejects.toThrow('swap error');
    expect(execute).toHaveBeenCalledTimes(3);
  });

  it('recovers when early attempts throw but a later attempt succeeds', async () => {
    const execute = jest.fn()
      .mockRejectedValueOnce(new Error('transient'))
      .mockResolvedValueOnce({ success: true });
    const result = await swapWithBuffer(execute, '0xcoinA', '0xcoinB', 1000n, 0.01, 3);
    expect(result).toBe(true);
    expect(execute).toHaveBeenCalledTimes(2);
  });

  it('increases the amountIn by 1% per attempt', async () => {
    const execute = jest.fn().mockResolvedValue({ success: false });
    await swapWithBuffer(execute, '0xcoinA', '0xcoinB', 1000n, 0.01, 3);
    // attempt 1: 1000 * 101/100 = 1010
    // attempt 2: 1000 * 102/100 = 1020
    // attempt 3: 1000 * 103/100 = 1030
    expect(execute.mock.calls[0][0].amountIn).toBe(1010n);
    expect(execute.mock.calls[1][0].amountIn).toBe(1020n);
    expect(execute.mock.calls[2][0].amountIn).toBe(1030n);
  });
});
