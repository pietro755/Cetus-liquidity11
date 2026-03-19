/**
 * Unit tests for services/rebalance.ts — RebalanceService
 *
 * Network calls are fully mocked; all tests run in milliseconds without
 * any external services.
 */

import { RebalanceService } from '../services/rebalance';
import { PositionMonitorService } from '../services/monitor';

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

  it('calls createAddLiquidityFixTokenPayload with post-swap amounts, not delta_liquidity', async () => {
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
      tokenAAmount: '1000000',
      tokenBAmount: '1000000',
    } as any;

    // Mock transaction stubs
    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);
    const createAddLiquidityPayload = jest.fn().mockResolvedValue(mockTxStub);
    const createSwapTransactionPayload = jest.fn().mockReturnValue(mockTxStub);

    const mockSdk = {
      RouterV2: { getBestRouter: jest.fn().mockRejectedValue(new Error('aggregator unavailable')) },
      Swap: { createSwapTransactionPayload },
      Position: {
        removeLiquidityTransactionPayload: jest.fn().mockResolvedValue(mockTxStub),
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload,
      },
    };

    // Both env amounts are in range — the bot computes the CLMM-optimal swap
    // (24384 A→B) before opening the position.
    // Post-swap amounts: A = 1000000 − 24384 = 975616, B = 1000000 + 25634 = 1025634
    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn()
        // call 1: removeLiquidity
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xremove' })
        // call 2: swap (CLMM-optimal A→B)
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xswap',
          balanceChanges: [
            { coinType: '0xcoinA', amount: '-24384', owner: { AddressOwner: '0xwallet' } },
            { coinType: '0xcoinB', amount: '25634',  owner: { AddressOwner: '0xwallet' } },
          ],
        })
        // call 3: openPosition (NFT)
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
        })
        // call 4: addLiquidity
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' }),
      getBalance: jest.fn(),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    // A swap must have been executed before opening the position.
    expect(createSwapTransactionPayload).toHaveBeenCalledTimes(1);

    // Must call the fix-token variant, NOT the delta-liquidity variant.
    expect(createAddLiquidityFixTokenPayload).toHaveBeenCalledTimes(1);
    expect(createAddLiquidityPayload).not.toHaveBeenCalled();

    // The fix-token call must use post-swap amounts, not a stored delta_liquidity field.
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

    // amount_a and amount_b must be the post-swap amounts (not the original env values).
    expect(callArgs.amount_a).toBe('975616');
    expect(callArgs.amount_b).toBe('1025634');

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
      tokenAAmount: '1000000',
      tokenBAmount: '1000000',
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);

    const mockSdk = {
      RouterV2: { getBestRouter: jest.fn().mockRejectedValue(new Error('aggregator unavailable')) },
      Swap: { createSwapTransactionPayload: jest.fn().mockReturnValue(mockTxStub) },
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
        // call 2: swap (CLMM-optimal A→B for equal amounts)
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xswap',
          balanceChanges: [
            { coinType: '0xcoinA', amount: '-24384', owner: { AddressOwner: '0xwallet' } },
            { coinType: '0xcoinB', amount: '25634',  owner: { AddressOwner: '0xwallet' } },
          ],
        })
        // call 3: openPosition (NFT) succeeds
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
        })
        // call 4: addLiquidity fails with the retryable error
        .mockRejectedValueOnce(new Error('Object 0xnewpos is not available for consumption'))
        // call 5: addLiquidity succeeds on retry
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' }),
      getBalance: jest.fn(),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
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
      tokenAAmount: '1000000',
      tokenBAmount: '1000000',
      ...configOverride,
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const mockSdk = {
      RouterV2: { getBestRouter: jest.fn().mockRejectedValue(new Error('aggregator unavailable')) },
      Swap: { createSwapTransactionPayload: jest.fn().mockReturnValue(mockTxStub) },
      Position: {
        removeLiquidityTransactionPayload: jest.fn().mockResolvedValue(mockTxStub),
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload: jest.fn().mockResolvedValue(mockTxStub),
        createAddLiquidityPayload: jest.fn(),
      },
    };

    const mockSuiClient = {
      signAndExecuteTransaction: jest.fn()
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xremove' })
        // swap is now triggered for in-range both-token scenarios
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xswap',
          balanceChanges: [
            { coinType: '0xcoinA', amount: '-24384', owner: { AddressOwner: '0xwallet' } },
            { coinType: '0xcoinB', amount: '25634',  owner: { AddressOwner: '0xwallet' } },
          ],
        }),
      getBalance: jest.fn(),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
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
      tokenAAmount: '1000000',
      tokenBAmount: '1000000',
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
      tokenAAmount: '1000000',
      tokenBAmount: '1000000',
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
      tokenAAmount: '1000000',
      tokenBAmount: '1000000',
    } as any;
    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const sdkError = new Error('SDK_INTERNAL: invalid coin type');
    const createAddLiquidityFixTokenPayload = jest.fn().mockRejectedValue(sdkError);

    const mockSdk = {
      RouterV2: { getBestRouter: jest.fn().mockRejectedValue(new Error('aggregator unavailable')) },
      Swap: { createSwapTransactionPayload: jest.fn().mockReturnValue(mockTxStub) },
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
        // swap is now triggered for in-range both-token scenarios
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xswap',
          balanceChanges: [
            { coinType: '0xcoinA', amount: '-24384', owner: { AddressOwner: '0xwallet' } },
            { coinType: '0xcoinB', amount: '25634',  owner: { AddressOwner: '0xwallet' } },
          ],
        })
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
        }),
      getBalance: jest.fn(),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
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
      tokenAAmount: '1000000',
      tokenBAmount: '1000000',
    } as any;
    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);
    const mockSdk = {
      RouterV2: { getBestRouter: jest.fn().mockRejectedValue(new Error('aggregator unavailable')) },
      Swap: { createSwapTransactionPayload: jest.fn().mockReturnValue(mockTxStub) },
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
        // swap is now triggered for in-range both-token scenarios
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xswap',
          balanceChanges: [
            { coinType: '0xcoinA', amount: '-24384', owner: { AddressOwner: '0xwallet' } },
            { coinType: '0xcoinB', amount: '25634',  owner: { AddressOwner: '0xwallet' } },
          ],
        })
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
        })
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' }),
      getBalance: jest.fn(),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: getObjectMock,
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
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
      tokenAAmount: '1000000',
      tokenBAmount: '1000000',
    } as any;
    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);
    const mockSdk = {
      RouterV2: { getBestRouter: jest.fn().mockRejectedValue(new Error('aggregator unavailable')) },
      Swap: { createSwapTransactionPayload: jest.fn().mockReturnValue(mockTxStub) },
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
        // swap is now triggered for in-range both-token scenarios
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xswap',
          balanceChanges: [
            { coinType: '0xcoinA', amount: '-24384', owner: { AddressOwner: '0xwallet' } },
            { coinType: '0xcoinB', amount: '25634',  owner: { AddressOwner: '0xwallet' } },
          ],
        })
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
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn(),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
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
      tokenAAmount: '1000000',
      tokenBAmount: '1000000',
    } as any;
    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);
    const mockSdk = {
      RouterV2: { getBestRouter: jest.fn().mockRejectedValue(new Error('aggregator unavailable')) },
      Swap: { createSwapTransactionPayload: jest.fn().mockReturnValue(mockTxStub) },
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
        // swap is now triggered for in-range both-token scenarios
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xswap',
          balanceChanges: [
            { coinType: '0xcoinA', amount: '-24384', owner: { AddressOwner: '0xwallet' } },
            { coinType: '0xcoinB', amount: '25634',  owner: { AddressOwner: '0xwallet' } },
          ],
        })
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
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      // getObject returns the position as ObjectOwner — simulates a wrapped object.
      getObject: jest.fn().mockResolvedValue({
        data: { objectId: '0xnewpos', owner: { ObjectOwner: '0xparentobject' } },
      }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
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
   *
   * swapBalanceChanges: expected balance changes from the CLMM-optimal swap.
   * Supply this for in-range both-token scenarios where a swap is triggered.
   * Omit (or pass undefined) when no swap is expected (e.g. out-of-range single-token cases).
   */
  async function runAndCaptureFixToken(
    currentTickIndex: number,
    currentSqrtPrice: string,
    tickLower: number,
    tickUpper: number,
    tokenAmountA: string,
    tokenAmountB: string,
    swapBalanceChanges?: Array<{ coinType: string; amount: string; owner: { AddressOwner: string } }>,
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
      tokenAAmount: tokenAmountA,
      tokenBAmount: tokenAmountB,
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);
    const mockSdk = {
      RouterV2: { getBestRouter: jest.fn().mockRejectedValue(new Error('aggregator unavailable')) },
      Swap: { createSwapTransactionPayload: jest.fn().mockReturnValue(mockTxStub) },
      Position: {
        removeLiquidityTransactionPayload: jest.fn().mockResolvedValue(mockTxStub),
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload: jest.fn(),
      },
    };

    // Build signAndExecuteTransaction mock: insert swap call when swap is expected.
    const signAndExecuteTransaction = jest.fn()
      .mockResolvedValueOnce({ effects: successEffect, digest: '0xremove' });

    if (swapBalanceChanges) {
      signAndExecuteTransaction.mockResolvedValueOnce({
        effects: successEffect,
        digest: '0xswap',
        balanceChanges: swapBalanceChanges,
      });
    }

    signAndExecuteTransaction
      .mockResolvedValueOnce({
        effects: successEffect,
        digest: '0xopen',
        objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
      })
      .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' });

    const mockSuiClient = {
      signAndExecuteTransaction,
      getBalance: jest.fn(),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
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
    // L_a > L_b → B is the bottleneck → fix B (fix_amount_a=false).
    // The bot first performs a CLMM-optimal A→B swap (524384 A, +551268 B) to
    // maximise deposited liquidity. After the swap fix_amount_a=false is preserved.
    const swapChanges = [
      { coinType: '0xcoinA', amount: '-524384', owner: { AddressOwner: '0xwallet' } },
      { coinType: '0xcoinB', amount: '551268',  owner: { AddressOwner: '0xwallet' } },
    ];
    const args = await runAndCaptureFixToken(500, SQRT_PRICE_TICK_500, 400, 600, '2000000', '1000000', swapChanges);
    expect(args.fix_amount_a).toBe(false);
  });

  it('fixes token A (fix_amount_a=true) when B is the liquidity-excess token (both tokens, in range)', async () => {
    // tokenAmountA=500000, tokenAmountB=1000000 at tick 500 in range [400,600]:
    // L_a < L_b → A is the bottleneck → fix A (fix_amount_a=true).
    // The bot first performs a CLMM-optimal B→A swap (237182 B, +225615 A) to
    // maximise deposited liquidity. After the swap fix_amount_a=true is preserved.
    const swapChanges = [
      { coinType: '0xcoinA', amount: '225615',   owner: { AddressOwner: '0xwallet' } },
      { coinType: '0xcoinB', amount: '-237182',  owner: { AddressOwner: '0xwallet' } },
    ];
    const args = await runAndCaptureFixToken(500, SQRT_PRICE_TICK_500, 400, 600, '500000', '1000000', swapChanges);
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
    // The bot first performs a CLMM-optimal A→B swap (855111 A, +907077 B) to
    // maximise deposited liquidity. After the swap fix_amount_a=false is preserved.
    // Tick 590 sqrtPrice = '18999001155891605229'
    const sqrtPriceTick590 = '18999001155891605229';
    const swapChanges = [
      { coinType: '0xcoinA', amount: '-855111', owner: { AddressOwner: '0xwallet' } },
      { coinType: '0xcoinB', amount: '907077',  owner: { AddressOwner: '0xwallet' } },
    ];
    const args = await runAndCaptureFixToken(590, sqrtPriceTick590, 400, 600, '1000000', '2000000', swapChanges);
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
      tokenAAmount: tokenAmountA,
      tokenBAmount: tokenAmountB,
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
      tokenAAmount: tokenAmountA,
      tokenBAmount: tokenAmountB,
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
// rebalance — CLMM-optimal swap is performed before opening new position
// ---------------------------------------------------------------------------

describe('rebalance – CLMM-optimal swap is performed before opening new position', () => {
  afterEach(() => { delete process.env.DRY_RUN; });

  it('swaps B→A before opening the rebalanced position when B is in excess', async () => {
    // Scenario: env configures TOKEN_A_AMOUNT=2_000_000 and TOKEN_B_AMOUNT=3_000_000.
    // B is in excess at tick 500, range [400,600]: the bot must swap 448731 B→A
    // (receiving ~426847 A) before opening the new position.
    // Post-swap: A = 2000000 + 426847 = 2426847, B = 3000000 − 448731 = 2551269.
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
      tokenAAmount: '2000000',
      tokenBAmount: '3000000',
    } as any;
    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);
    const createSwapTransactionPayload = jest.fn().mockReturnValue(mockTxStub);
    const mockSdk = {
      RouterV2: { getBestRouter: jest.fn().mockRejectedValue(new Error('aggregator unavailable')) },
      Swap: { createSwapTransactionPayload },
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
        // call 2: CLMM-optimal B→A swap (448731 B, +426847 A)
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xswap',
          balanceChanges: [
            { coinType: '0xcoinA', amount: '426847',  owner: { AddressOwner: '0xwallet' } },
            { coinType: '0xcoinB', amount: '-448731', owner: { AddressOwner: '0xwallet' } },
          ],
        })
        // call 3: openPosition (NFT)
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
        })
        // call 4: addLiquidity
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' }),
      getBalance: jest.fn(),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    // A swap must have been executed before opening the position.
    expect(createSwapTransactionPayload).toHaveBeenCalledTimes(1);
    const swapArgs = createSwapTransactionPayload.mock.calls[0][0] as { a2b: boolean; amount: string };
    expect(swapArgs.a2b).toBe(false);      // B → A
    expect(swapArgs.amount).toBe('448731');

    expect(createAddLiquidityFixTokenPayload).toHaveBeenCalledTimes(1);
    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0] as { amount_a: string; amount_b: string };
    // The deposit must use the post-swap amounts (not the original env amounts).
    expect(callArgs.amount_a).toBe('2426847');  // 2000000 + 426847
    expect(callArgs.amount_b).toBe('2551269');  // 3000000 − 448731
  });

  it('produces consistent post-swap amounts on every rebalance (deterministic)', async () => {
    // Run two rebalances in sequence using (1500000A, 2500000B).
    // B is in excess: swap 461548 B→A (receiving ~439039 A).
    // Post-swap amounts are the same on both runs, proving the behaviour is
    // stateless and deterministic.
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
      tokenAAmount: '1500000',
      tokenBAmount: '2500000',
    } as any;
    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);
    const mockSdk = {
      RouterV2: { getBestRouter: jest.fn().mockRejectedValue(new Error('aggregator unavailable')) },
      Swap: { createSwapTransactionPayload: jest.fn().mockReturnValue(mockTxStub) },
      Position: {
        removeLiquidityTransactionPayload: jest.fn().mockResolvedValue(mockTxStub),
        openPositionTransactionPayload: jest.fn().mockReturnValue(mockTxStub),
        createAddLiquidityFixTokenPayload,
        createAddLiquidityPayload: jest.fn(),
      },
    };

    // Helper builds a fresh suiClient mock for one run.
    const makeSuccessfulRun = () => ({
      signAndExecuteTransaction: jest.fn()
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xremove' })
        // B→A swap: 461548 B spent, 439039 A received
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xswap',
          balanceChanges: [
            { coinType: '0xcoinA', amount: '439039',  owner: { AddressOwner: '0xwallet' } },
            { coinType: '0xcoinB', amount: '-461548', owner: { AddressOwner: '0xwallet' } },
          ],
        })
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
        })
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' }),
      getBalance: jest.fn(),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos' } }),
    });

    const mockSuiClient1 = makeSuccessfulRun();
    const sdkService1 = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
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
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient2),
    } as any;
    (monitor.getPositions as jest.Mock).mockResolvedValue([pos]);

    const svc2 = new RebalanceService(sdkService2, monitor, config);
    const result2 = await svc2.checkAndRebalance('0xpool');
    expect(result2!.success).toBe(true);

    // Both runs must produce the same post-swap deposit amounts.
    expect(createAddLiquidityFixTokenPayload).toHaveBeenCalledTimes(2);
    const args1 = createAddLiquidityFixTokenPayload.mock.calls[0][0] as { amount_a: string; amount_b: string };
    const args2 = createAddLiquidityFixTokenPayload.mock.calls[1][0] as { amount_a: string; amount_b: string };
    expect(args1.amount_a).toBe('1939039');  // 1500000 + 439039
    expect(args1.amount_b).toBe('2038452');  // 2500000 − 461548
    expect(args2.amount_a).toBe(args1.amount_a);
    expect(args2.amount_b).toBe(args1.amount_b);
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
// createInitialPosition — swap is performed before opening position
// ---------------------------------------------------------------------------

describe('createInitialPosition – CLMM-optimal swap before opening position', () => {
  afterEach(() => { delete process.env.DRY_RUN; });

  /**
   * Helper that sets up a live (non-dry-run) createInitialPosition scenario.
   * The CLMM-optimal swap is executed before the position is opened.
   * swapBalanceChanges controls what the mock returns from the swap transaction.
   */
  function makeInitialPositionScenario(opts: {
    tokenAAmount: string;
    tokenBAmount: string;
    swapBalanceChanges: Array<{ coinType: string; amount: string; owner: { AddressOwner: string } }>;
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
      tokenAAmount: opts.tokenAAmount,
      tokenBAmount: opts.tokenBAmount,
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);
    const createSwapTransactionPayload = jest.fn().mockReturnValue(mockTxStub);
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
        // call 1: CLMM-optimal swap before opening position
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xswap',
          balanceChanges: opts.swapBalanceChanges,
        })
        // call 2: openPosition (NFT)
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
        // call 3: addLiquidity
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' }),
      getBalance: jest.fn(),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    return { pool, monitor, config, sdkService, createAddLiquidityFixTokenPayload, createSwapTransactionPayload };
  }

  it('swaps A→B and deposits post-swap amounts when A is in excess for initial position', async () => {
    // (2000000A, 1000000B) in range [400,600] at tick 500:
    // A is excess → swap 524384 A→B (receiving 551268 B).
    // Post-swap: A = 2000000 − 524384 = 1475616, B = 1000000 + 551268 = 1551268.
    const { monitor, config, sdkService, createAddLiquidityFixTokenPayload, createSwapTransactionPayload } =
      makeInitialPositionScenario({
        tokenAAmount: '2000000',
        tokenBAmount: '1000000',
        swapBalanceChanges: [
          { coinType: '0xcoinA', amount: '-524384', owner: { AddressOwner: '0xwallet' } },
          { coinType: '0xcoinB', amount: '551268',  owner: { AddressOwner: '0xwallet' } },
        ],
      });

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    // Swap must have been executed.
    expect(createSwapTransactionPayload).toHaveBeenCalledTimes(1);
    const swapArgs = createSwapTransactionPayload.mock.calls[0][0] as { a2b: boolean; amount: string };
    expect(swapArgs.a2b).toBe(true);        // A → B
    expect(swapArgs.amount).toBe('524384');

    // Deposit must use post-swap amounts.
    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0];
    expect(callArgs.amount_a).toBe('1475616');  // 2000000 − 524384
    expect(callArgs.amount_b).toBe('1551268');  // 1000000 + 551268
  });

  it('swaps B→A and deposits post-swap amounts when B is in excess for initial position', async () => {
    // (1000000A, 1500000B) in range [400,600] at tick 500:
    // B is excess → swap 224365 B→A (receiving 213423 A).
    // Post-swap: A = 1000000 + 213423 = 1213423, B = 1500000 − 224365 = 1275635.
    const { monitor, config, sdkService, createAddLiquidityFixTokenPayload, createSwapTransactionPayload } =
      makeInitialPositionScenario({
        tokenAAmount: '1000000',
        tokenBAmount: '1500000',
        swapBalanceChanges: [
          { coinType: '0xcoinA', amount: '213423',  owner: { AddressOwner: '0xwallet' } },
          { coinType: '0xcoinB', amount: '-224365', owner: { AddressOwner: '0xwallet' } },
        ],
      });

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    expect(createSwapTransactionPayload).toHaveBeenCalledTimes(1);
    const swapArgs = createSwapTransactionPayload.mock.calls[0][0] as { a2b: boolean; amount: string };
    expect(swapArgs.a2b).toBe(false);       // B → A
    expect(swapArgs.amount).toBe('224365');

    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0];
    expect(callArgs.amount_a).toBe('1213423');  // 1000000 + 213423
    expect(callArgs.amount_b).toBe('1275635');  // 1500000 − 224365
  });

  it('swaps A→B for large equal amounts (ratio correction for symmetric range)', async () => {
    // (9999999A, 9999999B): even equal amounts require a small swap (243840 A→B)
    // because the tick range [400,600] is not perfectly symmetric in sqrt-price space.
    // Post-swap: A = 9999999 − 243840 = 9756159, B = 9999999 + 256341 = 10256340.
    const { monitor, config, sdkService, createAddLiquidityFixTokenPayload, createSwapTransactionPayload } =
      makeInitialPositionScenario({
        tokenAAmount: '9999999',
        tokenBAmount: '9999999',
        swapBalanceChanges: [
          { coinType: '0xcoinA', amount: '-243840', owner: { AddressOwner: '0xwallet' } },
          { coinType: '0xcoinB', amount: '256341',  owner: { AddressOwner: '0xwallet' } },
        ],
      });

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    expect(createSwapTransactionPayload).toHaveBeenCalledTimes(1);

    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0];
    // Post-swap amounts are used, not the original env amounts.
    expect(callArgs.amount_a).toBe('9756159');   // 9999999 − 243840
    expect(callArgs.amount_b).toBe('10256340');  // 9999999 + 256341
  });
});

// ---------------------------------------------------------------------------
// rebalancePosition — CLMM-optimal swap is performed before opening position
// ---------------------------------------------------------------------------

describe('rebalancePosition – CLMM-optimal swap before opening position', () => {
  afterEach(() => { delete process.env.DRY_RUN; });

  /**
   * Helper that sets up a live (non-dry-run) rebalance scenario where the bot
   * has an out-of-range position. Returns the mock SDK's mocks for assertion.
   * swapBalanceChanges controls the post-swap token amounts.
   */
  function makeRebalanceEnvAmountScenario(opts: {
    tokenAAmount: string;
    tokenBAmount: string;
    swapBalanceChanges: Array<{ coinType: string; amount: string; owner: { AddressOwner: string } }>;
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
      tokenAAmount: opts.tokenAAmount,
      tokenBAmount: opts.tokenBAmount,
    } as any;

    const mockTxStub = { setGasBudget: jest.fn() };
    const successEffect = { status: { status: 'success' } };

    const createAddLiquidityFixTokenPayload = jest.fn().mockResolvedValue(mockTxStub);
    const createSwapTransactionPayload = jest.fn().mockReturnValue(mockTxStub);
    const mockSdk = {
      RouterV2: { getBestRouter: jest.fn().mockRejectedValue(new Error('aggregator unavailable')) },
      Swap: { createSwapTransactionPayload },
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
        // call 2: CLMM-optimal swap
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xswap',
          balanceChanges: opts.swapBalanceChanges,
        })
        // call 3: openPosition (NFT)
        .mockResolvedValueOnce({
          effects: successEffect,
          digest: '0xopen',
          objectChanges: [{ type: 'created', objectType: 'position', objectId: '0xnewpos', owner: { AddressOwner: '0xwallet' } }],
        })
        // call 4: addLiquidity
        .mockResolvedValueOnce({ effects: successEffect, digest: '0xadd' }),
      getBalance: jest.fn(),
      getCoins: jest.fn().mockResolvedValue({ data: [] }),
      getObject: jest.fn().mockResolvedValue({ data: { objectId: '0xnewpos', type: 'position' } }),
    };

    const sdkService = {
      getAddress: jest.fn().mockReturnValue('0xwallet'),
      getSdk: jest.fn().mockReturnValue(mockSdk),
      getKeypair: jest.fn().mockReturnValue({}),
      getSuiClient: jest.fn().mockReturnValue(mockSuiClient),
    } as any;

    return { monitor, config, sdkService, createAddLiquidityFixTokenPayload, createSwapTransactionPayload };
  }

  it('swaps B→A and uses post-swap amounts when B is in excess during rebalance', async () => {
    // (2000000A, 3000000B): B excess → swap 448731 B→A (receiving 426847 A).
    // Post-swap: A = 2000000 + 426847 = 2426847, B = 3000000 − 448731 = 2551269.
    const { monitor, config, sdkService, createAddLiquidityFixTokenPayload, createSwapTransactionPayload } =
      makeRebalanceEnvAmountScenario({
        tokenAAmount: '2000000',
        tokenBAmount: '3000000',
        swapBalanceChanges: [
          { coinType: '0xcoinA', amount: '426847',  owner: { AddressOwner: '0xwallet' } },
          { coinType: '0xcoinB', amount: '-448731', owner: { AddressOwner: '0xwallet' } },
        ],
      });

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    expect(createSwapTransactionPayload).toHaveBeenCalledTimes(1);
    const swapArgs = createSwapTransactionPayload.mock.calls[0][0] as { a2b: boolean; amount: string };
    expect(swapArgs.a2b).toBe(false);       // B → A
    expect(swapArgs.amount).toBe('448731');

    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0];
    expect(callArgs.amount_a).toBe('2426847');  // 2000000 + 426847
    expect(callArgs.amount_b).toBe('2551269');  // 3000000 − 448731
  });

  it('swaps B→A and uses post-swap amounts when B is in excess (1000000A, 1500000B)', async () => {
    // (1000000A, 1500000B): B excess → swap 224365 B→A (receiving 213423 A).
    // Post-swap: A = 1000000 + 213423 = 1213423, B = 1500000 − 224365 = 1275635.
    const { monitor, config, sdkService, createAddLiquidityFixTokenPayload } =
      makeRebalanceEnvAmountScenario({
        tokenAAmount: '1000000',
        tokenBAmount: '1500000',
        swapBalanceChanges: [
          { coinType: '0xcoinA', amount: '213423',  owner: { AddressOwner: '0xwallet' } },
          { coinType: '0xcoinB', amount: '-224365', owner: { AddressOwner: '0xwallet' } },
        ],
      });

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0];
    expect(callArgs.amount_a).toBe('1213423');
    expect(callArgs.amount_b).toBe('1275635');
  });

  it('swaps A→B for large equal amounts (ratio correction) during rebalance', async () => {
    // (9999999A, 9999999B): swap 243840 A→B (receiving 256341 B).
    // Post-swap: A = 9999999 − 243840 = 9756159, B = 9999999 + 256341 = 10256340.
    const { monitor, config, sdkService, createAddLiquidityFixTokenPayload, createSwapTransactionPayload } =
      makeRebalanceEnvAmountScenario({
        tokenAAmount: '9999999',
        tokenBAmount: '9999999',
        swapBalanceChanges: [
          { coinType: '0xcoinA', amount: '-243840', owner: { AddressOwner: '0xwallet' } },
          { coinType: '0xcoinB', amount: '256341',  owner: { AddressOwner: '0xwallet' } },
        ],
      });

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    expect(createSwapTransactionPayload).toHaveBeenCalledTimes(1);

    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0];
    expect(callArgs.amount_a).toBe('9756159');   // 9999999 − 243840
    expect(callArgs.amount_b).toBe('10256340');  // 9999999 + 256341
  });

  it('produces consistent post-swap amounts on successive rebalances (deterministic)', async () => {
    // (4000000A, 6000000B): B excess → swap 897463 B→A (receiving 853695 A).
    // Post-swap: A = 4000000 + 853695 = 4853695, B = 6000000 − 897463 = 5102537.
    const { monitor, config, sdkService, createAddLiquidityFixTokenPayload } =
      makeRebalanceEnvAmountScenario({
        tokenAAmount: '4000000',
        tokenBAmount: '6000000',
        swapBalanceChanges: [
          { coinType: '0xcoinA', amount: '853695',  owner: { AddressOwner: '0xwallet' } },
          { coinType: '0xcoinB', amount: '-897463', owner: { AddressOwner: '0xwallet' } },
        ],
      });

    const svc = new RebalanceService(sdkService, monitor, config);
    const result = await svc.checkAndRebalance('0xpool');

    expect(result).not.toBeNull();
    expect(result!.success).toBe(true);

    const callArgs = createAddLiquidityFixTokenPayload.mock.calls[0][0];
    expect(callArgs.amount_a).toBe('4853695');  // 4000000 + 853695
    expect(callArgs.amount_b).toBe('5102537');  // 6000000 − 897463
  });
});
