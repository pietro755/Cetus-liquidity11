import { CetusSDKService } from './sdk';
import { PositionMonitorService, PoolInfo, PositionInfo } from './monitor';
import { BotConfig } from '../config';
import { logger } from '../utils/logger';
import BN from 'bn.js';
import { TransactionUtil } from '@cetusprotocol/cetus-sui-clmm-sdk';
import type { AddLiquidityFixTokenParams, CoinAsset, SwapParams } from '@cetusprotocol/cetus-sui-clmm-sdk';
import type { BalanceChange } from '@mysten/sui/client';

/** The canonical Sui coin type used to identify the native SUI token. */
const SUI_COIN_TYPE = '0x2::sui::SUI';

export interface RebalanceResult {
  success: boolean;
  transactionDigest?: string;
  error?: string;
  oldPosition?: { tickLower: number; tickUpper: number };
  newPosition?: { tickLower: number; tickUpper: number };
}

// SDK parameter type for remove liquidity (avoids casting to `any` where possible)
interface RemoveLiquidityParams {
  pool_id: string;
  pos_id: string;
  delta_liquidity: string;
  min_amount_a: string;
  min_amount_b: string;
  coinTypeA: string;
  coinTypeB: string;
  collect_fee: boolean;
  rewarder_coin_types: string[];
}

export class RebalanceService {
  private sdkService: CetusSDKService;
  private monitorService: PositionMonitorService;
  private config: BotConfig;
  private dryRun: boolean;

  constructor(
    sdkService: CetusSDKService,
    monitorService: PositionMonitorService,
    config: BotConfig,
  ) {
    this.sdkService = sdkService;
    this.monitorService = monitorService;
    this.config = config;
    this.dryRun = process.env.DRY_RUN === 'true';

    if (this.dryRun) {
      logger.warn('⚠️  DRY RUN MODE — no transactions will be executed');
    }
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /**
   * Main entry point called every loop iteration.
   *
   * Step 4: fetch existing position by liquidity (highest liquidity position).
   * Step 5: check if position tick is inside [tickLower, tickUpper].
   * Step 6: if out of range →
   *           remove liquidity
   *           check token balances
   *           swap using Cetus aggregator if needed
   *           open new position with same liquidity
   *
   * Returns null when position is in range (no action needed).
   */
  async checkAndRebalance(poolAddress: string): Promise<RebalanceResult | null> {
    try {
      const poolInfo = await this.monitorService.getPoolInfo(poolAddress);
      const ownerAddress = this.sdkService.getAddress();
      const allPositions = await this.monitorService.getPositions(ownerAddress);

      // Step 4: fetch existing position by liquidity — pick the one with most liquidity.
      const poolPositions = allPositions.filter(p => {
        if (p.poolAddress !== poolAddress) return false;
        try {
          return p.liquidity && BigInt(p.liquidity) > 0n;
        } catch {
          return false;
        }
      });

      if (poolPositions.length === 0) {
        logger.info('No positions with liquidity found in pool — creating initial position');
        return this.createInitialPosition(poolInfo);
      }

      // Highest liquidity position
      const position = [...poolPositions].sort((a, b) => {
        const lA = BigInt(a.liquidity || '0');
        const lB = BigInt(b.liquidity || '0');
        return lA > lB ? -1 : lA < lB ? 1 : 0;
      })[0];

      logger.info('Current position', {
        positionId: position.positionId,
        tickRange: `[${position.tickLower}, ${position.tickUpper}]`,
        currentTick: poolInfo.currentTickIndex,
        liquidity: position.liquidity,
      });

      // Step 5: check if position tick is inside LOWER_TICK and UPPER_TICK.
      const inRange = this.monitorService.isPositionInRange(
        position.tickLower,
        position.tickUpper,
        poolInfo.currentTickIndex,
      );

      if (inRange) {
        logger.info('Position is in range — no action needed');
        return null;
      }

      // Step 6: out of range → rebalance.
      logger.info('Position is OUT OF RANGE — starting rebalance', {
        positionId: position.positionId,
        tickRange: `[${position.tickLower}, ${position.tickUpper}]`,
        currentTick: poolInfo.currentTickIndex,
      });

      return this.rebalancePosition(position, poolInfo);
    } catch (error) {
      logger.error('checkAndRebalance failed', error);
      throw error;
    }
  }

  // ---------------------------------------------------------------------------
  // Private: create initial position (no existing position to close)
  // ---------------------------------------------------------------------------

  /**
   * Called when no position with liquidity is found in the pool.
   * Determines a tick range, reads wallet balances, swaps if needed,
   * and opens a brand-new position.
   */
  private async createInitialPosition(poolInfo: PoolInfo): Promise<RebalanceResult> {
    try {
      // Determine tick range: use explicit env vars if set, otherwise default to
      // ±10 tick spacings centred on the current tick (aligned to tickSpacing).
      let lower: number;
      let upper: number;

      if (this.config.lowerTick !== undefined && this.config.upperTick !== undefined) {
        lower = this.config.lowerTick;
        upper = this.config.upperTick;
        logger.info('Using env-configured tick range for initial position', { lower, upper });
      } else {
        const tickSpacing = poolInfo.tickSpacing;
        const halfWidth = 10 * tickSpacing;
        lower = Math.floor((poolInfo.currentTickIndex - halfWidth) / tickSpacing) * tickSpacing;
        upper = Math.floor((poolInfo.currentTickIndex + halfWidth) / tickSpacing) * tickSpacing;
        logger.info('Using default tick range (±10 tick spacings) for initial position', {
          lower,
          upper,
          currentTick: poolInfo.currentTickIndex,
          tickSpacing,
        });
      }

      if (this.dryRun) {
        logger.info('[DRY RUN] Would create initial position', {
          tickRange: { lower, upper },
        });
        return {
          success: true,
          newPosition: { tickLower: lower, tickUpper: upper },
        };
      }

      // Read wallet balances.
      const balances = await this.getWalletBalances(poolInfo.coinTypeA, poolInfo.coinTypeB);
      logger.info('Wallet balances for initial position', {
        amountA: balances.amountA,
        amountB: balances.amountB,
        coinTypeA: poolInfo.coinTypeA,
        coinTypeB: poolInfo.coinTypeB,
      });

      // Swap to correct token ratio if needed.
      const adjusted = await this.swapTokensIfNeeded(
        balances.amountA,
        balances.amountB,
        poolInfo,
        lower,
        upper,
      );

      // Open the new position.
      const result = await this.openNewPosition(
        poolInfo,
        lower,
        upper,
        adjusted.amountA,
        adjusted.amountB,
      );

      logger.info('Initial position created', {
        newRange: { lower, upper },
        transactionDigest: result.transactionDigest,
      });

      return {
        success: true,
        transactionDigest: result.transactionDigest,
        newPosition: { tickLower: lower, tickUpper: upper },
      };
    } catch (error) {
      logger.error('createInitialPosition failed', error);
      return {
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error',
      };
    }
  }

  // ---------------------------------------------------------------------------
  // Private: rebalance flow
  // ---------------------------------------------------------------------------

  private async rebalancePosition(
    position: PositionInfo,
    poolInfo: PoolInfo,
  ): Promise<RebalanceResult> {
    try {
      // Determine new tick range.
      // Priority: explicit env vars → preserve old range width centred on current tick.
      let lower: number;
      let upper: number;

      if (this.config.lowerTick !== undefined && this.config.upperTick !== undefined) {
        lower = this.config.lowerTick;
        upper = this.config.upperTick;
        logger.info('Using env-configured tick range', { lower, upper });
      } else {
        // Preserve the width of the old position, centred on the current tick.
        // Both bounds are independently aligned to tickSpacing to ensure the
        // Cetus SDK accepts them as valid tick indices.
        const rangeWidth = position.tickUpper - position.tickLower;
        const tickSpacing = poolInfo.tickSpacing;
        const half = Math.floor(rangeWidth / 2);
        lower = Math.floor((poolInfo.currentTickIndex - half) / tickSpacing) * tickSpacing;
        upper = Math.ceil((poolInfo.currentTickIndex + half) / tickSpacing) * tickSpacing;
        logger.info('Calculated new tick range (preserving width)', { lower, upper, rangeWidth });
      }

      if (this.dryRun) {
        logger.info('[DRY RUN] Would rebalance position', {
          oldRange: { lower: position.tickLower, upper: position.tickUpper },
          newRange: { lower, upper },
          liquidity: position.liquidity,
        });
        return {
          success: true,
          oldPosition: { tickLower: position.tickLower, tickUpper: position.tickUpper },
          newPosition: { tickLower: lower, tickUpper: upper },
        };
      }

      // Store the exact liquidity value from the closing position so it can be
      // reused verbatim when opening the new position.
      const storedLiquidity = position.liquidity;
      if (!storedLiquidity || BigInt(storedLiquidity) === 0n) {
        throw new Error('Position liquidity is missing or zero — cannot proceed with rebalance');
      }

      // Step 1: Remove liquidity from the out-of-range position.
      await this.removeLiquidity(position.positionId, storedLiquidity, poolInfo);

      // Step 2: Check actual token balances in wallet.
      const balances = await this.getWalletBalances(poolInfo.coinTypeA, poolInfo.coinTypeB);
      logger.info('Wallet balances after removing liquidity', {
        amountA: balances.amountA,
        amountB: balances.amountB,
        coinTypeA: poolInfo.coinTypeA,
        coinTypeB: poolInfo.coinTypeB,
      });

      // Step 3: Swap using Cetus aggregator if needed to reach the correct token ratio.
      const adjusted = await this.swapTokensIfNeeded(
        balances.amountA,
        balances.amountB,
        poolInfo,
        lower,
        upper,
      );

      // Step 4: Open new position, reusing the exact stored liquidity value.
      const result = await this.openNewPosition(
        poolInfo,
        lower,
        upper,
        adjusted.amountA,
        adjusted.amountB,
        storedLiquidity,
      );

      logger.info('Rebalance completed', {
        oldRange: { lower: position.tickLower, upper: position.tickUpper },
        newRange: { lower, upper },
        transactionDigest: result.transactionDigest,
      });

      return {
        success: true,
        transactionDigest: result.transactionDigest,
        oldPosition: { tickLower: position.tickLower, tickUpper: position.tickUpper },
        newPosition: { tickLower: lower, tickUpper: upper },
      };
    } catch (error) {
      logger.error('Rebalance failed', error);
      return {
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error',
      };
    }
  }

  // ---------------------------------------------------------------------------
  // Private: remove liquidity
  // ---------------------------------------------------------------------------

  private async removeLiquidity(
    positionId: string,
    liquidity: string,
    poolInfo: PoolInfo,
  ): Promise<void> {
    logger.info('Removing liquidity', { positionId, liquidity });

    const sdk = this.sdkService.getSdk();
    const keypair = this.sdkService.getKeypair();
    const suiClient = this.sdkService.getSuiClient();
    const ownerAddress = this.sdkService.getAddress();

    await this.retryTransaction(
      async () => {
        // Re-fetch position to get fresh coin types on each retry.
        const positions = await this.monitorService.getPositions(ownerAddress);
        const pos = positions.find(p => p.positionId === positionId);
        if (!pos) throw new Error(`Position ${positionId} not found`);

        const params: RemoveLiquidityParams = {
          pool_id: pos.poolAddress,
          pos_id: positionId,
          delta_liquidity: liquidity,
          min_amount_a: '0',
          min_amount_b: '0',
          coinTypeA: pos.tokenA,
          coinTypeB: pos.tokenB,
          collect_fee: true,
          rewarder_coin_types: [],
        };

        const payload = await sdk.Position.removeLiquidityTransactionPayload(params as any);
        payload.setGasBudget(this.config.gasBudget);

        const txResult = await suiClient.signAndExecuteTransaction({
          transaction: payload,
          signer: keypair,
          options: { showEffects: true },
        });

        if (txResult.effects?.status?.status !== 'success') {
          throw new Error(`Transaction failed: ${txResult.effects?.status?.error || 'Unknown'}`);
        }

        logger.info('Remove liquidity transaction succeeded', { digest: txResult.digest });
      },
      'remove liquidity',
      3,
      2000,
    );
  }

  // ---------------------------------------------------------------------------
  // Private: check wallet balances
  // ---------------------------------------------------------------------------

  /**
   * Read the current wallet balances for both pool tokens.
   * Called after removing liquidity to determine what tokens are available
   * before deciding whether and how much to swap.
   *
   * When SUI (`0x2::sui::SUI`) is one of the pool tokens, the gas budget is
   * subtracted from its balance so that subsequent transactions always have
   * enough SUI available to pay fees.  Without this reservation the add-
   * liquidity transaction fails with `InsufficientCoinBalance` because the
   * full SUI balance is passed as a token amount, leaving nothing for gas.
   */
  private async getWalletBalances(
    coinTypeA: string,
    coinTypeB: string,
  ): Promise<{ amountA: string; amountB: string }> {
    const suiClient = this.sdkService.getSuiClient();
    const ownerAddress = this.sdkService.getAddress();

    const [balA, balB] = await Promise.all([
      suiClient.getBalance({ owner: ownerAddress, coinType: coinTypeA }),
      suiClient.getBalance({ owner: ownerAddress, coinType: coinTypeB }),
    ]);

    const gasBudget = BigInt(this.config.gasBudget);
    const isSuiA = this.normalizeCoinType(coinTypeA) === this.normalizeCoinType(SUI_COIN_TYPE);
    const isSuiB = this.normalizeCoinType(coinTypeB) === this.normalizeCoinType(SUI_COIN_TYPE);

    let amountA = BigInt(balA.totalBalance || '0');
    let amountB = BigInt(balB.totalBalance || '0');

    if (isSuiA) {
      amountA = amountA > gasBudget ? amountA - gasBudget : 0n;
    }
    if (isSuiB) {
      amountB = amountB > gasBudget ? amountB - gasBudget : 0n;
    }

    return {
      amountA: amountA.toString(),
      amountB: amountB.toString(),
    };
  }

  // ---------------------------------------------------------------------------
  // Private: swap tokens to correct ratio (via Cetus aggregator)
  // ---------------------------------------------------------------------------

  /**
   * Swap tokens to the correct ratio before opening the new position.
   *
   * Uses the Cetus aggregator (RouterV2.getBestRouter) for optimal routing,
   * with a fallback to the direct pool swap if the aggregator is unavailable.
   *
   * After removing an out-of-range position we typically have only one token:
   *   • new range below current price → need only A → swap all B→A
   *   • new range above current price → need only B → swap all A→B
   *   • new range in range with one token → swap half to get both
   *   • both tokens present → no swap needed
   */
  private async swapTokensIfNeeded(
    amountA: string,
    amountB: string,
    poolInfo: PoolInfo,
    tickLower: number,
    tickUpper: number,
  ): Promise<{ amountA: string; amountB: string }> {
    const bigA = BigInt(amountA || '0');
    const bigB = BigInt(amountB || '0');

    if (bigA === 0n && bigB === 0n) return { amountA, amountB };

    if (bigA > 0n && bigB > 0n) {
      logger.info('Both tokens available — no swap needed');
      return { amountA, amountB };
    }

    const currentTick = poolInfo.currentTickIndex;
    const priceIsBelowRange = currentTick < tickLower;
    const priceIsAboveRange = currentTick >= tickUpper;

    let a2b = false;
    let swapAmount = 0n;

    if (priceIsBelowRange) {
      if (bigA === 0n && bigB > 0n) {
        a2b = false; swapAmount = bigB; // swap all B→A
      } else {
        return { amountA, amountB }; // already have A
      }
    } else if (priceIsAboveRange) {
      if (bigB === 0n && bigA > 0n) {
        a2b = true; swapAmount = bigA; // swap all A→B
      } else {
        return { amountA, amountB }; // already have B
      }
    } else {
      // In-range position needs both tokens — swap half of whichever we have.
      if (bigA > 0n) { a2b = true; swapAmount = bigA / 2n; }
      else { a2b = false; swapAmount = bigB / 2n; }
      if (swapAmount === 0n) return { amountA, amountB };
    }

    logger.info('Swapping tokens to correct ratio via Cetus aggregator', {
      direction: a2b ? 'A→B' : 'B→A',
      swapAmount: swapAmount.toString(),
    });

    if (this.dryRun) {
      logger.info('[DRY RUN] Would swap tokens via aggregator');
      return { amountA, amountB };
    }

    const sdk = this.sdkService.getSdk();
    const keypair = this.sdkService.getKeypair();
    const suiClient = this.sdkService.getSuiClient();
    const ownerAddress = this.sdkService.getAddress();

    const fromCoin = a2b ? poolInfo.coinTypeA : poolInfo.coinTypeB;
    const toCoin   = a2b ? poolInfo.coinTypeB : poolInfo.coinTypeA;

    // Fetch coin objects for the from-coin (needed by the aggregator transaction builder)
    const fromCoinsResponse = await suiClient.getCoins({ owner: ownerAddress, coinType: fromCoin });
    const allCoinAsset: CoinAsset[] = fromCoinsResponse.data.map(c => ({
      coinAddress: c.coinType,
      coinObjectId: c.coinObjectId,
      balance: BigInt(c.balance),
    }));

    // Try the Cetus aggregator first for optimal routing.
    let swapTx;

    try {
      const { result } = await sdk.RouterV2.getBestRouter(
        fromCoin,
        toCoin,
        Number(swapAmount),   // SDK accepts number; precision loss is acceptable for routing
        true,                 // byAmountIn
        0,                    // no split
        '',                   // no partner
      );

      if (!result.isExceed && !result.isTimeout && result.splitPaths.length > 0) {
        swapTx = await TransactionUtil.buildAggregatorSwapTransaction(
          sdk,
          result,
          allCoinAsset,
          '',                          // no partner
          this.config.maxSlippage,
        );
        logger.info('Using aggregator route', {
          inputAmount: result.inputAmount,
          outputAmount: result.outputAmount,
          paths: result.splitPaths.length,
        });
      } else {
        logger.warn('Aggregator returned no usable route — falling back to direct pool swap', {
          isExceed: result.isExceed,
          isTimeout: result.isTimeout,
        });
      }
    } catch (aggErr) {
      const msg = aggErr instanceof Error ? aggErr.message : String(aggErr);
      logger.warn(`Aggregator request failed (${msg}) — falling back to direct pool swap`);
    }

    // Fallback: direct single-pool swap via sdk.Swap
    if (!swapTx) {
      logger.info('Using direct pool swap as fallback');

      const sqrtPriceBig = BigInt(poolInfo.currentSqrtPrice);
      const TWO_128 = 2n ** 128n;
      const priceX128 = sqrtPriceBig * sqrtPriceBig;
      const slippageFactor = BigInt(Math.floor((1 - this.config.maxSlippage) * 10000));

      let minOutput: bigint;
      if (a2b) {
        const rawOut = priceX128 > 0n ? swapAmount * priceX128 / TWO_128 : 0n;
        minOutput = rawOut * slippageFactor / 10000n;
      } else {
        const rawOut = priceX128 > 0n ? swapAmount * TWO_128 / priceX128 : 0n;
        minOutput = rawOut * slippageFactor / 10000n;
      }

      const swapParams: SwapParams = {
        pool_id: poolInfo.poolAddress,
        a2b,
        by_amount_in: true,
        amount: swapAmount.toString(),
        amount_limit: minOutput.toString(),
        coinTypeA: poolInfo.coinTypeA,
        coinTypeB: poolInfo.coinTypeB,
      };
      swapTx = await sdk.Swap.createSwapTransactionPayload(swapParams);
    }

    swapTx.setGasBudget(this.config.gasBudget);

    // Read wallet balances before the swap for post-swap delta calculation.
    const [preBalFrom, preBalTo] = await Promise.all([
      suiClient.getBalance({ owner: ownerAddress, coinType: fromCoin }),
      suiClient.getBalance({ owner: ownerAddress, coinType: toCoin }),
    ]);

    const swapResult = await suiClient.signAndExecuteTransaction({
      transaction: swapTx,
      signer: keypair,
      options: { showEffects: true, showBalanceChanges: true },
    });

    if (swapResult.effects?.status?.status !== 'success') {
      throw new Error(`Swap failed: ${swapResult.effects?.status?.error || 'Unknown'}`);
    }

    // Parse balance changes to compute post-swap amounts.
    const normalizedTypeA = this.normalizeCoinType(poolInfo.coinTypeA);
    const normalizedTypeB = this.normalizeCoinType(poolInfo.coinTypeB);
    const normalizedSuiType = this.normalizeCoinType('0x2::sui::SUI');

    const gasUsed = swapResult.effects?.gasUsed;
    const totalGasCost = gasUsed
      ? BigInt(gasUsed.computationCost) + BigInt(gasUsed.storageCost) - BigInt(gasUsed.storageRebate)
      : 0n;

    let deltaA = 0n;
    let deltaB = 0n;

    const balanceChanges: BalanceChange[] | null | undefined = swapResult.balanceChanges;
    if (balanceChanges) {
      for (const change of balanceChanges) {
        const owner = change.owner;
        if (typeof owner !== 'object' || !('AddressOwner' in owner)) continue;
        if ((owner as { AddressOwner: string }).AddressOwner.toLowerCase() !== ownerAddress.toLowerCase()) continue;
        let amt = BigInt(change.amount);
        const normalized = this.normalizeCoinType(change.coinType);
        // Recover gross SUI amount by adding back gas cost.
        if (amt < 0n && totalGasCost > 0n && normalized === normalizedSuiType) {
          const gross = amt + totalGasCost;
          amt = gross > 0n ? gross : 0n;
        }
        if (normalized === normalizedTypeA) deltaA += amt;
        else if (normalized === normalizedTypeB) deltaB += amt;
      }
    }

    // Fallback: compare pre/post wallet balances if balance-change parsing gives zero.
    if (deltaA === 0n && deltaB === 0n) {
      logger.warn('Balance-change parsing yielded zero delta — using pre/post balance fallback');
      const [postBalFrom, postBalTo] = await Promise.all([
        suiClient.getBalance({ owner: ownerAddress, coinType: fromCoin }),
        suiClient.getBalance({ owner: ownerAddress, coinType: toCoin }),
      ]);
      const dFrom = BigInt(postBalFrom.totalBalance) - BigInt(preBalFrom.totalBalance);
      const dTo   = BigInt(postBalTo.totalBalance)   - BigInt(preBalTo.totalBalance);
      if (a2b) { deltaA += dFrom; deltaB += dTo; }
      else     { deltaB += dFrom; deltaA += dTo; }
    }

    const rawNewA = bigA + deltaA;
    const rawNewB = bigB + deltaB;
    const newAmountA = (rawNewA < 0n ? 0n : rawNewA).toString();
    const newAmountB = (rawNewB < 0n ? 0n : rawNewB).toString();

    logger.info('Swap completed', { digest: swapResult.digest, newAmountA, newAmountB });
    return { amountA: newAmountA, amountB: newAmountB };
  }

  // ---------------------------------------------------------------------------
  // Private: open new position (explicit two-step — no zap-in)
  // ---------------------------------------------------------------------------

  /**
   * Open a new position using an explicit two-step approach:
   *   1. openPositionTransactionPayload  → creates the position NFT
   *   2. createAddLiquidityPayload       → deposits tokens using the stored delta_liquidity
   *
   * The delta_liquidity is the exact value stored from the closed position — it is
   * never recomputed from token amounts.  The wallet balances (amountA / amountB)
   * act as max_amount_a / max_amount_b, ensuring the bot never pulls extra funds.
   */
  private async openNewPosition(
    poolInfo: PoolInfo,
    tickLower: number,
    tickUpper: number,
    amountA: string,
    amountB: string,
    storedLiquidity?: string,
  ): Promise<{ transactionDigest?: string }> {
    // Validate that amountA and amountB are valid non-negative integer strings.
    const isValidAmountString = (v: string) => /^\d+$/.test(v);
    if (!isValidAmountString(amountA)) {
      throw new Error(`Invalid amountA: "${amountA}" is not a valid non-negative integer string`);
    }
    if (!isValidAmountString(amountB)) {
      throw new Error(`Invalid amountB: "${amountB}" is not a valid non-negative integer string`);
    }

    if (BigInt(amountA) === 0n && BigInt(amountB) === 0n) {
      throw new Error('No token amounts available to open new position');
    }

    // Only validate stored liquidity when it is explicitly provided (rebalance path).
    if (storedLiquidity !== undefined && (!storedLiquidity || BigInt(storedLiquidity) === 0n)) {
      throw new Error('Stored liquidity value is zero — cannot open new position');
    }

    // Validate that tickLower and tickUpper are valid integers.
    if (!Number.isInteger(tickLower)) {
      throw new Error(`Invalid tickLower: ${tickLower} is not a valid integer`);
    }
    if (!Number.isInteger(tickUpper)) {
      throw new Error(`Invalid tickUpper: ${tickUpper} is not a valid integer`);
    }

    const sdk = this.sdkService.getSdk();
    const keypair = this.sdkService.getKeypair();
    const suiClient = this.sdkService.getSuiClient();

    logger.info('Opening new position — step 1: create position NFT', {
      tickLower,
      tickUpper,
      amountA,
      amountB,
    });

    // -----------------------------------------------------------------------
    // Step 1: Open the position (creates the NFT, no tokens deposited yet).
    // -----------------------------------------------------------------------
    const openTx = sdk.Position.openPositionTransactionPayload({
      pool_id: poolInfo.poolAddress,
      tick_lower: String(tickLower),
      tick_upper: String(tickUpper),
      coinTypeA: poolInfo.coinTypeA,
      coinTypeB: poolInfo.coinTypeB,
    });
    openTx.setGasBudget(this.config.gasBudget);

    const openResult = await suiClient.signAndExecuteTransaction({
      transaction: openTx,
      signer: keypair,
      options: { showEffects: true, showObjectChanges: true },
    });

    if (openResult.effects?.status?.status !== 'success') {
      throw new Error(`Open position failed: ${openResult.effects?.status?.error || 'Unknown'}`);
    }

    // Extract the new position ID from the object changes.
    // We must only select objects that are directly owned by the wallet
    // (AddressOwner).  Child objects owned by other objects cannot be used
    // as transaction input arguments and will cause an "Objects owned by
    // other objects cannot be used as input arguments" error at runtime.
    const positionChange = openResult.objectChanges?.find(
      (c): c is Extract<typeof c, { type: 'created' }> =>
        c.type === 'created' &&
        typeof c.objectType === 'string' &&
        c.objectType.toLowerCase().includes('position') &&
        typeof c.owner === 'object' &&
        c.owner !== null &&
        'AddressOwner' in (c.owner as object),
    );

    if (!positionChange?.objectId) {
      throw new Error('Could not find new position ID in transaction object changes');
    }

    const newPositionId = positionChange.objectId;
    logger.info('Position NFT created', { positionId: newPositionId, digest: openResult.digest });

    // -----------------------------------------------------------------------
    // Step 1.5: Verify the position object is accessible on the network.
    //   There can be a race condition where the freshly-created NFT hasn't
    //   yet propagated through the Sui full-node.  Poll getObject until the
    //   object is visible before handing the position ID to the SDK.
    // -----------------------------------------------------------------------
    await this.waitForPositionObject(suiClient, newPositionId);

    // -----------------------------------------------------------------------
    // Step 2: Add liquidity using the actual wallet token amounts.
    //   createAddLiquidityFixTokenPayload derives the correct delta_liquidity
    //   from the available token amounts for the new tick range and current
    //   price, ensuring the wallet is never overdrawn regardless of what the
    //   old position's stored liquidity value was.
    // -----------------------------------------------------------------------
    const bigAmtA = BigInt(amountA || '0');
    const bigAmtB = BigInt(amountB || '0');

    // Determine which token to fix for the add-liquidity call.
    //
    // The SDK's createAddLiquidityFixTokenPayload uses the fixed token's amount
    // to derive delta_liquidity, then computes how much of the other token is
    // required.  If the required amount of the non-fixed token exceeds what is
    // available in the wallet, the transaction fails.
    //
    // Correct strategy:
    //   • Price below range  (currentTick < tickLower)  → only token A accepted;
    //                                                      always fix A.
    //   • Price above range  (currentTick >= tickUpper) → only token B accepted;
    //                                                      always fix B.
    //   • Price in range                                → fix whichever token has
    //                                                      the lower current value
    //                                                      so the computed amount
    //                                                      of the other token never
    //                                                      exceeds our balance.
    //
    // Value comparison (in-range):
    //   value_A_in_B = amountA × (sqrtPrice / 2^64)² = amountA × sqrtPrice² / 2^128
    //   fix A when value_A ≤ value_B  ↔  amountA × sqrtPrice² ≤ amountB × 2^128
    let fix_amount_a: boolean;
    const currentTick = poolInfo.currentTickIndex;
    if (bigAmtA === 0n) {
      fix_amount_a = false; // Only B available
    } else if (bigAmtB === 0n) {
      fix_amount_a = true;  // Only A available
    } else if (currentTick < tickLower) {
      // Price is below range — the position only accepts token A.
      fix_amount_a = true;
    } else if (currentTick >= tickUpper) {
      // Price is above range — the position only accepts token B.
      fix_amount_a = false;
    } else {
      // Price is in range — fix the lower-value token so the SDK's computed
      // amount of the other token stays within our wallet balance.
      const sqrtPriceBig = BigInt(poolInfo.currentSqrtPrice);
      const TWO_128 = 2n ** 128n;
      fix_amount_a = bigAmtA * sqrtPriceBig * sqrtPriceBig <= bigAmtB * TWO_128;
    }

    logger.info('Opening new position — step 2: deposit tokens', {
      positionId: newPositionId,
      amountA,
      amountB,
      fix_amount_a,
    });

    let addDigest: string | undefined;

    await this.retryTransaction(
      async () => {
        const addLiquidityParams: AddLiquidityFixTokenParams = {
          pool_id: poolInfo.poolAddress,
          pos_id: newPositionId,
          coinTypeA: poolInfo.coinTypeA,
          coinTypeB: poolInfo.coinTypeB,
          amount_a: amountA,
          amount_b: amountB,
          fix_amount_a: fix_amount_a,
          slippage: this.config.maxSlippage,
          is_open: false,
          tick_lower: String(tickLower),
          tick_upper: String(tickUpper),
          collect_fee: false,
          rewarder_coin_types: [],
        };

        logger.info('createAddLiquidityFixTokenPayload — parameters', { ...addLiquidityParams });

        let addTx;
        try {
          addTx = await sdk.Position.createAddLiquidityFixTokenPayload(addLiquidityParams);
        } catch (sdkErr) {
          logger.error('createAddLiquidityFixTokenPayload failed', {
            error: sdkErr instanceof Error ? sdkErr.message : String(sdkErr),
            params: addLiquidityParams,
          });
          throw sdkErr;
        }
        addTx.setGasBudget(this.config.gasBudget);

        const addResult = await suiClient.signAndExecuteTransaction({
          transaction: addTx,
          signer: keypair,
          options: { showEffects: true, showEvents: true },
        });

        if (addResult.effects?.status?.status !== 'success') {
          throw new Error(`Add liquidity failed: ${addResult.effects?.status?.error || 'Unknown'}`);
        }

        addDigest = addResult.digest;
        logger.info('New position opened with liquidity', {
          positionId: newPositionId,
          digest: addResult.digest,
        });
      },
      'add liquidity to new position',
      3,
      2000,
    );

    if (!addDigest) {
      throw new Error('Add liquidity did not return a transaction digest');
    }

    return { transactionDigest: addDigest };
  }

  // ---------------------------------------------------------------------------
  // Private: wait for position object to be accessible on the Sui network
  // ---------------------------------------------------------------------------

  /**
   * Poll suiClient.getObject until the position object is accessible on the
   * Sui network or maxAttempts is exhausted.  The first check is immediate
   * (no delay before attempt 1); subsequent retries wait delayMs before
   * checking again.
   *
   * This guards against the race condition where a newly-created position
   * NFT hasn't yet propagated through the Sui full-node at the time of the
   * add-liquidity call.
   */
  private async waitForPositionObject(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    suiClient: any,
    positionId: string,
    maxAttempts: number = 5,
    delayMs: number = 1500,
  ): Promise<void> {
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      if (attempt > 1) {
        logger.info(
          `Position object not yet accessible — waiting ${delayMs}ms before retry`,
          { positionId, attempt, maxAttempts },
        );
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }

      logger.info('Checking position object accessibility', { positionId, attempt, maxAttempts });

      const response = await suiClient.getObject({
        id: positionId,
        options: { showOwner: true, showType: true },
      });

      if (response.data) {
        // Guard: a position owned by another object (ObjectOwner) cannot be
        // used as a direct transaction input.  Fail fast rather than letting
        // the subsequent add-liquidity call surface a cryptic runtime error.
        const owner = response.data.owner;
        if (owner && typeof owner === 'object' && 'ObjectOwner' in (owner as object)) {
          throw new Error(
            `Position object ${positionId} is owned by another object and cannot be used as a transaction input argument`,
          );
        }
        logger.info('Position object confirmed accessible', { positionId });
        return;
      }

      logger.warn('Position object not yet visible on network', {
        positionId,
        attempt,
        maxAttempts,
        error: response.error,
      });
    }

    throw new Error(`Position object ${positionId} not accessible after ${maxAttempts} attempts`);
  }

  // ---------------------------------------------------------------------------
  // Private: retry helper
  // ---------------------------------------------------------------------------

  private async retryTransaction<T>(
    operation: () => Promise<T>,
    operationName: string,
    maxRetries: number = 3,
    initialDelayMs: number = 2000,
  ): Promise<T> {
    let lastError: Error | undefined;

    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        if (attempt > 0) {
          const delay = initialDelayMs * Math.pow(2, attempt - 1);
          logger.info(`Retry ${attempt + 1}/${maxRetries} for ${operationName} after ${delay}ms`);
          await new Promise(resolve => setTimeout(resolve, delay));
        }
        return await operation();
      } catch (error) {
        const msg = error instanceof Error ? error.message : String(error);
        lastError = error instanceof Error ? error : new Error(msg);

        const isRetryable =
          msg.includes('is not available for consumption') ||
          (msg.includes('Version') && msg.includes('Digest')) ||
          msg.includes('current version:') ||
          (msg.includes('pending') && msg.includes('seconds old')) ||
          (msg.includes('pending') && msg.includes('above threshold'));

        if (!isRetryable) throw error;

        if (attempt < maxRetries - 1) {
          logger.warn(`Retryable error in ${operationName} (attempt ${attempt + 1}/${maxRetries}): ${msg}`);
        } else {
          logger.error(`Max retries exceeded for ${operationName}`);
        }
      }
    }

    throw lastError || new Error(`All retries failed for ${operationName}`);
  }

  /** Normalise coin type for comparison (lowercased, leading zeros stripped). */
  private normalizeCoinType(ct: string): string {
    return ct.toLowerCase().replace(/^0x0+/, '0x');
  }
}
