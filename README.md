# Cetus Liquidity Rebalance Bot

A minimal automatic liquidity rebalancing bot for [Cetus CLMM](https://app.cetus.zone/) on the Sui Network.

The bot runs a simple loop every 60 seconds:

1. Fetch the position with the highest liquidity in the configured pool.
2. Check if the current pool tick is inside `[tickLower, tickUpper]`.
3. If **in range** → do nothing.
4. If **out of range** → remove liquidity, store the exact liquidity value, swap tokens to the correct ratio, open a new position with the stored liquidity.

## Prerequisites

- Node.js ≥ 18
- A Sui wallet with funds (SUI for gas + pool tokens for liquidity)

## Quick Start

```bash
# 1. Install dependencies
npm install

# 2. Configure environment
cp .env.example .env
# Edit .env — set PRIVATE_KEY and POOL_ADDRESS at minimum

# 3. Build
npm run build

# 4. Run
npm start
```

## Testing on Testnet

Use testnet to verify the bot's behaviour without risking real assets.

```bash
# Copy the example env and configure for testnet
cp .env.example .env

# Edit .env:
#   NETWORK=testnet
#   PRIVATE_KEY=<your testnet wallet key>
#   POOL_ADDRESS=<a Cetus testnet pool address>
#   DRY_RUN=true   # simulate without broadcasting any transactions

npm run build && npm start
```

The bot will log each step (pool tick, position range, swap direction) and mark all
actions as `[DRY Run]` so no gas is consumed.  Once you are satisfied with the
output, set `DRY_RUN=false` to execute real transactions on testnet.

## Unit Tests

```bash
npm test            # run all tests once
npm run test:watch  # watch mode
npm run test:coverage  # with coverage report
```

The test suite covers config loading (including mainnet/testnet network selection and
slippage safety), pool range detection, SDK config correctness for both networks,
retry/backoff resilience, `CetusSDKService` input validation, and the rebalance
flow in dry-run mode — all without any external network I/O.

## Configuration

| Variable | Required | Default | Description |
|---|---|---|---|
| `PRIVATE_KEY` | ✅ | — | 64-char hex Ed25519 private key |
| `POOL_ADDRESS` | ✅ | — | Cetus CLMM pool address (`0x…`) |
| `NETWORK` | | `mainnet` | `mainnet` or `testnet` |
| `SUI_RPC_URL` | | public endpoint | Custom Sui RPC URL |
| `CHECK_INTERVAL` | | `60` | Seconds between checks |
| `LOWER_TICK` | | auto | Lower tick for new position |
| `UPPER_TICK` | | auto | Upper tick for new position |
| `MAX_SLIPPAGE` | | `0.01` | Max slippage — must be > 0 and < 1 (i.e. 0–100 % exclusive) |
| `GAS_BUDGET` | | `50000000` | Gas budget in MIST |
| `LOG_LEVEL` | | `info` | `debug` \| `info` \| `warn` \| `error` |
| `DRY_RUN` | | `false` | Simulate without executing |

When `LOWER_TICK` / `UPPER_TICK` are not set, the bot preserves the old position's tick-range width, centred on the current tick.

## Rebalance Flow

```
out-of-range detected
  ↓
1. removeLiquidity          — captures exact liquidity value from closed position
2. getWalletBalances        — reads post-removal token balances from chain
3. swapTokensIfNeeded       — Cetus aggregator swap to achieve correct token ratio
4. openPosition (NFT)       — creates the new position object on-chain
5. addLiquidity             — deposits using the stored liquidity delta;
                              wallet balances are the hard cap (no extra funds drawn)
```

## Project Structure

```
src/
  index.ts              Main loop (60s interval)
  config/
    index.ts            Environment variable loading
    sdkConfig.ts        Cetus SDK on-chain addresses (mainnet + testnet)
  services/
    sdk.ts              Wallet + RPC + SDK initialization
    monitor.ts          Pool info & position fetching
    rebalance.ts        Rebalance logic (remove → swap → add)
  utils/
    logger.ts           Timestamped console logger
    retry.ts            Exponential-backoff retry helper
  __tests__/
    config.test.ts      Config loading + slippage safety tests (mainnet + testnet)
    monitor.test.ts     isPositionInRange unit tests
    sdkConfig.test.ts   SDK config correctness tests (mainnet + testnet)
    sdkService.test.ts  CetusSDKService input validation tests
    retry.test.ts       isNetworkError + retryWithBackoff tests
    rebalance.test.ts   Rebalance dry-run and guard tests
.env.example            Configuration template
```

## Security

- **Never commit your `.env` file** — it contains your private key.
- Use a dedicated bot wallet with only the tokens needed.
- Always test with `DRY_RUN=true` first (on testnet before mainnet).
- `MAX_SLIPPAGE` is validated at startup: values ≥ 1.0 (100 %) are rejected to prevent catastrophic sandwich attacks on mainnet.

## License

MIT
