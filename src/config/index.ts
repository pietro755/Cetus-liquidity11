import * as dotenv from 'dotenv';

dotenv.config();

export interface BotConfig {
  // Network
  network: 'mainnet' | 'testnet';
  suiRpcUrl?: string;
  privateKey: string;

  // Bot
  checkInterval: number; // seconds (default 60)

  // Pool
  poolAddress: string;
  lowerTick?: number;
  upperTick?: number;
  rangeWidth?: number;

  // Token amounts for zap-in (required — must be set in .env)
  tokenAAmount: string;
  tokenBAmount: string;

  // Risk management
  maxSlippage: number;
  gasBudget: number;

  // Logging
  logLevel: 'debug' | 'info' | 'warn' | 'error';
  verboseLogs: boolean;
}

function getEnvVar(key: string, required: boolean = true): string {
  const value = process.env[key];
  if (required && !value) {
    throw new Error(`Missing required environment variable: ${key}`);
  }
  return value || '';
}

function getEnvNumber(key: string, defaultValue: number): number {
  const value = process.env[key];
  return value ? parseFloat(value) : defaultValue;
}

function getEnvBoolean(key: string, defaultValue: boolean): boolean {
  const value = process.env[key];
  return value ? value.toLowerCase() === 'true' : defaultValue;
}

function validateTokenAmount(key: string, value: string | undefined): void {
  if (value === undefined || value === '') {
    throw new Error(
      `${key} is required and must be a positive integer (base units / MIST). Got: ${value}`,
    );
  }
  if (!/^\d+$/.test(value)) {
    throw new Error(
      `${key} must be a positive integer (base units / MIST). Got: ${value}`,
    );
  }
  if (BigInt(value) <= 0n) {
    throw new Error(
      `${key} must be a positive integer (base units / MIST). Got: ${value}`,
    );
  }
}

export function loadConfig(): BotConfig {
  const network = getEnvVar('NETWORK', false) || 'mainnet';

  if (network !== 'mainnet' && network !== 'testnet') {
    throw new Error(`Invalid NETWORK value: ${network}. Must be 'mainnet' or 'testnet'`);
  }

  const maxSlippage = getEnvNumber('MAX_SLIPPAGE', 0.01);

  // Safety guard: a slippage of ≥ 100 % would hand over all funds to MEV/sandwich attacks
  // on mainnet.  Reject any value outside (0, 1).
  if (maxSlippage <= 0 || maxSlippage >= 1) {
    throw new Error(
      `MAX_SLIPPAGE must be greater than 0 and less than 1 (100 %). Got: ${maxSlippage}`,
    );
  }

  return {
    network,
    suiRpcUrl: getEnvVar('SUI_RPC_URL', false) || undefined,
    privateKey: getEnvVar('PRIVATE_KEY'),
    checkInterval: getEnvNumber('CHECK_INTERVAL', 60), // 60s matches the loop requirement
    poolAddress: getEnvVar('POOL_ADDRESS'),
    lowerTick: getEnvVar('LOWER_TICK', false) ? parseInt(getEnvVar('LOWER_TICK', false)) : undefined,
    upperTick: getEnvVar('UPPER_TICK', false) ? parseInt(getEnvVar('UPPER_TICK', false)) : undefined,
    rangeWidth: getEnvVar('RANGE_WIDTH', false) ? parseInt(getEnvVar('RANGE_WIDTH', false)) : undefined,
    tokenAAmount: (() => {
      const v = getEnvVar('TOKEN_A_AMOUNT');
      validateTokenAmount('TOKEN_A_AMOUNT', v);
      return v;
    })(),
    tokenBAmount: (() => {
      const v = getEnvVar('TOKEN_B_AMOUNT');
      validateTokenAmount('TOKEN_B_AMOUNT', v);
      return v;
    })(),
    maxSlippage,
    gasBudget: getEnvNumber('GAS_BUDGET', 50000000),
    logLevel: (getEnvVar('LOG_LEVEL', false) || 'info') as 'debug' | 'info' | 'warn' | 'error',
    verboseLogs: getEnvBoolean('VERBOSE_LOGS', false),
  };
}

export const config = loadConfig();
