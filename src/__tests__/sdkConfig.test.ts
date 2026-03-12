/**
 * Unit tests for config/sdkConfig.ts
 *
 * Validates that the correct SDK options object is returned for each
 * network and that the required fields are present — no network I/O.
 */

import { getSDKConfig, clmmMainnet, clmmTestnet } from '../config/sdkConfig';

describe('getSDKConfig', () => {
  it('returns mainnet config when network is "mainnet"', () => {
    const cfg = getSDKConfig('mainnet');
    expect(cfg).toBe(clmmMainnet);
  });

  it('returns testnet config when network is "testnet"', () => {
    const cfg = getSDKConfig('testnet');
    expect(cfg).toBe(clmmTestnet);
  });
});

describe('clmmMainnet', () => {
  it('has a non-empty fullRpcUrl', () => {
    expect(clmmMainnet.fullRpcUrl).toBeTruthy();
  });

  it('has integrate package and published_at addresses', () => {
    expect(clmmMainnet.integrate.package_id).toMatch(/^0x/);
    expect(clmmMainnet.integrate.published_at).toMatch(/^0x/);
  });

  it('has clmm_pool package and config', () => {
    expect(clmmMainnet.clmm_pool.package_id).toMatch(/^0x/);
    expect(clmmMainnet.clmm_pool.config).toBeDefined();
  });

  it('has cetus_config package and config', () => {
    expect(clmmMainnet.cetus_config.package_id).toMatch(/^0x/);
    expect(clmmMainnet.cetus_config.config).toBeDefined();
  });

  it('has an aggregatorUrl', () => {
    expect(clmmMainnet.aggregatorUrl).toMatch(/^https?:\/\//);
  });
});

describe('clmmTestnet', () => {
  it('has a non-empty fullRpcUrl', () => {
    expect(clmmTestnet.fullRpcUrl).toBeTruthy();
  });

  it('has integrate package and published_at addresses', () => {
    expect(clmmTestnet.integrate.package_id).toMatch(/^0x/);
    expect(clmmTestnet.integrate.published_at).toMatch(/^0x/);
  });

  it('has clmm_pool package and config', () => {
    expect(clmmTestnet.clmm_pool.package_id).toMatch(/^0x/);
    expect(clmmTestnet.clmm_pool.config).toBeDefined();
  });

  it('has cetus_config package and config', () => {
    expect(clmmTestnet.cetus_config.package_id).toMatch(/^0x/);
    expect(clmmTestnet.cetus_config.config).toBeDefined();
  });

  it('has an aggregatorUrl pointing to devcetus.com (testnet aggregator)', () => {
    expect(clmmTestnet.aggregatorUrl).toMatch(/devcetus\.com/);
  });

  it('mainnet and testnet use different package_id values for clmm_pool', () => {
    expect(clmmMainnet.clmm_pool.package_id).not.toBe(clmmTestnet.clmm_pool.package_id);
  });

  it('mainnet and testnet use different aggregator URLs', () => {
    expect(clmmMainnet.aggregatorUrl).not.toBe(clmmTestnet.aggregatorUrl);
  });
});
