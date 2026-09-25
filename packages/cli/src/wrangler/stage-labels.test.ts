import { describe, expect, it } from 'bun:test';
import {
  hasStageLabel,
  replaceExactToken,
  replaceHostnameLabel,
  routeHostname,
  wildcardDnsRecord,
} from './stage-labels.js';

describe('hasStageLabel', () => {
  it('detects the staging label as a hostname segment', () => {
    expect(hasStageLabel('site.staging.example.com/*')).toBe(true);
    expect(hasStageLabel('https://app.staging.example.com')).toBe(true);
    expect(hasStageLabel('staging.example.com/*')).toBe(true);
  });

  it('does not match a name without the label or with the label as part of a word', () => {
    expect(hasStageLabel('next.example.com')).toBe(false);
    expect(hasStageLabel('oauth-staging.example.com/*')).toBe(false);
    expect(hasStageLabel('app.staging-old.example.com/*')).toBe(false);
  });

  it('is true exactly when the hostname rewrite would change the value', () => {
    for (const value of ['site.staging.example.com/*', 'next.example.com', 'app.staging-old.example.com/*']) {
      expect(hasStageLabel(value)).toBe(replaceHostnameLabel(value, 'pr7') !== value);
    }
  });
});

describe('label rewrites', () => {
  it('rewrites the staging hostname label and the exact hyphen-delimited token', () => {
    expect(replaceHostnameLabel('*.staging.example.com/*', 'pr7')).toBe('*.pr7.example.com/*');
    expect(replaceExactToken('site-staging-db', 'staging', 'pr7')).toBe('site-pr7-db');
  });
});

describe('routeHostname', () => {
  it('is the host a route pattern serves, without its wildcard label or path', () => {
    expect(routeHostname('*.pr7.example.com')).toBe('pr7.example.com');
    expect(routeHostname('*.pr7.example.com/*')).toBe('pr7.example.com');
    expect(routeHostname('site.pr7.example.com/api/*')).toBe('site.pr7.example.com');
  });

  it('is lowercase, since hostnames do not distinguish case', () => {
    expect(routeHostname('*.PR7.Example.com/*')).toBe('pr7.example.com');
  });
});

describe('wildcardDnsRecord', () => {
  it('points the wildcard name at the host a zoned *. route serves, with or without a path', () => {
    const record = { zoneName: 'example.com', name: '*.pr7.example.com', content: 'pr7.example.com' };
    expect(wildcardDnsRecord({ pattern: '*.pr7.example.com/*', zoneName: 'example.com' })).toEqual(record);
    expect(wildcardDnsRecord({ pattern: '*.pr7.example.com', zoneName: 'example.com' })).toEqual(record);
    expect(wildcardDnsRecord({ pattern: '*.PR7.Example.com/*', zoneName: 'example.com' })).toEqual(record);
  });

  it('is nothing for a route without a zone name or without a leading *. label', () => {
    expect(wildcardDnsRecord({ pattern: '*.pr7.example.com/*' })).toBeUndefined();
    expect(wildcardDnsRecord({ pattern: 'site.pr7.example.com/*', zoneName: 'example.com' })).toBeUndefined();
  });
});
