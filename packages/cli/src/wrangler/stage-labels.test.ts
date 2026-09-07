import { describe, expect, it } from 'bun:test';
import { hasStageLabel, replaceExactToken, replaceHostnameLabel } from './stage-labels.js';

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
