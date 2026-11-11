import { describe, expect, it } from 'bun:test';
import { declarationOf, goDeclarations, goRunSummaryText, parseGoList } from './go-run-summary.js';

const SUMMARY = {
  passed: 571,
  failed: 2,
  skipped: 4,
  packages: 32,
  failedTests: [
    { package: 'example.com/m/hts/pipeline', test: 'TestStatesItOnce' },
    { package: 'example.com/m/hts/pipeline', test: 'TestNowhere' },
  ],
  failedPackages: ['example.com/m/host'],
};

describe('a Go run summary', () => {
  it('states the counts, each failed test at its declaration and each package that failed without one', () => {
    const declared = new Map([['example.com/m/hts/pipeline::TestStatesItOnce', 'pkg/go/hts/pipeline/once_test.go:12']]);
    expect(goRunSummaryText(SUMMARY, declared)).toBe(
      [
        'go test summary: 571 passed, 2 failed, 4 skipped, 32 packages',
        'go test failed: pkg/go/hts/pipeline/once_test.go:12 TestStatesItOnce',
        // A test whose declaration was not found keeps its package: a reader must not take it for a file.
        'go test failed: example.com/m/hts/pipeline TestNowhere',
        'go test package failed: example.com/m/host',
        '',
      ].join('\n'),
    );
  });

  it('finds the one file of a package that declares a test, and none when two do or none does', () => {
    const once = {
      path: 'once_test.go',
      text: 'package p\n\nimport "testing"\n\nfunc TestStatesItOnce(t *testing.T) {\n}\n',
    };
    const other = { path: 'other_test.go', text: 'package p\n\nfunc TestStatesItOnceMore(t *testing.T) {}\n' };
    const method = { path: 'suite_test.go', text: 'package p\n\nfunc (s *Suite) TestStatesItOnce() {}\n' };
    expect(declarationOf('TestStatesItOnce', [other, method, once])).toEqual({ path: 'once_test.go', line: 5 });
    expect(declarationOf('TestStatesItOnce', [other, method])).toBeNull();
    // An in-package and an external test file may both declare the name; then no one file is the test's.
    expect(declarationOf('TestStatesItOnce', [once, { ...once, path: 'again_test.go' }])).toBeNull();
  });

  it('reads go list by its format and resolves a failed test to a workspace path', () => {
    const listed = parseGoList(
      'example.com/m/hts/pipeline\t/ws/pkg/go/hts/pipeline\tonce_test.go,other_test.go\texternal_test.go\nexample.com/m/bare\t/ws/pkg/go/bare\t\t\n',
    );
    expect(listed).toEqual([
      {
        package: 'example.com/m/hts/pipeline',
        directory: '/ws/pkg/go/hts/pipeline',
        files: ['once_test.go', 'other_test.go', 'external_test.go'],
      },
      { package: 'example.com/m/bare', directory: '/ws/pkg/go/bare', files: [] },
    ]);
    const texts = new Map([
      ['/ws/pkg/go/hts/pipeline/once_test.go', 'package p\n\nfunc TestStatesItOnce(t *testing.T) {}\n'],
      ['/ws/pkg/go/hts/pipeline/other_test.go', 'package p\n'],
    ]);
    const declared = goDeclarations(SUMMARY.failedTests, '/ws', listed, (path) => texts.get(path) ?? null);
    // A file that could not be read declares nothing; it does not stop the others.
    expect([...declared]).toEqual([
      ['example.com/m/hts/pipeline::TestStatesItOnce', 'pkg/go/hts/pipeline/once_test.go:3'],
    ]);
  });
});
