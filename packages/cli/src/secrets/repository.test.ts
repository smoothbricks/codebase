import { describe, expect, it } from 'bun:test';
import { chooseRepository, repositorySlugFromUrl } from './repository.js';

const mirror = new Map([
  ['private-repo', 'https://github.com/acme/private.git'],
  ['public-repo', 'https://github.com/acme/acme.git'],
]);

describe('secrets repository resolution', () => {
  it("reads the repository from the current branch's upstream when several remotes exist", () => {
    // `gh` refuses outright here ("multiple remotes detected"), which is the
    // public/private mirror layout. The branch already records which side it
    // pushes to, and that is the side holding its secrets.
    const resolved = chooseRepository({ remotes: mirror, upstreamRemote: 'private-repo', requested: undefined });

    expect(resolved).toEqual({
      ok: true,
      choice: { repo: 'acme/private', source: 'upstream of the current branch (private-repo)' },
    });
  });

  it('accepts a remote name where an operator would type one', () => {
    const resolved = chooseRepository({ remotes: mirror, upstreamRemote: null, requested: 'private-repo' });

    expect(resolved).toEqual({ ok: true, choice: { repo: 'acme/private', source: 'remote private-repo' } });
  });

  it('uses the one remote that already carries this branch when nothing tracks it', () => {
    // A branch pushed by explicit refspec has no upstream, which is how the
    // mirror layout is driven; its remote-tracking ref still records where it
    // went, and one carrier is an answer.
    const resolved = chooseRepository({
      remotes: mirror,
      upstreamRemote: null,
      remotesCarryingBranch: ['private-repo'],
      branch: 'billing',
      requested: undefined,
    });

    expect(resolved).toEqual({
      ok: true,
      choice: { repo: 'acme/private', source: 'the only remote carrying this branch (private-repo)' },
    });
  });

  it('refuses when two remotes carry the branch, naming both', () => {
    const resolved = chooseRepository({
      remotes: mirror,
      upstreamRemote: null,
      remotesCarryingBranch: ['private-repo', 'public-repo'],
      branch: 'main',
      requested: undefined,
    });

    expect(resolved.ok).toBe(false);
    if (resolved.ok) throw new Error('expected a refusal');
    expect(resolved.reason).toContain('branch main has no upstream and no remote carries it');
  });

  it('names the candidates instead of guessing when nothing decides', () => {
    const resolved = chooseRepository({ remotes: mirror, upstreamRemote: null, requested: undefined });

    expect(resolved.ok).toBe(false);
    if (resolved.ok) throw new Error('expected a refusal');
    expect(resolved.reason).toContain('private-repo -> acme/private');
    expect(resolved.reason).toContain('public-repo -> acme/acme');
  });

  it('refuses a name that is neither a remote nor a slug, listing the remotes', () => {
    const resolved = chooseRepository({ remotes: mirror, upstreamRemote: null, requested: 'privaterepo' });

    expect(resolved.ok).toBe(false);
    if (resolved.ok) throw new Error('expected a refusal');
    expect(resolved.reason).toContain('private-repo, public-repo');
  });

  it('falls back to origin, then to a sole remote', () => {
    const withOrigin = new Map([...mirror, ['origin', 'git@github.com:acme/app.git']]);
    expect(chooseRepository({ remotes: withOrigin, upstreamRemote: null, requested: undefined })).toEqual({
      ok: true,
      choice: { repo: 'acme/app', source: 'remote origin' },
    });

    const single = new Map([['forge', 'ssh://forge.example.net:2223/acme/widgets.git']]);
    expect(chooseRepository({ remotes: single, upstreamRemote: null, requested: undefined })).toEqual({
      ok: true,
      choice: { repo: 'acme/widgets', source: 'the only remote (forge)' },
    });
  });

  it('parses every remote URL form git writes', () => {
    expect(repositorySlugFromUrl('https://github.com/acme/app.git')).toBe('acme/app');
    expect(repositorySlugFromUrl('git@github.com:acme/app.git')).toBe('acme/app');
    expect(repositorySlugFromUrl('ssh://git@forge.example.net:2223/acme/app')).toBe('acme/app');
    expect(repositorySlugFromUrl('https://user:token@github.com/acme/app')).toBe('acme/app');
    expect(repositorySlugFromUrl('/srv/git/bare.git')).toBeNull();
  });
});
