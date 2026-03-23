# patchnote-action

GitHub Action wrapper for patchnote, published from the `smoothbricks` monorepo as a same-repo subpath action.

## Usage

```yaml
- uses: smoothbricks/codebase/packages/patchnote-action@feat/add-dep-updater-package
  with:
    token: ${{ secrets.PATCHNOTE_TOKEN }}
```

## Maintainer Notes

- The action runs the co-located `packages/patchnote` source from the same git ref as the action itself.
- While the action is still under test, point consumers at the branch ref `feat/add-dep-updater-package`.
