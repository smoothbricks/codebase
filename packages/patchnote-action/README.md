# patchnote-action

GitHub Action wrapper for patchnote, published from the `smoothbricks` monorepo as a same-repo subpath action.

## Usage

```yaml
- uses: smoothbricks/smoothbricks/packages/patchnote-action@v1
  with:
    token: ${{ secrets.PATCHNOTE_TOKEN }}
```

## Maintainer Notes

- The action runs the co-located `packages/patchnote` source from the same git ref as the action itself.
- Keep the major tag (`v1`) updated to a released commit so the documented action reference resolves for users.
