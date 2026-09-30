#!/bin/sh
# Build the fixed source data set once, on the host, outside any image: a real node_modules tree,
# 40,000 extra symlinks (node_modules/.bin-like), 2 GiB of cargo-target-like files, all committed to git.
set -eu
P=${PROTO_ROOT:?set PROTO_ROOT}
NODE_MODULES=${1:?usage: mkds.sh <a Bun-installed node_modules directory>}
DS=$P/ds
rm -rf "$DS"
mkdir -p "$DS/target/deps" "$DS/farm"
/usr/bin/ditto "$NODE_MODULES" "$DS/node_modules"
i=0
while [ $i -lt 24 ]; do "$P/xtool" gen "$DS/target/big$i.bin" 64; i=$((i + 1)); done
i=0
while [ $i -lt 512 ]; do "$P/xtool" gen "$DS/target/deps/lib$i.rlib" 1; i=$((i + 1)); done
python3 - "$DS" <<'EOF'
import os, sys
ds = sys.argv[1]
targets = sorted(os.listdir(os.path.join(ds, 'node_modules')))
for d in range(400):
    dd = os.path.join(ds, 'farm', f'p{d:03d}')
    os.makedirs(dd)
    for k in range(100):
        t = targets[(d * 100 + k) % len(targets)]
        os.symlink(f'../../node_modules/{t}', os.path.join(dd, f'l{k:03d}'))
EOF
cd "$DS"
printf 'target/\n' > .gitignore
git init -q
git add -A
git -c user.email=proto@example.invalid -c user.name=proto -c core.hooksPath=/dev/null commit -qm fill
du -sh "$DS" "$DS/.git" "$DS/node_modules" "$DS/target"
echo files=$(find "$DS" -type f | wc -l) symlinks=$(find "$DS" -type l | wc -l) dirs=$(find "$DS" -type d | wc -l)
git ls-files | wc -l
