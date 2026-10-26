#!/bin/sh
# The p99 of each kind of remote request the smoo cli makes, for the remote-http row of
# specs/cowshed/05_gateway.md "Deadlines" (bound: the slowest kind's p99 x 20). Each curl sample is a
# fresh connection read to the end of its body; each gh sample is one gh process, start to exit.
# Usage: FORGEJO_URL=https://<forgejo> tooling/measure/remote-http-p99.sh [samples]   (needs network and a logged-in gh)
set -eu
n=${1:-100}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
gh_token=$(gh auth token)

summarize() {
  kind=$1
  codes=$(cut -d' ' -f1 "$work/$kind" | sort | uniq -c | tr -s ' \n' ' ')
  sort -n -k2 "$work/$kind" | awk -v kind="$kind" -v codes="$codes" '
    { t[NR] = $2 }
    END {
      p50 = t[int(NR * 0.50 + 0.999)]; p99 = t[int(NR * 0.99 + 0.999)]
      printf "%-14s n=%d p50=%.3fs p99=%.3fs max=%.3fs status=[%s]\n", kind, NR, p50, p99, t[NR], codes
    }'
}

curled() {
  kind=$1
  shift
  i=0
  while [ "$i" -lt "$n" ]; do
    curl -s -o /dev/null --max-time 120 -w '%{http_code} %{time_total}\n' "$@" >>"$work/$kind"
    i=$((i + 1))
  done
  summarize "$kind"
}

timed() {
  kind=$1
  shift
  i=0
  while [ "$i" -lt "$n" ]; do
    start=$(perl -MTime::HiRes=time -e 'printf "%.6f", time')
    if "$@" >/dev/null 2>&1; then code=0; else code=$?; fi
    end=$(perl -MTime::HiRes=time -e 'printf "%.6f", time')
    echo "$code $(echo "$end - $start" | bc)" >>"$work/$kind"
    i=$((i + 1))
  done
  summarize "$kind"
}

date -u +%FT%TZ
uptime
curled github -H "Authorization: Bearer $gh_token" -H 'Accept: application/vnd.github+json' \
  'https://api.github.com/repos/smoothbricks/smoothbricks/releases?per_page=5'
curled npm -H 'accept: application/vnd.npm.install-v1+json' 'https://registry.npmjs.org/@smoothbricks%2fcli'
curled forgejo -H 'accept: application/json' "${FORGEJO_URL:?FORGEJO_URL names the Forgejo to measure}/api/v1/repos/search?limit=5"
timed gh-api gh api 'repos/smoothbricks/smoothbricks/releases?per_page=5'
timed gh-secret-list gh secret list --repo smoothbricks/smoothbricks --json name
uptime
