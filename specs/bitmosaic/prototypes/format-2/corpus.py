"""A real text corpus with a tail: the lines of the smoothbricks repository's own tracked files (public, so the
run is repeatable by anyone with the repository).

corpus.py <repo> <out dir> <name> <pathspec>...   writes <out dir>/<name>-postings.txt and <name>-ngrams.txt

A row is one non-empty line; rows are numbered from 0 in `git ls-files` order, then line order. A token is a
maximal run of [a-z0-9_] of at least two bytes after lowercasing; a gram is every byte trigram of the lowercased
line. One list a line, ascending row ordinals, in the key's sorted order; the keys themselves are not written.
"""
import re
import subprocess
import sys
from array import array

repo, out, name, specs = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4:]
files = subprocess.run(['git', '-C', repo, 'ls-files', '--'] + specs, capture_output=True, text=True, check=True).stdout.split('\n')
word = re.compile(rb'[a-z0-9_]{2,}')
post, gram = {}, {}
row = 0
for path in files:
    if not path:
        continue
    try:
        data = open(f'{repo}/{path}', 'rb').read()
    except (FileNotFoundError, IsADirectoryError):
        continue
    for line in data.split(b'\n'):
        line = line.strip().lower()
        if not line:
            continue
        for t in set(word.findall(line)):
            post.setdefault(t, array('I')).append(row)
        for g in {line[i:i + 3] for i in range(len(line) - 2)}:
            gram.setdefault(g, array('I')).append(row)
        row += 1
for kind, table in (('postings', post), ('ngrams', gram)):
    with open(f'{out}/{name}-{kind}.txt', 'w') as f:
        for key in sorted(table):
            f.write(' '.join(map(str, table[key])) + '\n')
    lens = sorted(len(v) for v in table.values())
    n = len(lens)
    print(f'{name} {kind}: rows {row}, lists {n}, members {sum(lens)}, '
          f'len 1: {sum(x == 1 for x in lens)}, 2-3: {sum(2 <= x <= 3 for x in lens)}, 4-32: {sum(4 <= x <= 32 for x in lens)}, '
          f'33-256: {sum(33 <= x <= 256 for x in lens)}, 257-4096: {sum(257 <= x <= 4096 for x in lens)}, >4096: {sum(x > 4096 for x in lens)}, '
          f'p50 {lens[n // 2]} p90 {lens[int(n * .9)]} p99 {lens[int(n * .99)]} max {lens[-1]}')