#!/usr/bin/env python3
"""Explicit host diagnostic: plain/facade controls and old/new detach A/B, 10 cycles each.

Run only after host recovery, with other attachers stopped. No subprocess timeout,
process kill, sudo, daemon restart, or cowshed setup occurs here. Do not interrupt
an image operation. The plain leg deliberately bypasses cowshed as the control.
The first two legs use identical hdiutil argv and the physical whole disk. The
last two both use facade diskutil image attach, changing only hdiutil's detach
target: old synthesized APFS container versus new physical image device.
"""
import argparse
import json
import os
from pathlib import Path
import plistlib
import re
import subprocess
import sys
import time
import uuid

CLASSES = ('AppleDiskImageDevice', 'DIDeviceIOUserClient', 'AppleAPFSVolume', 'IOMedia')
TOOLS = {'hdiutil': '/usr/bin/hdiutil', 'diskutil': '/usr/sbin/diskutil'}


def disk_argv(cowshed, tool, args):
    return ([str(cowshed), 'disk-command', tool, '--'] if cowshed else [TOOLS[tool]]) + args


def physical_device(report):
    """Select image media, never the synthesized APFS container or a slice."""
    candidates = []
    for entity in report['system-entities']:
        device = entity.get('dev-entry', '')
        if not device.startswith('/dev/'):
            device = '/dev/' + device
        hint = entity.get('content-hint', '')
        if re.fullmatch(r'/dev/disk[0-9]+', device) and hint in ('', 'GUID_partition_scheme', 'Apple_partition_scheme'):
            candidates.append(device)
    if len(candidates) != 1:
        raise ValueError(f'attach must name exactly one physical image device, got {candidates!r}')
    return candidates[0]


def container_device(report):
    candidates = [
        entity['dev-entry'] for entity in report['system-entities']
        if entity.get('content-hint', '') == 'Apple_APFS_Container'
        or entity.get('content-hint', '').upper().startswith('EF57347C-')
    ]
    candidates = [value if value.startswith('/dev/') else '/dev/' + value for value in candidates]
    candidates = [value for value in candidates if re.fullmatch(r'/dev/disk[0-9]+', value)]
    if len(candidates) != 1:
        raise ValueError(f'attach must name exactly one synthesized container, got {candidates!r}')
    return candidates[0]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('cowshed', type=Path, help='absolute path of the built cowshed with disk-command')
    parser.add_argument('scratch_parent', type=Path, help='existing directory in the assigned shed, never /tmp')
    args = parser.parse_args()
    cowshed = args.cowshed.resolve(strict=True)
    if not os.access(cowshed, os.X_OK):
        parser.error(f'{cowshed} is not executable')
    parent = args.scratch_parent.resolve(strict=True)
    root = parent / ('disk-image-control-' + uuid.uuid4().hex)
    root.mkdir(mode=0o700)
    image = root / 'control.dmg'
    device = None
    uncertain_attachment = False
    with (root / 'results.jsonl').open('w') as journal:
        def record(row):
            line = json.dumps(row, sort_keys=True)
            print(line, flush=True)
            journal.write(line + '\n')
            journal.flush()

        def run(argv):
            record({'command': argv})
            result = subprocess.run(argv, capture_output=True)
            if result.returncode:
                record({'exit': result.returncode, 'stdout': result.stdout.decode(errors='replace'),
                        'stderr': result.stderr.decode(errors='replace')})
                raise RuntimeError(f'command exited {result.returncode}; evidence retained in {root}')
            return result.stdout

        def counts(leg, cycle, phase):
            raw = run(['/usr/sbin/ioclasscount', *CLASSES]).decode().strip()
            values = {name: int(value) for name, value in re.findall(r'(\w+)\s*=\s*(\d+)', raw)}
            if any(name not in values for name in CLASSES):
                raise ValueError(f'ioclasscount did not report all classes: {raw}')
            record({'leg': leg, 'cycle': cycle, 'phase': phase, 'ioclasscount': raw, 'counts': values})
            return values

        try:
            counts('create', 0, 'before')
            uncertain_attachment = True
            run(disk_argv(None, 'hdiutil', ['create', '-size', '128m', '-type', 'UDIF',
                                         '-fs', 'APFS', '-volname', 'cowshed-control', str(image)]))
            uncertain_attachment = False
            counts('create', 0, 'after')
            legs = [
                ('plain-hdiutil', None, 'hdiutil', False),
                ('facade-hdiutil', cowshed, 'hdiutil', False),
                ('old-container-detach', cowshed, 'diskutil', True),
                ('new-physical-detach', cowshed, 'diskutil', False),
            ]
            for leg, facade, tool, old_target in legs:
                before = counts(leg, 0, 'baseline')
                for cycle in range(1, 11):
                    counts(leg, cycle, 'before')
                    attach = (['attach', '-nomount', '-plist', str(image)] if tool == 'hdiutil' else
                              ['image', 'attach', '--nobrowse', '--noMount', '--plist', str(image)])
                    uncertain_attachment = True
                    output = run(disk_argv(facade, tool, attach))
                    (root / 'last-attach.plist').write_bytes(output)
                    report = plistlib.loads(output)
                    device = physical_device(report)
                    target = container_device(report) if old_target else device
                    uncertain_attachment = False
                    record({'leg': leg, 'cycle': cycle, 'physicalDevice': device, 'detachTarget': target})
                    run(disk_argv(facade, 'hdiutil', ['detach', target]))
                    device = None
                    time.sleep(1)
                    after = counts(leg, cycle, 'after')
                record({'leg': leg, 'cycles': 10, 'delta': {name: after[name] - before[name] for name in CLASSES}})
        finally:
            if device is not None:
                # Only the exact device returned by this run is eligible for cleanup.
                run(disk_argv(cowshed, 'hdiutil', ['detach', device]))
                device = None
                uncertain_attachment = False
            if uncertain_attachment:
                print(f'Attachment outcome unknown; preserve {image} and {root}/last-attach.plist', file=sys.stderr)
            elif image.exists():
                image.unlink()
    print(f'Evidence: {root}/results.jsonl', flush=True)


if __name__ == '__main__':
    main()
