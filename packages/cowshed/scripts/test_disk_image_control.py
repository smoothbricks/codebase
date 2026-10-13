"""Pure argument/identity proofs; importing the control never attaches an image."""
from pathlib import Path
import runpy
import unittest

control = runpy.run_path(str(Path(__file__).with_name('disk-image-control.py')))


class DiskImageControlTests(unittest.TestCase):
    def test_facade_preserves_the_plain_argv(self):
        args = ['attach', '-nomount', '-plist', '/image with spaces.dmg']
        self.assertEqual(control['disk_argv'](None, 'hdiutil', args), ['/usr/bin/hdiutil', *args])
        self.assertEqual(control['disk_argv'](Path('/built/cowshed'), 'hdiutil', args),
                         ['/built/cowshed', 'disk-command', 'hdiutil', '--', *args])

    def test_physical_and_synthesized_devices_cannot_be_confused(self):
        report = {'system-entities': [
            {'dev-entry': 'disk42s1', 'content-hint': 'Apple_APFS_Volume'},
            {'dev-entry': 'disk42', 'content-hint': 'Apple_APFS_Container'},
            {'dev-entry': 'disk7', 'content-hint': 'GUID_partition_scheme'},
        ]}
        self.assertEqual(control['physical_device'](report), '/dev/disk7')
        self.assertEqual(control['container_device'](report), '/dev/disk42')
        report['system-entities'].pop()
        with self.assertRaises(ValueError):
            control['physical_device'](report)

    def test_ambiguous_physical_media_is_refused(self):
        report = {'system-entities': [
            {'dev-entry': '/dev/disk7', 'content-hint': ''},
            {'dev-entry': '/dev/disk8', 'content-hint': ''},
        ]}
        with self.assertRaises(ValueError):
            control['physical_device'](report)


if __name__ == '__main__':
    unittest.main()
