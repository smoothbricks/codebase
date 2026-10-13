import { describe, expect, it } from 'bun:test';
import { directDiskCommands, PHYSICAL_BOOT_LAYER } from './disk-command-policy.js';

const imageTool = '/usr/bin/hdiutil';
const diskTool = '/usr/sbin/diskutil';

describe('disk-image execution boundary', () => {
  it('rejects direct process calls in Rust, Node, Python and shell, including aliases', () => {
    for (const source of [
      `Command::new("${imageTool}").args(["attach", image]);`,
      `const PROGRAM: &str = "${diskTool}"; Command::new(PROGRAM);`,
      `const executable = '${imageTool}'; spawn(executable, ['attach', image]);`,
      `execFileSync('${diskTool}', ['image', 'attach', image]);`,
      `Bun.spawn(['${imageTool}', 'attach', image]);`,
      `Bun.spawn({ cmd: ['${imageTool}', 'attach', image] });`,
      `argv = ['${diskTool}', 'eject', device]\nsubprocess.run(argv)`,
      `subprocess.run(['${diskTool}', 'image', 'attach', image]);`,
      `sh(['${diskTool}', 'eject', device])`,
      `$\`${imageTool} detach \${device}\``,
      `#!/bin/sh\n${imageTool} attach "$image"`,
      `info=$(${diskTool} info -plist "$device")`,
    ]) {
      expect(directDiskCommands(source).length, source).toBeGreaterThan(0);
    }
  });

  it('allows the shared command layer, its Nx adapter, and declarative command requests', () => {
    for (const source of [
      `SystemCommandRunner.run(&CommandRequest::new("${imageTool}", ["detach", device]));`,
      `host.run('${diskTool}', ['image', 'attach', image]);`,
      `this.commands.run('${imageTool}', ['attach', image]);`,
      `subprocess.run(['cowshed', 'disk-command', '${diskTool}', '--', 'info']);`,
      '$`cowshed disk-command hdiutil -- detach "$device"`',
      `// ${imageTool} attach is prohibited\n/* Command::new("${diskTool}") */`,
      `"""Measure \`${diskTool} image attach\` and \`${imageTool} detach\`."""`,
      `format!("retry with \`${imageTool} detach\`; this message executes nothing");`,
      `{"op":"disk-lease","command":"${imageTool} detach /dev/disk9"}`,
    ]) {
      expect(directDiskCommands(source), source).toEqual([]);
    }
  });

  it('admits only the fixed physical-volume boot commands in their owning layer', () => {
    const boot = [
      `info=$(${diskTool} info -plist "$uuid")`,
      `| ${diskTool} apfs unlockVolume "$uuid" -nomount -stdinpassphrase`,
      `${diskTool} mount -nobrowse -mountPoint "$mountpoint" "$uuid"`,
    ].join('\n');
    expect(directDiskCommands(boot, PHYSICAL_BOOT_LAYER)).toEqual([]);
    expect(directDiskCommands(boot, 'elsewhere.sh').length).toBeGreaterThan(0);
    for (const verb of ['image attach', 'image create', 'attach', 'detach', 'eject']) {
      expect(directDiskCommands(`${diskTool} ${verb} "$image"`, PHYSICAL_BOOT_LAYER)).toEqual([1]);
      expect(directDiskCommands(`${imageTool} ${verb} "$image"`, PHYSICAL_BOOT_LAYER)).toEqual([1]);
    }
    expect(directDiskCommands(`HostCommand::new("${diskTool}", ["image", "attach", path])`)).toEqual([1]);
  });

  it('reports the source line, retaining multiline comment offsets', () => {
    expect(directDiskCommands(`/* example\nnot executed */\nspawn('${imageTool}', []);`)).toEqual([3]);
  });
});
