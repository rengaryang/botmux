import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const workflow = readFileSync('.github/workflows/release-assets.yml', 'utf8');
const legacyWorkflow = readFileSync('.github/workflows/release.yml', 'utf8');
const builder = readFileSync('electron-builder.yml', 'utf8');
const runtime = readFileSync('scripts/prepare-desktop-runtime.mjs', 'utf8');

function jobBlock(name: string, nextName: string): string {
  const start = workflow.indexOf(`  ${name}:`);
  const end = workflow.indexOf(`  ${nextName}:`, start + 1);
  expect(start).toBeGreaterThanOrEqual(0);
  expect(end).toBeGreaterThan(start);
  return workflow.slice(start, end);
}

describe('cross-platform desktop release', () => {
  it('declares native Windows and Linux package targets', () => {
    expect(builder).toContain('win:');
    expect(builder).toContain('target: nsis');
    expect(builder).toContain('target: zip');
    expect(builder).toContain('linux:');
    expect(builder).toContain('target: AppImage');
    expect(builder).toContain('target: deb');
    expect(builder).toContain('target: tar.gz');
  });

  it('stages architecture-matched Node runtimes for every supported OS', () => {
    for (const target of ['darwin-arm64', 'darwin-x64', 'linux-x64', 'linux-arm64', 'win32-x64']) {
      expect(runtime).toContain(`'${target}'`);
    }
    expect(runtime).toContain('BOTMUX_DESKTOP_TARGETS');
    expect(runtime).toContain('node.exe');
    expect(runtime).toContain("run('unzip', ['-q', archive, '-d', extracted], root)");
    expect(runtime).not.toContain('Expand-Archive');
    expect(runtime).toContain("os: supportedOs");
  });

  it('builds macOS, Windows, and Linux assets on native GitHub runners', () => {
    const mac = jobBlock('desktop-macos', 'desktop-windows');
    const windows = jobBlock('desktop-windows', 'desktop-linux');
    const linux = jobBlock('desktop-linux', 'publish-desktop-release');

    expect(mac).toContain('runs-on: macos-14');
    expect(mac).toContain('electron-builder --mac dmg zip --universal');
    expect(windows).toContain('runs-on: windows-2022');
    expect(windows).toContain('electron-builder --win nsis zip --x64');
    expect(linux).toContain('ubuntu-22.04');
    expect(linux).toContain('ubuntu-24.04-arm');
    expect(linux).toContain('electron-builder --linux AppImage');
    expect(linux).toContain('Build optional Linux deb');
    expect(linux).toContain('continue-on-error: true');
    expect(linux).toContain('electron-builder --linux deb');
    expect(linux).toContain('electron-builder --linux tar.gz');
  });

  it('publishes checksums and attaches all platform artifacts to one release', () => {
    expect(workflow).toContain('Diagnose Windows runtime on failure');
    expect(workflow).toContain('scripts/diagnose-desktop-release.mjs');
    expect(workflow).toContain('botmux-desktop-windows-diagnostics');
    expect(workflow).toContain('Diagnose Linux package output on failure');
    expect(workflow).toContain('SHA256SUMS');
    expect(workflow).toContain('pattern: botmux-desktop-*');
    expect(workflow).toContain('merge-multiple: true');
    expect(workflow).toContain('needs: [desktop-macos, desktop-windows, desktop-linux]');
  });

  it('does not require upstream npm trust to create a fork prerelease', () => {
    expect(workflow).toContain("if: github.repository == 'deepcoldy/botmux'");
    expect(workflow).toContain("if: github.repository != 'deepcoldy/botmux'");
    expect(workflow).toContain('Create GitHub Release');
    expect(workflow).toContain('unsigned preview; Gatekeeper may require manual approval');
    expect(workflow).toContain('unsigned preview; SmartScreen may warn');
    expect(workflow.match(/Verify tag still resolves to the checked-out commit/g)).toHaveLength(3);
    expect(workflow.match(/git fetch origin "refs\/tags\/\$\{RELEASE_TAG\}:refs\/tags\/\$\{RELEASE_TAG\}" --force --quiet/g)).toHaveLength(3);
    expect(legacyWorkflow).toContain("if: github.repository == 'deepcoldy/botmux' && github.event_name == 'push'");
  });
});
