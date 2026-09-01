const { execFileSync } = require('node:child_process');
const { existsSync, unlinkSync } = require('node:fs');
const { join } = require('node:path');

const unusedPrivacyUsageKeys = [
  'NSAppleEventsUsageDescription',
  'NSBluetoothAlwaysUsageDescription',
  'NSBluetoothPeripheralUsageDescription',
  'NSCameraUsageDescription',
  'NSMicrophoneUsageDescription',
  'NSScreenCaptureUsageDescription',
];

async function afterPack(context) {
  const productFilename = context.packager?.appInfo?.productFilename ?? 'Botmux';
  const resourcesPath = resolveResourcesPath(context, productFilename);
  expandRuntimeModules(resourcesPath);
  if (context.electronPlatformName === 'darwin') scrubMacPrivacyUsageKeys(context.appOutDir, productFilename);
}

function resolveResourcesPath(context, productFilename) {
  if (context.electronPlatformName === 'darwin') {
    return join(context.appOutDir, `${productFilename}.app`, 'Contents', 'Resources');
  }
  return join(context.appOutDir, 'resources');
}

function expandRuntimeModules(resourcesPath) {
  const stagedModules = join(resourcesPath, 'runtime', 'node_modules.tar.gz');
  const runtimeModules = join(resourcesPath, 'runtime', 'node_modules');
  if (existsSync(stagedModules) && !existsSync(runtimeModules)) {
    execFileSync('tar', ['-xzf', stagedModules, '-C', join(resourcesPath, 'runtime')]);
    unlinkSync(stagedModules);
  }
}

function scrubMacPrivacyUsageKeys(appOutDir, productFilename) {
  const plistPath = join(appOutDir, `${productFilename}.app`, 'Contents', 'Info.plist');
  if (!existsSync(plistPath)) return;
  for (const key of unusedPrivacyUsageKeys) {
    try {
      execFileSync('/usr/libexec/PlistBuddy', ['-c', `Delete :${key}`, plistPath], { stdio: 'ignore' });
    } catch {
      // Missing keys are fine; different Electron versions stamp different defaults.
    }
  }
}

module.exports = afterPack;
module.exports.default = afterPack;
