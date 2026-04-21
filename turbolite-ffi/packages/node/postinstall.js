/**
 * postinstall: enable SQLITE_USE_URI in better-sqlite3.
 *
 * turbolite uses URI filenames (file:path?vfs=turbolite-N) to select
 * per-database VFS instances. better-sqlite3 ships with SQLITE_USE_URI=0,
 * so we patch the define and rebuild from source.
 *
 * This runs automatically after `npm install`.
 */

'use strict';

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const definesPath = path.join(
  __dirname, 'node_modules', 'better-sqlite3', 'deps', 'defines.gypi'
);

if (!fs.existsSync(definesPath)) {
  // better-sqlite3 not installed yet (e.g. during npm pack). Skip.
  process.exit(0);
}

const content = fs.readFileSync(definesPath, 'utf8');
if (content.includes('SQLITE_USE_URI=1')) {
  // Already patched.
  process.exit(0);
}

if (!content.includes('SQLITE_USE_URI=0')) {
  console.warn('turbolite postinstall: SQLITE_USE_URI not found in defines.gypi, skipping');
  process.exit(0);
}

console.log('turbolite postinstall: enabling SQLITE_USE_URI in better-sqlite3...');
fs.writeFileSync(definesPath, content.replace('SQLITE_USE_URI=0', 'SQLITE_USE_URI=1'));

// Rebuild better-sqlite3 from source with the patched define.
const bsDir = path.join(__dirname, 'node_modules', 'better-sqlite3');
try {
  execSync('npx --no-install prebuild-install || npm run build-release', {
    cwd: bsDir,
    stdio: 'inherit',
    timeout: 120000,
  });
  console.log('turbolite postinstall: better-sqlite3 rebuilt with SQLITE_USE_URI=1');
} catch (e) {
  console.error('turbolite postinstall: failed to rebuild better-sqlite3:', e.message);
  process.exit(1);
}
