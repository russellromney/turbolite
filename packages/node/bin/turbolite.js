#!/usr/bin/env node

const { execFileSync } = require("child_process");
const path = require("path");
const fs = require("fs");

const platform = process.platform;
const arch = process.arch;

// Map Node platform/arch to binary name
const ext = platform === "win32" ? ".exe" : "";
const binaryName = `turbolite${ext}`;
const binaryPath = path.join(__dirname, "..", binaryName);

if (!fs.existsSync(binaryPath)) {
  console.error(
    `turbolite CLI binary not found at ${binaryPath}.\n` +
      `Ensure the package was installed with the platform-specific binary.`
  );
  process.exit(1);
}

try {
  execFileSync(binaryPath, process.argv.slice(2), { stdio: "inherit" });
} catch (e) {
  if (e.status != null) {
    process.exit(e.status);
  }
  throw e;
}
