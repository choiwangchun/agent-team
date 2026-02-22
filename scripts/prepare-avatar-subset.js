const fs = require("fs");
const path = require("path");

const SOURCE_DIR = path.resolve(
  process.cwd(),
  process.env.AVATAR_SUBSET_SOURCE_DIR || process.argv[2] || "avatars"
);
const OUTPUT_DIR = path.resolve(
  process.cwd(),
  process.env.AVATAR_SUBSET_OUTPUT_DIR || ".cache/avatar-subset"
);
const MAX_BYTES = Math.max(
  10 * 1024 * 1024,
  Math.floor(
    Number(process.env.AVATAR_SUBSET_MAX_MB || 900) * 1024 * 1024
  )
);
const SEED_TEXT = String(process.env.AVATAR_SUBSET_SEED || "agent-team-subset");
const MAX_FILES = Math.max(
  1,
  Number(process.env.AVATAR_SUBSET_MAX_FILES || 50000)
);

function toPosixPath(value) {
  return String(value || "").split(path.sep).join("/");
}

function listSvgFiles(rootDir) {
  const files = [];
  const dirs = [rootDir];

  while (dirs.length > 0) {
    const current = dirs.pop();
    const entries = fs.readdirSync(current, { withFileTypes: true });

    for (const entry of entries) {
      const absolute = path.join(current, entry.name);
      if (entry.isDirectory()) {
        dirs.push(absolute);
        continue;
      }
      if (!entry.isFile()) {
        continue;
      }
      if (!entry.name.toLowerCase().endsWith(".svg")) {
        continue;
      }

      const stat = fs.statSync(absolute);
      if (stat.size <= 0) {
        continue;
      }
      files.push({
        absolute,
        relative: toPosixPath(path.relative(rootDir, absolute)),
        size: stat.size,
      });
    }
  }

  files.sort((a, b) => a.relative.localeCompare(b.relative));
  return files;
}

function hashSeed(text) {
  let h = 2166136261;
  const value = String(text || "");
  for (let i = 0; i < value.length; i += 1) {
    h ^= value.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}

function mulberry32(seed) {
  let t = seed >>> 0;
  return function rand() {
    t += 0x6d2b79f5;
    let r = Math.imul(t ^ (t >>> 15), t | 1);
    r ^= r + Math.imul(r ^ (r >>> 7), r | 61);
    return ((r ^ (r >>> 14)) >>> 0) / 4294967296;
  };
}

function shuffleInPlace(items, seedText) {
  const rand = mulberry32(hashSeed(seedText));
  for (let i = items.length - 1; i > 0; i -= 1) {
    const j = Math.floor(rand() * (i + 1));
    const tmp = items[i];
    items[i] = items[j];
    items[j] = tmp;
  }
}

function ensureCleanDir(dirPath) {
  fs.rmSync(dirPath, { recursive: true, force: true });
  fs.mkdirSync(dirPath, { recursive: true });
}

function bytesToMb(bytes) {
  return Number((bytes / 1024 / 1024).toFixed(2));
}

function main() {
  if (!fs.existsSync(SOURCE_DIR)) {
    throw new Error(`source directory not found: ${SOURCE_DIR}`);
  }

  const all = listSvgFiles(SOURCE_DIR);
  if (all.length === 0) {
    throw new Error(`no svg files found in: ${SOURCE_DIR}`);
  }

  console.log(
    `Found ${all.length} svg files. Preparing subset <= ${bytesToMb(MAX_BYTES)} MB`
  );

  shuffleInPlace(all, SEED_TEXT);

  ensureCleanDir(OUTPUT_DIR);

  const selected = [];
  let totalBytes = 0;
  for (const file of all) {
    if (selected.length >= MAX_FILES) {
      break;
    }
    if (totalBytes + file.size > MAX_BYTES) {
      continue;
    }

    const targetPath = path.join(OUTPUT_DIR, file.relative);
    fs.mkdirSync(path.dirname(targetPath), { recursive: true });
    fs.copyFileSync(file.absolute, targetPath);
    selected.push(file);
    totalBytes += file.size;

    if (totalBytes >= MAX_BYTES) {
      break;
    }
  }

  const manifest = {
    generatedAt: new Date().toISOString(),
    sourceDir: SOURCE_DIR,
    outputDir: OUTPUT_DIR,
    seed: SEED_TEXT,
    maxBytes: MAX_BYTES,
    maxFiles: MAX_FILES,
    selectedFiles: selected.length,
    selectedBytes: totalBytes,
    selectedMB: bytesToMb(totalBytes),
    files: selected.map((item) => ({
      path: item.relative,
      size: item.size,
    })),
  };

  const manifestPath = path.join(OUTPUT_DIR, "_subset_manifest.json");
  fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2), "utf8");

  console.log(
    `Subset ready: files=${selected.length}, size=${bytesToMb(totalBytes)} MB`
  );
  console.log(`Output: ${OUTPUT_DIR}`);
  console.log(`Manifest: ${manifestPath}`);
}

try {
  main();
} catch (error) {
  console.error(error.message || String(error));
  process.exit(1);
}
