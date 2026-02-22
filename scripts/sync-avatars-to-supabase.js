const fs = require("fs");
const path = require("path");
const { createClient } = require("@supabase/supabase-js");

const SUPABASE_URL = String(process.env.SUPABASE_URL || "").trim();
const SUPABASE_SERVICE_ROLE_KEY = String(
  process.env.SUPABASE_SERVICE_ROLE_KEY || ""
).trim();
const SUPABASE_AVATAR_BUCKET = String(
  process.env.SUPABASE_AVATAR_BUCKET || "avatars"
).trim();
const SOURCE_DIR = path.resolve(process.cwd(), process.argv[2] || "avatars");
const UPSERT = String(process.env.AVATAR_SYNC_UPSERT || "false")
  .trim()
  .toLowerCase() === "true";

function toPosixPath(value) {
  return String(value || "").split(path.sep).join("/");
}

function listSvgFilesRecursive(rootDir) {
  if (!rootDir || !fs.existsSync(rootDir)) {
    return [];
  }

  const files = [];
  const stack = [rootDir];
  while (stack.length > 0) {
    const currentDir = stack.pop();
    const entries = fs.readdirSync(currentDir, { withFileTypes: true });
    for (const entry of entries) {
      const absolute = path.join(currentDir, entry.name);
      if (entry.isDirectory()) {
        stack.push(absolute);
        continue;
      }
      if (!entry.isFile()) {
        continue;
      }
      if (!entry.name.toLowerCase().endsWith(".svg")) {
        continue;
      }
      files.push(absolute);
    }
  }

  return files.sort((a, b) => a.localeCompare(b));
}

function isAlreadyExistsError(error) {
  const message = String(error?.message || "").toLowerCase();
  const code = String(error?.statusCode || error?.status || "");
  return message.includes("already exists") || code === "409";
}

async function main() {
  if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    throw new Error(
      "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required"
    );
  }

  if (!fs.existsSync(SOURCE_DIR)) {
    throw new Error(`source directory not found: ${SOURCE_DIR}`);
  }

  const files = listSvgFilesRecursive(SOURCE_DIR);
  if (files.length === 0) {
    console.log(`No SVG files found in ${SOURCE_DIR}`);
    return;
  }

  const client = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
    },
  });

  let uploaded = 0;
  let skipped = 0;
  let failed = 0;

  console.log(
    `Sync start: ${files.length} files -> bucket "${SUPABASE_AVATAR_BUCKET}" (upsert=${UPSERT})`
  );

  for (const absolutePath of files) {
    const relativePath = toPosixPath(path.relative(SOURCE_DIR, absolutePath));
    const file = fs.readFileSync(absolutePath);
    const { error } = await client.storage
      .from(SUPABASE_AVATAR_BUCKET)
      .upload(relativePath, file, {
        contentType: "image/svg+xml",
        cacheControl: "31536000",
        upsert: UPSERT,
      });

    if (!error) {
      uploaded += 1;
      continue;
    }

    if (!UPSERT && isAlreadyExistsError(error)) {
      skipped += 1;
      continue;
    }

    failed += 1;
    console.error(`Upload failed: ${relativePath} -> ${error.message}`);
  }

  console.log(
    `Sync complete: uploaded=${uploaded}, skipped=${skipped}, failed=${failed}, total=${files.length}`
  );

  if (failed > 0) {
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
