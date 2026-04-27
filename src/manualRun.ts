import fs from "node:fs";
import path from "node:path";
import { runFl3xxAptaeroSync } from "./sync/runSync.js";
import { getBooleanSetting, getSyncEnvironment } from "./config/environment.js";

function loadLocalSettings(): void {
  const localSettingsPath = path.resolve(process.cwd(), "local.settings.json");

  if (!fs.existsSync(localSettingsPath)) {
    console.warn("local.settings.json not found. Using existing environment variables.");
    return;
  }

  const raw = fs.readFileSync(localSettingsPath, "utf8");
  const parsed = JSON.parse(raw) as {
    Values?: Record<string, string>;
  };

  for (const [key, value] of Object.entries(parsed.Values || {})) {
    if (process.env[key] === undefined) {
      process.env[key] = value;
    }
  }
}

loadLocalSettings();

async function main(): Promise<void> {
  const environment = getSyncEnvironment();
  const allowNewFlights = getBooleanSetting("ALLOW_NEW_FLIGHTS", false);

  console.log("Starting manual FL3XX → Aptaero sync", {
    environment,
    allowNewFlights
  });

  const result = await runFl3xxAptaeroSync({
    environment,
    triggeredBy: "manual",
    confirmProduction: environment === "production",
    allowNewFlights,
    source: "manual-local-run"
  });

  console.log(JSON.stringify(result, null, 2));

  if (!result.success) {
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error("Manual sync failed:", error);
  process.exit(1);
});