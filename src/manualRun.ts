import { runFl3xxAptaeroSync } from "./sync/runSync.js";
import { getBooleanSetting, getSyncEnvironment } from "./config/environment.js";

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