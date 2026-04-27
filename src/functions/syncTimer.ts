import { app, InvocationContext, Timer } from "@azure/functions";
import { runFl3xxAptaeroSync } from "../sync/runSync.js";
import { getBooleanSetting, getSyncEnvironment } from "../config/environment.js";

app.timer("fl3xxAptaeroSyncTimer", {
  schedule: process.env.SYNC_TIMER_SCHEDULE || "0 */15 * * * *",
  runOnStartup: false,
  handler: async (_timer: Timer, context: InvocationContext): Promise<void> => {
    const environment = getSyncEnvironment();
    const allowNewFlights = getBooleanSetting("ALLOW_NEW_FLIGHTS", false);

    context.log("Starting FL3XX → Aptaero sync", {
      environment,
      allowNewFlights,
      source: "azure-function-timer"
    });

    try {
      const result = await runFl3xxAptaeroSync({
        environment,
        triggeredBy: "timer",
        confirmProduction: environment === "production",
        allowNewFlights,
        source: "azure-function-timer"
      });

      if (!result.success) {
        context.error("FL3XX → Aptaero sync completed with failure", result);
        throw new Error(result.error || result.message || "Sync failed");
      }

      context.log("FL3XX → Aptaero sync completed", result);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      context.error(`FL3XX → Aptaero sync failed: ${message}`, error);
      throw error;
    }
  }
});