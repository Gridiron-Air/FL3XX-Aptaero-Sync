import type { Environment } from "../../shared/schema.js";

export type SyncTrigger = "timer" | "manual";

export interface RunSyncOptions {
  environment: Environment;
  triggeredBy: SyncTrigger;
  confirmProduction: boolean;
  allowNewFlights: boolean;
  source: string;
}

export interface SyncResult {
  success: boolean;
  message: string;
  environment: Environment;
  synced?: {
    crew: {
      added: number;
      updated: number;
      deactivated: number;
      errors: number;
    };
    flights: {
      created: number;
      updated: number;
      skipped: number;
      errors: number;
      blocked: number;
    };
    crewManifests: {
      assigned: number;
      skipped: number;
      errors: number;
    };
  };
  warning?: string;
  error?: string;
}