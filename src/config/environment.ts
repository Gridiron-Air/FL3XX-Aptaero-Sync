import { environmentEnum, type Environment } from "../../shared/schema.js";

export function getSyncEnvironment(): Environment {
  const raw = process.env.SYNC_ENVIRONMENT || "staging";
  const parsed = environmentEnum.safeParse(raw);

  if (!parsed.success) {
    console.warn(`Invalid SYNC_ENVIRONMENT '${raw}', falling back to staging`);
    return "staging";
  }

  return parsed.data;
}

export function getBooleanSetting(name: string, defaultValue: boolean): boolean {
  const raw = process.env[name];

  if (raw === undefined || raw === null || raw.trim() === "") {
    return defaultValue;
  }

  return ["true", "1", "yes", "y"].includes(raw.toLowerCase());
}

export function requireEnv(name: string): string {
  const value = process.env[name];

  if (!value || value.trim() === "") {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value;
}