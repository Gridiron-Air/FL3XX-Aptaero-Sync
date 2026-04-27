import { sql } from "drizzle-orm";
import { pgTable, text, varchar, timestamp, boolean, integer, jsonb } from "drizzle-orm/pg-core";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod";

// Environment types for staging/production isolation
export const environmentEnum = z.enum(['development', 'staging', 'production']);
export type Environment = z.infer<typeof environmentEnum>;

export const users = pgTable("users", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  username: text("username").notNull().unique(),
  password: text("password").notNull(),
});

export const insertUserSchema = createInsertSchema(users).pick({
  username: true,
  password: true,
});

export type InsertUser = z.infer<typeof insertUserSchema>;
export type User = typeof users.$inferSelect;

// API Connection Configuration - now with environment support
export const apiConnections = pgTable("api_connections", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  name: text("name").notNull(),
  type: text("type").notNull(), // 'fl3xx' or 'aptaero'
  environment: text("environment").notNull().default('staging'), // 'development', 'staging', 'production'
  baseUrl: text("base_url").notNull(),
  apiKey: text("api_key"),
  isConnected: boolean("is_connected").default(false),
  lastTestedAt: timestamp("last_tested_at"),
});

export const insertApiConnectionSchema = createInsertSchema(apiConnections).omit({
  id: true,
  lastTestedAt: true,
}).extend({
  environment: environmentEnum.default('staging'),
});

export type InsertApiConnection = z.infer<typeof insertApiConnectionSchema>;
export type ApiConnection = typeof apiConnections.$inferSelect;

// Sync Logs - now with environment tracking
export const syncLogs = pgTable("sync_logs", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  eventType: text("event_type").notNull(),
  status: text("status").notNull(), // 'success', 'error', 'pending', 'syncing'
  source: text("source").notNull(), // 'fl3xx', 'aptaero', 'system'
  environment: text("environment").notNull().default('staging'),
  details: text("details"),
  metadata: jsonb("metadata"),
  createdAt: timestamp("created_at").defaultNow(),
});

export const insertSyncLogSchema = createInsertSchema(syncLogs).omit({
  id: true,
  createdAt: true,
}).extend({
  environment: environmentEnum.default('staging'),
});

export type InsertSyncLog = z.infer<typeof insertSyncLogSchema>;
export type SyncLog = typeof syncLogs.$inferSelect;

// Sync Statistics
export interface SyncStats {
  totalSyncs: number;
  successfulSyncs: number;
  failedSyncs: number;
  lastSyncTime: string | null;
  errorRate: number;
  flightsSynced: number;
  crewSynced: number;
}

// API Endpoint Definition - removed 'passengers' category since FL3XX doesn't handle passenger data
export interface ApiEndpoint {
  id: string;
  method: 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH';
  path: string;
  description: string;
  category: 'flights' | 'aircraft' | 'crew' | 'sync';
  requestExample?: string;
  responseExample?: string;
}

// Sync Configuration - FL3XX only syncs flights and crew (no passengers/APIS)
export const syncConfigs = pgTable("sync_configs", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  environment: text("environment").notNull().default('staging'),
  syncInterval: integer("sync_interval").default(15), // minutes
  autoSync: boolean("auto_sync").default(true), // Default to automatic mode
  syncFlights: boolean("sync_flights").default(true),
  syncAircraft: boolean("sync_aircraft").default(true),
  syncCrew: boolean("sync_crew").default(true),
  lastChangedBy: text("last_changed_by"), // Who changed the autoSync setting
  lastChangedAt: text("last_changed_at"), // When was autoSync last changed (ISO timestamp)
  // Note: Passenger data and APIS compliance are handled directly in Aptaero, not synced from FL3XX
});

export const insertSyncConfigSchema = createInsertSchema(syncConfigs).omit({
  id: true,
}).extend({
  environment: environmentEnum.default('staging'),
  syncInterval: z.number().min(5).max(60).default(15),
  autoSync: z.boolean().default(true),
  syncFlights: z.boolean().default(true),
  syncAircraft: z.boolean().default(true),
  syncCrew: z.boolean().default(true),
  lastChangedBy: z.string().optional(),
  lastChangedAt: z.string().optional(),
});

export type InsertSyncConfig = z.infer<typeof insertSyncConfigSchema>;
export type SyncConfig = typeof syncConfigs.$inferSelect;
