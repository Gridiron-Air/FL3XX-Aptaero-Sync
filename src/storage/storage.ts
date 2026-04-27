import { 
  type User, 
  type InsertUser, 
  type ApiConnection, 
  type InsertApiConnection,
  type SyncLog,
  type InsertSyncLog,
  type SyncConfig,
  type InsertSyncConfig,
  type SyncStats,
  type Environment
} from "../../shared/schema.js";
import { randomUUID } from "crypto";
import { AzureSqlStorage, initializeDatabase } from "./azure-storage.js";

export interface IStorage {
  getUser(id: string): Promise<User | undefined>;
  getUserByUsername(username: string): Promise<User | undefined>;
  createUser(user: InsertUser): Promise<User>;
  
  // API Connections - now environment-scoped
  getConnections(environment: Environment): Promise<ApiConnection[]>;
  getConnectionByType(type: string, environment: Environment): Promise<ApiConnection | undefined>;
  upsertConnection(type: string, environment: Environment, connection: InsertApiConnection): Promise<ApiConnection>;
  updateConnectionStatus(type: string, environment: Environment, isConnected: boolean): Promise<void>;
  
  // Sync Logs - now environment-scoped
  getLogs(environment: Environment, limit?: number): Promise<SyncLog[]>;
  getRecentLogs(environment: Environment, limit?: number): Promise<SyncLog[]>;
  createLog(log: InsertSyncLog): Promise<SyncLog>;
  
  // Sync Config - now environment-scoped
  getSyncConfig(environment: Environment): Promise<SyncConfig | undefined>;
  upsertSyncConfig(environment: Environment, config: InsertSyncConfig): Promise<SyncConfig>;
  
  // Stats - now environment-scoped
  getStats(environment: Environment): Promise<SyncStats>;
}

export class MemStorage implements IStorage {
  private users: Map<string, User>;
  private connections: Map<string, ApiConnection>; // key: `${environment}:${type}`
  private logs: SyncLog[];
  private syncConfigs: Map<string, SyncConfig>; // key: environment

  constructor() {
    this.users = new Map();
    this.connections = new Map();
    this.logs = [];
    this.syncConfigs = new Map();
    
    // Initialize with demo data for staging environment
    this.initializeDemoData();
  }

  private initializeDemoData() {
    // Create sample logs for staging environment (FL3XX only syncs flights and crew)
    const sampleLogs: SyncLog[] = [
      {
        id: randomUUID(),
        eventType: "Flight Sync",
        status: "success",
        source: "fl3xx",
        environment: "staging",
        details: "Synchronized 12 flights from FL3XX",
        metadata: { flightCount: 12 },
        createdAt: new Date(Date.now() - 1000 * 60 * 5),
      },
      {
        id: randomUUID(),
        eventType: "Aircraft Update",
        status: "success",
        source: "fl3xx",
        environment: "staging",
        details: "Updated 3 aircraft records",
        metadata: { aircraftCount: 3 },
        createdAt: new Date(Date.now() - 1000 * 60 * 15),
      },
      {
        id: randomUUID(),
        eventType: "Crew Sync",
        status: "success",
        source: "fl3xx",
        environment: "staging",
        details: "Synchronized 6 crew members from FL3XX",
        metadata: { crewCount: 6 },
        createdAt: new Date(Date.now() - 1000 * 60 * 30),
      },
      {
        id: randomUUID(),
        eventType: "Aptaero Flight Read",
        status: "success",
        source: "aptaero",
        environment: "staging",
        details: "Retrieved 8 flights from Aptaero (read-only validation mode)",
        metadata: { flightCount: 8, mode: "read-only" },
        createdAt: new Date(Date.now() - 1000 * 60 * 45),
      },
      {
        id: randomUUID(),
        eventType: "Validation Mode",
        status: "info",
        source: "system",
        environment: "staging",
        details: "Aptaero sync disabled - currently in read-only validation mode",
        metadata: { note: "No data pushed to Aptaero until staging validation complete" },
        createdAt: new Date(Date.now() - 1000 * 60 * 60),
      },
      {
        id: randomUUID(),
        eventType: "System Health Check",
        status: "success",
        source: "system",
        environment: "staging",
        details: "All staging connections verified",
        metadata: null,
        createdAt: new Date(Date.now() - 1000 * 60 * 90),
      },
    ];
    
    this.logs = sampleLogs;

    // Initialize default sync config for staging (no passenger sync - FL3XX doesn't handle that)
    const stagingConfig: SyncConfig = {
      id: randomUUID(),
      environment: "staging",
      syncInterval: 15,
      autoSync: true, // Default to Automatic mode - toggle on dashboard to switch to manual if issues arise
      syncFlights: true,
      syncAircraft: true,
      syncCrew: true,
      lastChangedBy: "System",
      lastChangedAt: new Date().toISOString(),
    };
    this.syncConfigs.set("staging", stagingConfig);

    // Initialize demo connections for staging environment
    const fl3xxConnection: ApiConnection = {
      id: randomUUID(),
      name: "FL3XX Staging",
      type: "fl3xx",
      environment: "staging",
      baseUrl: "https://app.fl3xx.us",
      apiKey: "fl3xx_staging_key_xxxx",
      isConnected: true,
      lastTestedAt: new Date(Date.now() - 1000 * 60 * 10),
    };

    const aptaeroConnection: ApiConnection = {
      id: randomUUID(),
      name: "Aptaero Staging",
      type: "aptaero",
      environment: "staging",
      baseUrl: "https://staging.aptaero.com",
      apiKey: "aptaero_staging_key_xxxx",
      isConnected: true,
      lastTestedAt: new Date(Date.now() - 1000 * 60 * 10),
    };

    this.connections.set("staging:fl3xx", fl3xxConnection);
    this.connections.set("staging:aptaero", aptaeroConnection);
  }

  async getUser(id: string): Promise<User | undefined> {
    return this.users.get(id);
  }

  async getUserByUsername(username: string): Promise<User | undefined> {
    return Array.from(this.users.values()).find(
      (user) => user.username === username,
    );
  }

  async createUser(insertUser: InsertUser): Promise<User> {
    const id = randomUUID();
    const user: User = { ...insertUser, id };
    this.users.set(id, user);
    return user;
  }

  // API Connections - environment-scoped
  async getConnections(environment: Environment): Promise<ApiConnection[]> {
    return Array.from(this.connections.values()).filter(
      conn => conn.environment === environment
    );
  }

  async getConnectionByType(type: string, environment: Environment): Promise<ApiConnection | undefined> {
    return this.connections.get(`${environment}:${type}`);
  }

  async upsertConnection(type: string, environment: Environment, connection: InsertApiConnection): Promise<ApiConnection> {
    const key = `${environment}:${type}`;
    const existing = this.connections.get(key);
    const newConnection: ApiConnection = {
      id: existing?.id || randomUUID(),
      name: connection.name,
      type: connection.type,
      environment,
      baseUrl: connection.baseUrl,
      apiKey: connection.apiKey ?? null,
      isConnected: connection.isConnected ?? false,
      lastTestedAt: existing?.lastTestedAt || null,
    };
    this.connections.set(key, newConnection);
    return newConnection;
  }

  async updateConnectionStatus(type: string, environment: Environment, isConnected: boolean): Promise<void> {
    const key = `${environment}:${type}`;
    const connection = this.connections.get(key);
    if (connection) {
      connection.isConnected = isConnected;
      connection.lastTestedAt = new Date();
      this.connections.set(key, connection);
    }
  }

  // Sync Logs - environment-scoped
  async getLogs(environment: Environment, limit?: number): Promise<SyncLog[]> {
    const filtered = this.logs.filter(log => log.environment === environment);
    const sorted = filtered.sort((a, b) => {
      const dateA = a.createdAt ? new Date(a.createdAt).getTime() : 0;
      const dateB = b.createdAt ? new Date(b.createdAt).getTime() : 0;
      return dateB - dateA;
    });
    return limit ? sorted.slice(0, limit) : sorted;
  }

  async getRecentLogs(environment: Environment, limit: number = 5): Promise<SyncLog[]> {
    return this.getLogs(environment, limit);
  }

  async createLog(log: InsertSyncLog): Promise<SyncLog> {
    const newLog: SyncLog = {
      id: randomUUID(),
      eventType: log.eventType,
      status: log.status,
      source: log.source,
      environment: log.environment ?? 'staging',
      details: log.details ?? null,
      metadata: log.metadata ?? null,
      createdAt: new Date(),
    };
    this.logs.unshift(newLog);
    return newLog;
  }

  // Sync Config - environment-scoped
  async getSyncConfig(environment: Environment): Promise<SyncConfig | undefined> {
    return this.syncConfigs.get(environment);
  }

  async upsertSyncConfig(environment: Environment, config: InsertSyncConfig): Promise<SyncConfig> {
    const existing = this.syncConfigs.get(environment);
    const newConfig: SyncConfig = {
      id: existing?.id || randomUUID(),
      environment,
      syncInterval: config.syncInterval ?? existing?.syncInterval ?? 15,
      autoSync: config.autoSync ?? existing?.autoSync ?? true,
      syncFlights: config.syncFlights ?? existing?.syncFlights ?? true,
      syncAircraft: config.syncAircraft ?? existing?.syncAircraft ?? true,
      syncCrew: config.syncCrew ?? existing?.syncCrew ?? true,
      lastChangedBy: config.lastChangedBy ?? existing?.lastChangedBy ?? null,
      lastChangedAt: config.lastChangedAt ?? existing?.lastChangedAt ?? null,
    };
    this.syncConfigs.set(environment, newConfig);
    return newConfig;
  }

  // Stats - environment-scoped
  async getStats(environment: Environment): Promise<SyncStats> {
    const envLogs = this.logs.filter(l => l.environment === environment);
    const successfulSyncs = envLogs.filter(l => l.status === 'success').length;
    const failedSyncs = envLogs.filter(l => l.status === 'error').length;
    const totalSyncs = envLogs.length;
    
    const lastSuccessLog = envLogs.find(l => l.status === 'success');
    const lastSyncTime = lastSuccessLog?.createdAt 
      ? (lastSuccessLog.createdAt instanceof Date 
          ? lastSuccessLog.createdAt.toISOString() 
          : lastSuccessLog.createdAt)
      : null;

    // Count flights and crew synced from metadata
    const flightsSynced = envLogs
      .filter(l => l.eventType === 'Flight Sync' && l.status === 'success')
      .reduce((sum, l) => sum + ((l.metadata as any)?.flightCount || 0), 0);
    
    const crewSynced = envLogs
      .filter(l => l.eventType === 'Crew Sync' && l.status === 'success')
      .reduce((sum, l) => sum + ((l.metadata as any)?.crewCount || 0), 0);

    return {
      totalSyncs,
      successfulSyncs,
      failedSyncs,
      lastSyncTime,
      errorRate: totalSyncs > 0 ? (failedSyncs / totalSyncs) * 100 : 0,
      flightsSynced,
      crewSynced,
    };
  }
}

// Check if Azure SQL credentials are configured
const azureSqlConfigured = Boolean(
  process.env.AZURE_SQL_USERNAME && 
  process.env.AZURE_SQL_PASSWORD && 
  process.env.AZURE_SQL_SERVER
);

// Storage instance - starts as MemStorage, can be switched to Azure SQL after initialization
let storageInstance: IStorage = new MemStorage();
let usingAzureSql = false;

export async function initializeStorage(): Promise<void> {
  if (azureSqlConfigured) {
    try {
      await initializeDatabase();
      storageInstance = new AzureSqlStorage();
      usingAzureSql = true;
      console.log("Azure SQL storage initialized successfully");
    } catch (error) {
      console.error("Failed to initialize Azure SQL, using in-memory storage:", error);
      storageInstance = new MemStorage();
      usingAzureSql = false;
    }
  } else {
    console.log("Azure SQL credentials not configured, using in-memory storage");
  }
}

export const storage: IStorage = new Proxy({} as IStorage, {
  get(_target, prop) {
    return (storageInstance as any)[prop];
  }
});

export { initializeDatabase };
export const isUsingAzureSql = () => usingAzureSql;
export const isAzureSqlConfigured = azureSqlConfigured;
