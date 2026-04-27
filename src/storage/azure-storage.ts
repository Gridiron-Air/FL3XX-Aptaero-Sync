import sql from 'mssql';
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
import type { IStorage } from "./storage.js";

const config: sql.config = {
  user: process.env.AZURE_SQL_USERNAME,
  password: process.env.AZURE_SQL_PASSWORD || process.env.GA_SQL_ADMIN,
  server: process.env.AZURE_SQL_SERVER || 'gridiron-sqlserver.database.windows.net',
  database: process.env.AZURE_SQL_DATABASE || 'PassengerManifestAppRG',
  port: parseInt(process.env.AZURE_SQL_PORT || '1433'),
  options: {
    encrypt: true,
    trustServerCertificate: false,
  },
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000,
  },
};

let pool: sql.ConnectionPool | null = null;

async function getPool(): Promise<sql.ConnectionPool> {
  if (!pool) {
    pool = await sql.connect(config);
  }
  return pool;
}

export async function initializeDatabase(): Promise<void> {
  console.log(`Connecting to Azure SQL: server=${config.server}, database=${config.database}, user=${config.user}`);
  const db = await getPool();
  
  await db.request().query(`
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='users' AND xtype='U')
    CREATE TABLE users (
      id NVARCHAR(36) PRIMARY KEY DEFAULT NEWID(),
      username NVARCHAR(255) NOT NULL UNIQUE,
      password NVARCHAR(255) NOT NULL
    )
  `);

  await db.request().query(`
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='api_connections' AND xtype='U')
    CREATE TABLE api_connections (
      id NVARCHAR(36) PRIMARY KEY DEFAULT NEWID(),
      name NVARCHAR(255) NOT NULL,
      type NVARCHAR(50) NOT NULL,
      environment NVARCHAR(50) NOT NULL DEFAULT 'staging',
      base_url NVARCHAR(500) NOT NULL,
      api_key NVARCHAR(500),
      is_connected BIT DEFAULT 0,
      last_tested_at DATETIME2,
      CONSTRAINT UQ_connection_env_type UNIQUE (environment, type)
    )
  `);

  await db.request().query(`
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='sync_logs' AND xtype='U')
    CREATE TABLE sync_logs (
      id NVARCHAR(36) PRIMARY KEY DEFAULT NEWID(),
      event_type NVARCHAR(100) NOT NULL,
      status NVARCHAR(50) NOT NULL,
      source NVARCHAR(50) NOT NULL,
      environment NVARCHAR(50) NOT NULL DEFAULT 'staging',
      details NVARCHAR(MAX),
      metadata NVARCHAR(MAX),
      created_at DATETIME2 DEFAULT GETUTCDATE()
    )
  `);

  await db.request().query(`
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='sync_configs' AND xtype='U')
    CREATE TABLE sync_configs (
      id NVARCHAR(36) PRIMARY KEY DEFAULT NEWID(),
      environment NVARCHAR(50) NOT NULL UNIQUE,
      sync_interval INT DEFAULT 15,
      auto_sync BIT DEFAULT 1,
      sync_flights BIT DEFAULT 1,
      sync_aircraft BIT DEFAULT 1,
      sync_crew BIT DEFAULT 1
    )
  `);

  console.log('Azure SQL database tables initialized');
}

export class AzureSqlStorage implements IStorage {
  async getUser(id: string): Promise<User | undefined> {
    const db = await getPool();
    const result = await db.request()
      .input('id', sql.NVarChar, id)
      .query('SELECT * FROM users WHERE id = @id');
    return result.recordset[0];
  }

  async getUserByUsername(username: string): Promise<User | undefined> {
    const db = await getPool();
    const result = await db.request()
      .input('username', sql.NVarChar, username)
      .query('SELECT * FROM users WHERE username = @username');
    return result.recordset[0];
  }

  async createUser(insertUser: InsertUser): Promise<User> {
    const db = await getPool();
    const id = randomUUID();
    await db.request()
      .input('id', sql.NVarChar, id)
      .input('username', sql.NVarChar, insertUser.username)
      .input('password', sql.NVarChar, insertUser.password)
      .query('INSERT INTO users (id, username, password) VALUES (@id, @username, @password)');
    return { id, ...insertUser };
  }

  async getConnections(environment: Environment): Promise<ApiConnection[]> {
    const db = await getPool();
    const result = await db.request()
      .input('environment', sql.NVarChar, environment)
      .query('SELECT * FROM api_connections WHERE environment = @environment');
    return result.recordset.map(this.mapConnection);
  }

  async getConnectionByType(type: string, environment: Environment): Promise<ApiConnection | undefined> {
    const db = await getPool();
    const result = await db.request()
      .input('type', sql.NVarChar, type)
      .input('environment', sql.NVarChar, environment)
      .query('SELECT * FROM api_connections WHERE type = @type AND environment = @environment');
    return result.recordset[0] ? this.mapConnection(result.recordset[0]) : undefined;
  }

  async upsertConnection(type: string, environment: Environment, connection: InsertApiConnection): Promise<ApiConnection> {
    const db = await getPool();
    const existing = await this.getConnectionByType(type, environment);
    const id = existing?.id || randomUUID();

    await db.request()
      .input('id', sql.NVarChar, id)
      .input('name', sql.NVarChar, connection.name)
      .input('type', sql.NVarChar, connection.type)
      .input('environment', sql.NVarChar, environment)
      .input('baseUrl', sql.NVarChar, connection.baseUrl)
      .input('apiKey', sql.NVarChar, connection.apiKey || null)
      .input('isConnected', sql.Bit, connection.isConnected ?? false)
      .query(`
        MERGE api_connections AS target
        USING (SELECT @id AS id) AS source
        ON target.id = source.id
        WHEN MATCHED THEN
          UPDATE SET name = @name, type = @type, environment = @environment, 
                     base_url = @baseUrl, api_key = @apiKey, is_connected = @isConnected
        WHEN NOT MATCHED THEN
          INSERT (id, name, type, environment, base_url, api_key, is_connected)
          VALUES (@id, @name, @type, @environment, @baseUrl, @apiKey, @isConnected);
      `);

    return {
      id,
      name: connection.name,
      type: connection.type,
      environment,
      baseUrl: connection.baseUrl,
      apiKey: connection.apiKey ?? null,
      isConnected: connection.isConnected ?? false,
      lastTestedAt: existing?.lastTestedAt || null,
    };
  }

  async updateConnectionStatus(type: string, environment: Environment, isConnected: boolean): Promise<void> {
    const db = await getPool();
    await db.request()
      .input('type', sql.NVarChar, type)
      .input('environment', sql.NVarChar, environment)
      .input('isConnected', sql.Bit, isConnected)
      .input('lastTestedAt', sql.DateTime2, new Date())
      .query(`
        UPDATE api_connections 
        SET is_connected = @isConnected, last_tested_at = @lastTestedAt 
        WHERE type = @type AND environment = @environment
      `);
  }

  async getLogs(environment: Environment, limit?: number): Promise<SyncLog[]> {
    const db = await getPool();
    const query = limit 
      ? `SELECT TOP ${limit} * FROM sync_logs WHERE environment = @environment ORDER BY created_at DESC`
      : 'SELECT * FROM sync_logs WHERE environment = @environment ORDER BY created_at DESC';
    const result = await db.request()
      .input('environment', sql.NVarChar, environment)
      .query(query);
    return result.recordset.map(this.mapLog);
  }

  async getRecentLogs(environment: Environment, limit: number = 5): Promise<SyncLog[]> {
    return this.getLogs(environment, limit);
  }

  async createLog(log: InsertSyncLog): Promise<SyncLog> {
    const db = await getPool();
    const id = randomUUID();
    const createdAt = new Date();
    const metadata = log.metadata ? JSON.stringify(log.metadata) : null;

    await db.request()
      .input('id', sql.NVarChar, id)
      .input('eventType', sql.NVarChar, log.eventType)
      .input('status', sql.NVarChar, log.status)
      .input('source', sql.NVarChar, log.source)
      .input('environment', sql.NVarChar, log.environment ?? 'staging')
      .input('details', sql.NVarChar, log.details || null)
      .input('metadata', sql.NVarChar, metadata)
      .input('createdAt', sql.DateTime2, createdAt)
      .query(`
        INSERT INTO sync_logs (id, event_type, status, source, environment, details, metadata, created_at)
        VALUES (@id, @eventType, @status, @source, @environment, @details, @metadata, @createdAt)
      `);

    return {
      id,
      eventType: log.eventType,
      status: log.status,
      source: log.source,
      environment: log.environment ?? 'staging',
      details: log.details ?? null,
      metadata: log.metadata ?? null,
      createdAt,
    };
  }

  async getSyncConfig(environment: Environment): Promise<SyncConfig | undefined> {
    const db = await getPool();
    const result = await db.request()
      .input('environment', sql.NVarChar, environment)
      .query('SELECT * FROM sync_configs WHERE environment = @environment');
    return result.recordset[0] ? this.mapConfig(result.recordset[0]) : undefined;
  }

  async upsertSyncConfig(environment: Environment, config: InsertSyncConfig): Promise<SyncConfig> {
    const db = await getPool();
    const existing = await this.getSyncConfig(environment);
    const id = existing?.id || randomUUID();

    await db.request()
      .input('id', sql.NVarChar, id)
      .input('environment', sql.NVarChar, environment)
      .input('syncInterval', sql.Int, config.syncInterval ?? 15)
      .input('autoSync', sql.Bit, config.autoSync ?? true)
      .input('syncFlights', sql.Bit, config.syncFlights ?? true)
      .input('syncAircraft', sql.Bit, config.syncAircraft ?? true)
      .input('syncCrew', sql.Bit, config.syncCrew ?? true)
      .query(`
        MERGE sync_configs AS target
        USING (SELECT @environment AS environment) AS source
        ON target.environment = source.environment
        WHEN MATCHED THEN
          UPDATE SET sync_interval = @syncInterval, auto_sync = @autoSync,
                     sync_flights = @syncFlights, sync_aircraft = @syncAircraft, sync_crew = @syncCrew
        WHEN NOT MATCHED THEN
          INSERT (id, environment, sync_interval, auto_sync, sync_flights, sync_aircraft, sync_crew)
          VALUES (@id, @environment, @syncInterval, @autoSync, @syncFlights, @syncAircraft, @syncCrew);
      `);

    return {
      id,
      environment,
      syncInterval: config.syncInterval ?? 15,
      autoSync: config.autoSync ?? true,
      syncFlights: config.syncFlights ?? true,
      syncAircraft: config.syncAircraft ?? true,
      syncCrew: config.syncCrew ?? true,
    };
  }

  async getStats(environment: Environment): Promise<SyncStats> {
    const db = await getPool();
    
    const statsResult = await db.request()
      .input('environment', sql.NVarChar, environment)
      .query(`
        SELECT 
          COUNT(*) as totalSyncs,
          SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successfulSyncs,
          SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as failedSyncs
        FROM sync_logs WHERE environment = @environment
      `);

    const lastSyncResult = await db.request()
      .input('environment', sql.NVarChar, environment)
      .query(`
        SELECT TOP 1 created_at FROM sync_logs 
        WHERE environment = @environment AND status = 'success' 
        ORDER BY created_at DESC
      `);

    const flightSyncResult = await db.request()
      .input('environment', sql.NVarChar, environment)
      .query(`
        SELECT metadata FROM sync_logs 
        WHERE environment = @environment AND event_type = 'Flight Sync' AND status = 'success'
      `);

    const crewSyncResult = await db.request()
      .input('environment', sql.NVarChar, environment)
      .query(`
        SELECT metadata FROM sync_logs 
        WHERE environment = @environment AND event_type = 'Crew Sync' AND status = 'success'
      `);

    const stats = statsResult.recordset[0];
    const totalSyncs = stats.totalSyncs || 0;
    const successfulSyncs = stats.successfulSyncs || 0;
    const failedSyncs = stats.failedSyncs || 0;

    let flightsSynced = 0;
    for (const row of flightSyncResult.recordset) {
      if (row.metadata) {
        try {
          const meta = typeof row.metadata === 'string' ? JSON.parse(row.metadata) : row.metadata;
          flightsSynced += meta.flightCount || 0;
        } catch {}
      }
    }

    let crewSynced = 0;
    for (const row of crewSyncResult.recordset) {
      if (row.metadata) {
        try {
          const meta = typeof row.metadata === 'string' ? JSON.parse(row.metadata) : row.metadata;
          crewSynced += meta.crewCount || 0;
        } catch {}
      }
    }

    const lastSyncTime = lastSyncResult.recordset[0]?.created_at 
      ? new Date(lastSyncResult.recordset[0].created_at).toISOString()
      : null;

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

  private mapConnection(row: any): ApiConnection {
    return {
      id: row.id,
      name: row.name,
      type: row.type,
      environment: row.environment,
      baseUrl: row.base_url,
      apiKey: row.api_key,
      isConnected: Boolean(row.is_connected),
      lastTestedAt: row.last_tested_at ? new Date(row.last_tested_at) : null,
    };
  }

  private mapLog(row: any): SyncLog {
    let metadata = null;
    if (row.metadata) {
      try {
        metadata = typeof row.metadata === 'string' ? JSON.parse(row.metadata) : row.metadata;
      } catch {}
    }
    return {
      id: row.id,
      eventType: row.event_type,
      status: row.status,
      source: row.source,
      environment: row.environment,
      details: row.details,
      metadata,
      createdAt: row.created_at ? new Date(row.created_at) : null,
    };
  }

  private mapConfig(row: any): SyncConfig {
    return {
      id: row.id,
      environment: row.environment,
      syncInterval: row.sync_interval,
      autoSync: Boolean(row.auto_sync),
      syncFlights: Boolean(row.sync_flights),
      syncAircraft: Boolean(row.sync_aircraft),
      syncCrew: Boolean(row.sync_crew),
    };
  }
}
