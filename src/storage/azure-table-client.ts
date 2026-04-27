import { TableClient, TableServiceClient } from "@azure/data-tables";

export interface FlightMapping {
  fl3xxFlightId: string;
  airlineChoiceFlightId: string;
  estimatedDepartureTime?: Date;
}

export interface CrewMapping {
  fl3xxId: string;             // FL3XX's id field (Users table primary key, e.g., "705928")
  aptaeroCrewId: string;       // Aptaero's MasterCrewMembers.ID (UUID)
  badgeNoLastSynced: string;   // Last known badge number (for change detection)
  lastSyncedAt: string;        // ISO timestamp of last sync
  firstName?: string;          // For display/debugging
  lastName?: string;           // For display/debugging
}

export interface SyncStatus {
  id: string;                  // "latest" for most recent sync
  timestamp: string;           // ISO timestamp
  success: boolean;
  environment: string;
  crew: { added: number; updated: number; deactivated: number; errors: number };
  flights: { created: number; updated: number; skipped: number; errors: number };
  crewManifests: { assigned: number; skipped: number; errors: number };
  source: string;              // "webjob" or "manual"
  duration?: number;           // milliseconds
}

export class AzureTableClient {
  private connectionString: string;

  constructor(connectionString: string) {
    this.connectionString = connectionString;
  }

  private getTableClient(tableName: string): TableClient {
    return TableClient.fromConnectionString(this.connectionString, tableName);
  }

  async getFlightMappings(dateFrom?: Date, dateTo?: Date): Promise<FlightMapping[]> {
    try {
      const tableClient = this.getTableClient("tblflights");
      
      let filter = "";
      if (dateFrom && dateTo) {
        filter = `estimatedDepartureTime ge datetime'${dateFrom.toISOString()}' and estimatedDepartureTime le datetime'${dateTo.toISOString()}'`;
      }
      
      const mappings: FlightMapping[] = [];
      const entities = tableClient.listEntities({ queryOptions: filter ? { filter } : undefined });
      
      for await (const entity of entities) {
        // Support both old field name (aptaeroSegmentId) and new field name (airlineChoiceFlightId)
        const segmentId = String(entity.airlineChoiceFlightId || entity.aptaeroSegmentId || "");
        mappings.push({
          fl3xxFlightId: String(entity.fl3xxFlightId || entity.rowKey || ""),
          airlineChoiceFlightId: segmentId,
          estimatedDepartureTime: entity.estimatedDepartureTime 
            ? new Date(entity.estimatedDepartureTime as string) 
            : undefined,
        });
      }
      
      console.log(`Azure Table: Retrieved ${mappings.length} flight mappings`);
      return mappings;
    } catch (error) {
      console.error("Error fetching flight mappings from Azure Table:", error);
      throw error;
    }
  }

  async getFlightMappingByFL3XXId(fl3xxFlightId: string): Promise<FlightMapping | null> {
    try {
      const tableClient = this.getTableClient("tblflights");
      const entity = await tableClient.getEntity("flights", String(fl3xxFlightId));
      
      // Support both old field name (aptaeroSegmentId) and new field name (airlineChoiceFlightId)
      const segmentId = String(entity.airlineChoiceFlightId || entity.aptaeroSegmentId || "");
      
      return {
        fl3xxFlightId: String(entity.fl3xxFlightId || entity.rowKey || ""),
        airlineChoiceFlightId: segmentId,
        estimatedDepartureTime: entity.estimatedDepartureTime 
          ? new Date(entity.estimatedDepartureTime as string) 
          : undefined,
      };
    } catch (error) {
      console.error(`Error fetching mapping for FL3XX ID ${fl3xxFlightId}:`, error);
      return null;
    }
  }

  async testConnection(): Promise<{ success: boolean; message: string; count?: number }> {
    try {
      const tableClient = this.getTableClient("tblflights");
      let count = 0;
      const entities = tableClient.listEntities();
      for await (const _entity of entities) {
        count++;
        if (count >= 5) break;
      }
      return { success: true, message: "Azure Table Storage connection successful", count };
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      return { success: false, message: `Azure Table connection failed: ${message}` };
    }
  }

  // Upsert (create or update) a flight mapping after successful POST to Aptaero
  async upsertFlightMapping(
    fl3xxFlightId: string, 
    aptaeroSegmentId: string, 
    estimatedDepartureTime?: Date
  ): Promise<{ success: boolean; message: string }> {
    try {
      const tableClient = this.getTableClient("tblflights");
      
      const entity = {
        partitionKey: "flights",
        rowKey: String(fl3xxFlightId),
        fl3xxFlightId: String(fl3xxFlightId),
        airlineChoiceFlightId: aptaeroSegmentId,
        estimatedDepartureTime: estimatedDepartureTime?.toISOString(),
        lastUpdated: new Date().toISOString(),
      };
      
      await tableClient.upsertEntity(entity, "Merge");
      
      console.log(`Azure Table: Upserted mapping FL3XX ${fl3xxFlightId} -> Aptaero ${aptaeroSegmentId}`);
      return { success: true, message: `Mapping saved: FL3XX ${fl3xxFlightId} -> Aptaero ${aptaeroSegmentId}` };
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      console.error(`Error upserting flight mapping: ${message}`);
      return { success: false, message: `Failed to save mapping: ${message}` };
    }
  }

  // ========== CREW MAPPING METHODS ==========
  
  // Get all crew mappings
  async getCrewMappings(): Promise<CrewMapping[]> {
    try {
      const tableClient = this.getTableClient("tblcrew");
      const mappings: CrewMapping[] = [];
      const entities = tableClient.listEntities();
      
      for await (const entity of entities) {
        mappings.push({
          fl3xxId: String(entity.fl3xxId || entity.rowKey || ""),
          aptaeroCrewId: String(entity.aptaeroCrewId || ""),
          badgeNoLastSynced: String(entity.badgeNoLastSynced || ""),
          lastSyncedAt: String(entity.lastSyncedAt || ""),
          firstName: entity.firstName ? String(entity.firstName) : undefined,
          lastName: entity.lastName ? String(entity.lastName) : undefined,
        });
      }
      
      console.log(`Azure Table: Retrieved ${mappings.length} crew mappings`);
      return mappings;
    } catch (error) {
      console.error("Error fetching crew mappings from Azure Table:", error);
      throw error;
    }
  }

  // Get crew mapping by FL3XX pilotId
  async getCrewMappingByFL3XXId(fl3xxId: string): Promise<CrewMapping | null> {
    try {
      const tableClient = this.getTableClient("tblcrew");
      const entity = await tableClient.getEntity("crew", String(fl3xxId));
      
      return {
        fl3xxId: String(entity.fl3xxId || entity.rowKey || ""),
        aptaeroCrewId: String(entity.aptaeroCrewId || ""),
        badgeNoLastSynced: String(entity.badgeNoLastSynced || ""),
        lastSyncedAt: String(entity.lastSyncedAt || ""),
        firstName: entity.firstName ? String(entity.firstName) : undefined,
        lastName: entity.lastName ? String(entity.lastName) : undefined,
      };
    } catch (error) {
      // Entity not found is expected for new crew
      return null;
    }
  }

  // Get crew mapping by Aptaero ID (for reverse lookup)
  async getCrewMappingByAptaeroId(aptaeroCrewId: string): Promise<CrewMapping | null> {
    try {
      const tableClient = this.getTableClient("tblcrew");
      const filter = `aptaeroCrewId eq '${aptaeroCrewId}'`;
      const entities = tableClient.listEntities({ queryOptions: { filter } });
      
      for await (const entity of entities) {
        return {
          fl3xxId: String(entity.fl3xxId || entity.rowKey || ""),
          aptaeroCrewId: String(entity.aptaeroCrewId || ""),
          badgeNoLastSynced: String(entity.badgeNoLastSynced || ""),
          lastSyncedAt: String(entity.lastSyncedAt || ""),
          firstName: entity.firstName ? String(entity.firstName) : undefined,
          lastName: entity.lastName ? String(entity.lastName) : undefined,
        };
      }
      return null;
    } catch (error) {
      console.error(`Error fetching crew mapping by Aptaero ID ${aptaeroCrewId}:`, error);
      return null;
    }
  }

  // Ensure table exists (create if not)
  private async ensureTableExists(tableName: string): Promise<void> {
    try {
      const serviceClient = TableServiceClient.fromConnectionString(this.connectionString);
      await serviceClient.createTable(tableName);
      console.log(`Azure Table: Created table '${tableName}'`);
    } catch (error: any) {
      // TableAlreadyExists is not an error
      if (error.statusCode !== 409) {
        throw error;
      }
    }
  }

  // Upsert (create or update) a crew mapping
  async upsertCrewMapping(mapping: CrewMapping): Promise<{ success: boolean; message: string }> {
    try {
      // Ensure the tblcrew table exists
      await this.ensureTableExists("tblcrew");
      
      const tableClient = this.getTableClient("tblcrew");
      
      const entity = {
        partitionKey: "crew",
        rowKey: String(mapping.fl3xxId),
        fl3xxId: String(mapping.fl3xxId),
        aptaeroCrewId: mapping.aptaeroCrewId,
        badgeNoLastSynced: mapping.badgeNoLastSynced,
        lastSyncedAt: mapping.lastSyncedAt || new Date().toISOString(),
        firstName: mapping.firstName || "",
        lastName: mapping.lastName || "",
      };
      
      await tableClient.upsertEntity(entity, "Merge");
      
      console.log(`Azure Table: Upserted crew mapping FL3XX ${mapping.fl3xxId} (${mapping.firstName} ${mapping.lastName}) -> Aptaero ${mapping.aptaeroCrewId}`);
      return { 
        success: true, 
        message: `Crew mapping saved: FL3XX ${mapping.fl3xxId} -> Aptaero ${mapping.aptaeroCrewId}` 
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      console.error(`Error upserting crew mapping: ${message}`);
      return { success: false, message: `Failed to save crew mapping: ${message}` };
    }
  }

  // Delete a crew mapping (for cleanup)
  async deleteCrewMapping(fl3xxId: string): Promise<{ success: boolean; message: string }> {
    try {
      const tableClient = this.getTableClient("tblcrew");
      await tableClient.deleteEntity("crew", String(fl3xxId));
      
      console.log(`Azure Table: Deleted crew mapping for FL3XX ${fl3xxId}`);
      return { success: true, message: `Crew mapping deleted for FL3XX ${fl3xxId}` };
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      console.error(`Error deleting crew mapping: ${message}`);
      return { success: false, message: `Failed to delete crew mapping: ${message}` };
    }
  }

  // ========== SYNC STATUS METHODS ==========

  // Save sync execution status (persists across app restarts)
  async saveSyncStatus(status: Omit<SyncStatus, 'id'>): Promise<{ success: boolean; message: string }> {
    try {
      await this.ensureTableExists("tblsyncstatus");
      const tableClient = this.getTableClient("tblsyncstatus");
      
      // Save as "latest" for quick retrieval
      const entity = {
        partitionKey: "syncstatus",
        rowKey: "latest",
        timestamp: status.timestamp,
        success: status.success,
        environment: status.environment,
        crewAdded: status.crew.added,
        crewUpdated: status.crew.updated,
        crewDeactivated: status.crew.deactivated,
        crewErrors: status.crew.errors,
        flightsCreated: status.flights.created,
        flightsUpdated: status.flights.updated,
        flightsSkipped: status.flights.skipped,
        flightsErrors: status.flights.errors,
        manifestsAssigned: status.crewManifests.assigned,
        manifestsSkipped: status.crewManifests.skipped,
        manifestsErrors: status.crewManifests.errors,
        source: status.source,
        duration: status.duration || 0,
      };
      
      await tableClient.upsertEntity(entity, "Replace");
      
      // Also save a historical record with unique rowKey
      const uniqueSuffix = Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
      const historyEntity = {
        ...entity,
        rowKey: `${status.timestamp.replace(/[:.]/g, '-')}-${uniqueSuffix}`,
      };
      try {
        await tableClient.upsertEntity(historyEntity, "Replace");
      } catch (historyError) {
        // Historical record is optional, don't fail the main save
        console.warn('Could not save historical sync record:', historyError);
      }
      
      console.log(`Azure Table: Saved sync status (${status.success ? 'success' : 'failed'}) at ${status.timestamp}`);
      return { success: true, message: "Sync status saved" };
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      console.error(`Error saving sync status: ${message}`);
      return { success: false, message: `Failed to save sync status: ${message}` };
    }
  }

  // Get the most recent sync status
  async getLatestSyncStatus(): Promise<SyncStatus | null> {
    try {
      const tableClient = this.getTableClient("tblsyncstatus");
      const entity = await tableClient.getEntity("syncstatus", "latest");
      
      return {
        id: "latest",
        timestamp: String(entity.timestamp || ""),
        success: Boolean(entity.success),
        environment: String(entity.environment || ""),
        crew: {
          added: Number(entity.crewAdded || 0),
          updated: Number(entity.crewUpdated || 0),
          deactivated: Number(entity.crewDeactivated || 0),
          errors: Number(entity.crewErrors || 0),
        },
        flights: {
          created: Number(entity.flightsCreated || 0),
          updated: Number(entity.flightsUpdated || 0),
          skipped: Number(entity.flightsSkipped || 0),
          errors: Number(entity.flightsErrors || 0),
        },
        crewManifests: {
          assigned: Number(entity.manifestsAssigned || 0),
          skipped: Number(entity.manifestsSkipped || 0),
          errors: Number(entity.manifestsErrors || 0),
        },
        source: String(entity.source || "unknown"),
        duration: Number(entity.duration || 0),
      };
    } catch (error) {
      // No sync status yet is not an error
      console.log("No sync status found in Azure Table (first run)");
      return null;
    }
  }
}

export function createAzureTableClient(connectionString: string): AzureTableClient {
  return new AzureTableClient(connectionString);
}
