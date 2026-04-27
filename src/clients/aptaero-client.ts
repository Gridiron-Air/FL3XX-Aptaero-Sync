import type { Environment } from "../../shared/schema.js";

export interface AptaeroFlightSegment {
  id: string;
  flightNumber: string;
  departureDate: string;
  departureTime: string;
  arrivalTime: string;
  originAirport: string;
  destinationAirport: string;
  carrierCode: string;
  tailNo: string;
  capacity: number | null;
  booked: number | null;
  status: string;
  type?: number | null; // 1=Public, 2=Private
  typeLabel?: string | null; // "Public", "Private"
  // Raw datetime values from Aptaero API (ISO 8601 format)
  scheduledDepartureTime?: string;
  scheduledArrivalTime?: string;
  estimatedDepartureTime?: string;
  estimatedArrivalTime?: string;
  message?: string;
}

export interface AptaeroConnectionConfig {
  baseUrl: string;
  username: string;
  password: string;
}

// Aptaero Customer (for mapping FL3XX accounts to Aptaero customers)
export interface AptaeroCustomer {
  id: string;
  carrierCode: string;
  code: string;
  name: string;
  email: string;
}

// Master Crew Member (full list of all crew) - from MasterCrewMembers endpoint
export interface AptaeroMasterCrewMember {
  id: string;
  externalId: string;  // ExternalID field - populated with FL3XX pilotId for stable correlation
  badgeNo: string;
  firstName: string;
  lastName: string;
  middleName: string;
  gender: string;
  weight: string;
  email: string;
  phone: string;
  dateOfBirth: string;
  nationality: string;
  residence: string;
  statusOnBoard: number;
  isActive: boolean;
  // Home Address (from HomeAddress object)
  homeStreet1: string;
  homeStreet2: string;
  homeCity: string;
  homeStateCode: string;
  homePostalCode: string;
  homeCountryCode: string;
  // Birthplace Address (from BirthplaceAddress object)
  birthplaceCity: string;
  birthplaceStateCode: string;
  birthplaceCountryCode: string;
  // Travel Document 1 (Passport)
  passportDocNo: string;
  passportDocExpiry: string;
  passportDocIssue: string;
  // Travel Document 2 (Pilot License)
  pilotLicenseDocNo: string;
  pilotLicenseDocIssue: string;
}

// Crew Member assigned to a specific flight
export interface AptaeroFlightCrewMember {
  id: string;
  masterCrewMemberId: string;
  flightSegmentId: string;
  badgeNo: string;
  firstName: string;
  lastName: string;
  gender: string;
  email: string;
  statusOnBoard: number; // 0=Not checked in, 1=Checked in, etc.
  position: string;
  isDeleted?: boolean;  // True if crew has strikethrough (Journey.IsDeleted=true)
}

export interface AptaeroCrewDataResponse {
  masterCrew: AptaeroMasterCrewMember[];
  lastSync: string;
  connectionStatus: "connected" | "disconnected" | "error";
  error?: string;
}

export interface AptaeroFlightCrewResponse {
  crewMembers: AptaeroFlightCrewMember[];
  flightSegmentId: string;
  lastSync: string;
  connectionStatus: "connected" | "disconnected" | "error";
  error?: string;
}

// Request to add a crew member to the Master Crew List
export interface AddMasterCrewMemberRequest {
  CarrierCode: string;
  ExternalID?: string;  // FL3XX pilotId - stable correlation key for "Import Master Crew List" workflow
  BadgeNo: string;
  FirstName: string;
  LastName: string;
  MiddleName?: string;
  Gender: string; // "M" or "F"
  Email?: string;
  Telephone?: string;
  DOB?: string; // Date of birth
  Nationality?: string; // 3-letter country code
  Residence?: string; // 3-letter country code
  StatusOnBoard: number; // 1=Pilot, 2=Flight Attendant, 4=Other
  IsActive?: boolean;
  Weight?: number;
  HomeAddress?: {
    Street1?: string;
    Street2?: string;
    City?: string;
    StateCode?: string;
    PostalCode?: string;
    CountryCode?: string;
  };
  BirthplaceAddress?: {
    City?: string;
    StateCode?: string;
    CountryCode?: string;
  };
  TravelDocument1?: {
    DocCode?: string;  // P=Passport, etc.
    DocNo?: string;
    DocExpiry?: string;
    DocIssue?: string;
  };
  TravelDocument2?: {
    DocCode?: string;  // L=License, etc.
    DocNo?: string;
    DocExpiry?: string;
    DocIssue?: string;
  };
}

export interface AptaeroDataResponse {
  flights: AptaeroFlightSegment[];
  lastSync: string;
  connectionStatus: "connected" | "disconnected" | "error";
  error?: string;
}

// Request types for Aptaero API (based on FL3XX -> Aptaero mapping)
export interface AddFlightSegmentRequest {
  CarrierCode: string;
  FlightNo: string;
  Type: number;
  TailNo: string;
  OriginIATA: string;
  DestinationIATA: string;
  ScheduledDepartureTime: string;
  EstimatedDepartureTime: string;
  ScheduledArrivalTime: string;
  EstimatedArrivalTime: string;
  OffBlocksTime?: string;
  TakeOffTime?: string;
  TouchDownTime?: string;
  OnBlocksTime?: string;
  Message?: string;
  Status: number;
  CustomerID?: string;  // Aptaero customer GUID - maps from FL3XX accountName
}

// Update request extends Add request with required FlightSegmentID
export interface UpdateFlightSegmentRequest extends AddFlightSegmentRequest {
  ID: string; // Some Aptaero endpoints use ID
  FlightSegmentID: string; // Others use FlightSegmentID
}

export interface ImportCrewManifestRequest {
  FlightSegmentID: string;  // Required: Aptaero's flight segment GUID to link crew
  CarrierCode: string;
  FlightNo: string;
  ScheduledDepartureDate: string;
  OriginIATA?: string;
  CrewRecordReferenceType?: string;  // "BadgeNo" = match by badge number, not internal ID
  CrewMembers: {
    BadgeNo: string;
    StatusOnBoard: number;
  }[];
}

export class AptaeroClient {
  private baseUrl: string;
  private username: string;
  private password: string;

  constructor(config: AptaeroConnectionConfig) {
    this.baseUrl = config.baseUrl.replace(/\/$/, '');
    this.username = config.username;
    this.password = config.password;
  }

  private getBasicAuthHeader(): string {
    const credentials = Buffer.from(`${this.username}:${this.password}`).toString('base64');
    return `Basic ${credentials}`;
  }

  private sanitizeError(status: number, errorText: string): string {
    if (status === 401) return 'Authentication failed - check username/password';
    if (status === 403) return 'Access denied - insufficient permissions';
    if (status === 404) return 'Endpoint not found';
    if (status >= 500) return 'Aptaero server error - try again later';
    const maxLen = 500;
    const sanitized = errorText.replace(/[\n\r]/g, ' ').substring(0, maxLen);
    return sanitized.length < errorText.length ? sanitized + '...' : sanitized;
  }

  private async request<T>(endpoint: string, body?: object, method: 'POST' | 'PUT' = 'POST'): Promise<T> {
    const url = `${this.baseUrl}${endpoint}`;
    
    const response = await fetch(url, {
      method,
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': this.getBasicAuthHeader(),
      },
      body: body ? JSON.stringify(body) : undefined,
    });

    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`Aptaero API error ${response.status}: ${this.sanitizeError(response.status, errorText)}`);
    }

    return (await response.json()) as T;
  }

  async testConnection(): Promise<{ success: boolean; message: string }> {
    try {
      // Test connection using GetFlightSegments with date range (as per working Azure Function)
      const url = `${this.baseUrl}/rest/public/v1/flightSegments/GetFlightSegments`;
      const today = new Date();
      const dateStr = today.toISOString().split('T')[0];
      
      console.log('Testing Aptaero connection to:', url);
      
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json',
          'Authorization': this.getBasicAuthHeader(),
        },
        body: JSON.stringify({
          ScheduledDepartureStartDate: dateStr,
          ScheduledDepartureEndDate: dateStr,
          FlightNo: "",
        }),
      });

      const responseText = await response.text();
      console.log('Aptaero test response status:', response.status, 'body:', responseText.substring(0, 500));
      
      // 401 means auth failed
      if (response.status === 401) {
        return { success: false, message: 'Authentication failed - check username/password' };
      }
      
      // 403 means access denied
      if (response.status === 403) {
        return { success: false, message: 'Access denied - insufficient permissions' };
      }
      
      // Parse response to check success
      if (response.status === 200) {
        try {
          const data = JSON.parse(responseText);
          if (data.Success === true) {
            const flightCount = data.FlightSegments?.length || 0;
            return { success: true, message: `Aptaero API connection successful (${flightCount} flights found for today)` };
          }
        } catch (e) {
          // JSON parse failed but status was 200
        }
        return { success: true, message: 'Aptaero API connection successful' };
      }
      
      return { success: false, message: `Aptaero API returned status ${response.status}` };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      return { success: false, message: `Aptaero connection failed: ${message}` };
    }
  }

  // Add a flight segment to Aptaero (push from FL3XX)
  async addFlightSegment(flight: AddFlightSegmentRequest): Promise<{ success: boolean; message: string; data?: any }> {
    try {
      const response = await this.request<any>('/rest/public/v1/flightSegments/AddFlightSegment', flight);
      
      if (response.Success === true) {
        return { success: true, message: 'Flight segment added successfully', data: response };
      }
      return { success: false, message: response.Error?.Message || 'Failed to add flight segment' };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      return { success: false, message };
    }
  }

  // Update a flight segment in Aptaero (uses PUT method)
  // Requires FlightSegmentID to identify which flight to update
  async updateFlightSegment(flight: UpdateFlightSegmentRequest): Promise<{ success: boolean; message: string; data?: any }> {
    try {
      // GUARD: Validate that required IDs are present before making the request
      // Without these IDs, Aptaero will return Success=true but not persist any changes
      if (!flight.ID && !flight.FlightSegmentID) {
        console.error('BLOCKED: updateFlightSegment called without ID or FlightSegmentID - this would be a silent no-op');
        return { success: false, message: 'Missing required FlightSegmentID/ID - update blocked to prevent silent no-op' };
      }
      
      // Ensure both ID fields are populated for maximum compatibility
      const payload = {
        ...flight,
        ID: flight.ID || flight.FlightSegmentID,
        FlightSegmentID: flight.FlightSegmentID || flight.ID,
      };
      
      console.log(`Updating flight segment: ID=${payload.ID}, FlightNo=${payload.FlightNo}`);
      const response = await this.request<any>('/rest/public/v1/flightSegments/UpdateFlightSegment', payload, 'PUT');
      
      if (response.Success === true) {
        console.log(`Flight segment ${payload.ID} updated successfully`);
        return { success: true, message: 'Flight segment updated successfully', data: response };
      }
      const errorMsg = response.Error?.Message || 'Failed to update flight segment';
      console.error(`Flight segment ${payload.ID} update failed:`, errorMsg);
      return { success: false, message: errorMsg };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      console.error('updateFlightSegment exception:', message);
      return { success: false, message };
    }
  }

  // Cancel a flight segment in Aptaero (uses PUT method)
  async cancelFlightSegment(flightSegmentId: string, cancellationReason?: string): Promise<{ success: boolean; message: string }> {
    try {
      const response = await this.request<any>('/rest/public/v1/flightSegments/CancelFlightSegment', {
        FlightSegmentID: flightSegmentId,
        CancellationReason: cancellationReason || "",
      }, 'PUT');
      
      if (response.Success === true) {
        return { success: true, message: 'Flight segment cancelled successfully' };
      }
      return { success: false, message: response.Error?.Message || 'Failed to cancel flight segment' };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      return { success: false, message };
    }
  }

  // Import crew manifest to Aptaero
  async importCrewManifest(manifest: ImportCrewManifestRequest): Promise<{ success: boolean; message: string; data?: any }> {
    try {
      console.log('ImportCrewManifest request:', JSON.stringify(manifest, null, 2));
      const response = await this.request<any>('/rest/public/v1/crewmembers/ImportCrewManifest', manifest);
      console.log('ImportCrewManifest response:', JSON.stringify(response, null, 2));
      
      if (response.Success === true) {
        return { success: true, message: 'Crew manifest imported successfully', data: response };
      }
      return { success: false, message: response.Error?.Message || 'Failed to import crew manifest' };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      console.error('ImportCrewManifest error:', message);
      return { success: false, message };
    }
  }

  // Get flight segments from Aptaero (using date range as per working Azure Function)
  // Extended start date to 7 days in the past to include Active flights that may have earlier scheduled dates
  async getFlightSegments(startDate?: string, endDate?: string, flightNo?: string): Promise<AptaeroFlightSegment[]> {
    try {
      const today = new Date();
      const sevenDaysAgo = new Date(today);
      sevenDaysAgo.setDate(today.getDate() - 7);
      const thirtyDaysLater = new Date(today);
      thirtyDaysLater.setDate(today.getDate() + 30);
      
      const start = startDate || sevenDaysAgo.toISOString().split('T')[0];
      const end = endDate || thirtyDaysLater.toISOString().split('T')[0];
      
      const response = await this.request<any>('/rest/public/v1/flightSegments/GetFlightSegments', {
        ScheduledDepartureStartDate: start,
        ScheduledDepartureEndDate: end,
        FlightNo: flightNo || "",
      });
      
      if (!response.Success) {
        console.error('Aptaero API returned error:', response);
        return [];
      }

      const segments = response.FlightSegments || [];
      
      // Log first segment to see actual structure
      if (segments.length > 0) {
        console.log('Sample Aptaero flight segment structure:', JSON.stringify(segments[0], null, 2));
      }
      
      // Return ALL segments with unique Segment IDs - no deduplication
      // Each segment has its own unique ID in Aptaero
      const allFlights: AptaeroFlightSegment[] = [];
      
      for (const seg of segments) {
        const scheduledDep = seg.ScheduledDepartureTime || '';
        let departureDate = '';
        let departureTime = '';
        
        if (scheduledDep) {
          const dateObj = new Date(scheduledDep);
          departureDate = dateObj.toISOString().split('T')[0];
          departureTime = dateObj.toTimeString().substring(0, 5);
        }
        
        // Map numeric status to string: 0=Scheduled, 1=Active, 2=Completed, 3=Cancelled
        const statusNum = seg.Status ?? seg.StatusLabel ?? 0;
        let statusStr = 'scheduled';
        if (statusNum === 1 || statusNum === '1' || String(statusNum).toLowerCase() === 'active') {
          statusStr = 'active';
        } else if (statusNum === 2 || statusNum === '2' || String(statusNum).toLowerCase() === 'completed') {
          statusStr = 'completed';
        } else if (statusNum === 3 || statusNum === '3' || String(statusNum).toLowerCase() === 'cancelled') {
          statusStr = 'cancelled';
        }

        // Parse arrival time (keep raw value for comparison, also extract time-only for display)
        const scheduledArr = seg.ScheduledArrivalTime || '';
        let arrivalTime = '';
        if (scheduledArr) {
          const arrDateObj = new Date(scheduledArr);
          arrivalTime = arrDateObj.toTimeString().substring(0, 5);
        }

        const flightData: AptaeroFlightSegment = {
          id: seg.ID || seg.FlightSegmentID || String(Math.random()),
          flightNumber: seg.FlightNo || 'N/A',
          departureDate,
          departureTime,
          arrivalTime,
          originAirport: seg.OriginIATA || 'N/A',
          destinationAirport: seg.DestinationIATA || 'N/A',
          carrierCode: seg.CarrierCode || 'GI',
          tailNo: seg.TailNo || seg.AircraftRegistration || '',
          capacity: seg.Capacity ?? seg.AircraftCapacity ?? null,
          booked: seg.BookedPassengersCount ?? seg.BookedPassengers ?? seg.PassengerCount ?? null,
          status: statusStr,
          // Type: 1=Public, 2=Private
          type: seg.Type ?? seg.FlightType ?? null,
          typeLabel: seg.TypeLabel ?? seg.FlightTypeName ?? null,
          // Store raw datetime values exactly as returned from Aptaero API (no conversion)
          scheduledDepartureTime: scheduledDep || undefined,
          scheduledArrivalTime: scheduledArr || undefined,
        };

        allFlights.push(flightData);
      }
      
      console.log(`Returning ${allFlights.length} total flight segments (no deduplication)`);
      return allFlights;
    } catch (error) {
      console.error('Error fetching Aptaero flight segments:', error);
      throw error;
    }
  }

  // Get all flight data from Aptaero
  async getAllData(): Promise<AptaeroDataResponse> {
    try {
      const flights = await this.getFlightSegments();
      
      return {
        flights,
        lastSync: new Date().toISOString(),
        connectionStatus: 'connected',
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      return {
        flights: [],
        lastSync: new Date().toISOString(),
        connectionStatus: 'error',
        error: message,
      };
    }
  }

  // Get Master Crew List from Aptaero
  async getMasterCrewMembers(carrierCode: string = 'GI'): Promise<AptaeroMasterCrewMember[]> {
    try {
      const response = await this.request<any>('/rest/public/v1/mastercrewmembers/GetMasterCrewMembers', {
        CarrierCode: carrierCode,
      });
      
      if (!response.Success) {
        console.error('Aptaero GetMasterCrewMembers error:', response);
        return [];
      }

      const members = response.MasterCrewMembers || [];
      
      if (members.length > 0) {
        console.log('Sample Master Crew Member structure:', JSON.stringify(members[0], null, 2));
      }
      
      return members.map((m: any) => {
        const homeAddr = m.HomeAddress || {};
        const birthAddr = m.BirthplaceAddress || {};
        const travelDoc1 = m.TravelDocument1 || {};
        const travelDoc2 = m.TravelDocument2 || {};
        
        return {
          id: m.ID || String(Math.random()),
          externalId: m.ExternalID || '',  // FL3XX pilotId - for stable correlation
          badgeNo: m.BadgeNo || '',
          firstName: m.FirstName || '',
          lastName: m.LastName || '',
          middleName: m.MiddleName || '',
          gender: m.Gender || '',
          weight: m.Weight ? String(m.Weight) : '',
          email: m.Email || '',
          phone: m.Telephone || m.Telephone2 || '',
          dateOfBirth: m.DOB || '',
          nationality: m.Nationality || '',
          residence: m.Residence || '',
          statusOnBoard: m.StatusOnBoard ?? 0,
          isActive: m.IsActive === true || m.IsActive === 1 || m.IsActive === 'true',
          // Home Address
          homeStreet1: homeAddr.Street1 || '',
          homeStreet2: homeAddr.Street2 || '',
          homeCity: homeAddr.City || '',
          homeStateCode: homeAddr.StateCode || '',
          homePostalCode: homeAddr.PostalCode || '',
          homeCountryCode: homeAddr.CountryCode || '',
          // Birthplace Address
          birthplaceCity: birthAddr.City || '',
          birthplaceStateCode: birthAddr.StateCode || '',
          birthplaceCountryCode: birthAddr.CountryCode || '',
          // Travel Document 1 (Passport)
          passportDocNo: travelDoc1.DocNo || '',
          passportDocExpiry: travelDoc1.DocExpiry || '',
          passportDocIssue: travelDoc1.DocIssue || '',
          // Travel Document 2 (Pilot License)
          pilotLicenseDocNo: travelDoc2.DocNo || '',
          pilotLicenseDocIssue: travelDoc2.DocIssue || '',
        };
      });
    } catch (error) {
      console.error('Error fetching Master Crew Members:', error);
      throw error;
    }
  }

  // Get all Master Crew data
  async getMasterCrewData(carrierCode: string = 'GI'): Promise<AptaeroCrewDataResponse> {
    try {
      const masterCrew = await this.getMasterCrewMembers(carrierCode);
      
      return {
        masterCrew,
        lastSync: new Date().toISOString(),
        connectionStatus: 'connected',
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      return {
        masterCrew: [],
        lastSync: new Date().toISOString(),
        connectionStatus: 'error',
        error: message,
      };
    }
  }

  // Get customers from Aptaero (for mapping FL3XX accounts)
  async getCustomers(carrierCode: string = 'GI'): Promise<AptaeroCustomer[]> {
    try {
      const response = await this.request<any>('/rest/public/v1/customers/GetCustomers', {
        CarrierCode: carrierCode,
      });
      
      if (!response.Success) {
        console.error('Aptaero GetCustomers error:', response);
        return [];
      }

      const customers = response.Customers || [];
      console.log(`Found ${customers.length} Aptaero customers`);
      
      return customers.map((c: any) => ({
        id: c.ID || '',
        carrierCode: c.CarrierCode || carrierCode,
        code: c.Code || '',
        name: c.Name || '',
        email: c.Email || '',
      }));
    } catch (error) {
      console.error('Error fetching Aptaero customers:', error);
      return [];
    }
  }

  // Find a customer by name (fuzzy match for FL3XX accountName)
  async findCustomerByName(accountName: string, carrierCode: string = 'GI'): Promise<AptaeroCustomer | null> {
    if (!accountName || accountName.trim() === '') {
      return null;
    }
    
    const customers = await this.getCustomers(carrierCode);
    const normalizedSearch = accountName.toUpperCase().trim();
    
    // Try exact match first
    let match = customers.find(c => c.name.toUpperCase() === normalizedSearch);
    
    // Try contains match if no exact match
    if (!match) {
      match = customers.find(c => 
        c.name.toUpperCase().includes(normalizedSearch) ||
        normalizedSearch.includes(c.name.toUpperCase())
      );
    }
    
    // Try code match
    if (!match) {
      match = customers.find(c => c.code.toUpperCase() === normalizedSearch);
    }
    
    if (match) {
      console.log(`Matched FL3XX account "${accountName}" to Aptaero customer "${match.name}" (${match.code})`);
    } else {
      console.log(`No Aptaero customer match found for FL3XX account "${accountName}"`);
    }
    
    return match || null;
  }

  // Add a new crew member to the Master Crew List
  async addMasterCrewMember(crew: AddMasterCrewMemberRequest): Promise<{ success: boolean; message: string; data?: any }> {
    try {
      console.log('Adding Master Crew Member:', JSON.stringify(crew, null, 2));
      const response = await this.request<any>('/rest/public/v1/mastercrewmembers/AddMasterCrewMember', crew);
      
      if (response.Success === true) {
        console.log('AddMasterCrewMember success:', response);
        return { success: true, message: `Master crew member ${crew.BadgeNo} added successfully`, data: response };
      }
      const errorMsg = response.Error?.Message || response.Message || 'Failed to add master crew member';
      console.error('AddMasterCrewMember failed:', errorMsg, response);
      return { success: false, message: errorMsg };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      console.error('AddMasterCrewMember exception:', message);
      return { success: false, message };
    }
  }

  // Update an existing crew member in the Master Crew List
  async updateMasterCrewMember(crewId: string, crew: AddMasterCrewMemberRequest): Promise<{ success: boolean; message: string; data?: any }> {
    try {
      // GUARD: Validate that crew ID is present before making the request
      // Without the ID, Aptaero will return Success=true but not persist any changes
      if (!crewId || crewId.trim() === '') {
        console.error('BLOCKED: updateMasterCrewMember called without crewId - this would be a silent no-op');
        return { success: false, message: 'Missing required crew ID - update blocked to prevent silent no-op' };
      }
      
      const payload = { ...crew, ID: crewId };
      console.log(`Updating Master Crew Member: ID=${crewId}, Badge=${crew.BadgeNo}, Name=${crew.FirstName} ${crew.LastName}, ExternalID=${crew.ExternalID || 'not set'}`);
      const response = await this.request<any>('/rest/public/v1/mastercrewmembers/UpdateMasterCrewMember', payload, 'PUT');
      
      if (response.Success === true) {
        console.log(`UpdateMasterCrewMember success: ID=${crewId}, Badge=${crew.BadgeNo}`);
        return { success: true, message: `Master crew member ${crew.BadgeNo} updated successfully`, data: response };
      }
      const errorMsg = response.Error?.Message || response.Message || 'Failed to update master crew member';
      console.error(`UpdateMasterCrewMember failed for ID=${crewId}:`, errorMsg, response);
      return { success: false, message: errorMsg };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      console.error(`UpdateMasterCrewMember exception for ID=${crewId}:`, message);
      return { success: false, message };
    }
  }

  // Bulk import Master Crew List - uses ImportMasterCrewList endpoint
  // This endpoint may persist ExternalID field (unlike individual Add/Update endpoints)
  // Payload format per Aptaero documentation:
  // { "CarrierCode": "GI", "MasterCrewList": [{ crew objects with nested HomeAddress, BirthplaceAddress, TravelDocument1, TravelDocument2 }] }
  async importMasterCrewList(crewMembers: AddMasterCrewMemberRequest[], carrierCode: string = 'GI'): Promise<{ 
    success: boolean; 
    message: string; 
    stats?: { 
      total: number;
      newRecords: number;
      changedRecords: number;
      errorRecords: number;
      duplicateRecords: number;
    };
    data?: any;
  }> {
    try {
      if (crewMembers.length === 0) {
        return { success: true, message: 'No crew members to import', stats: { total: 0, newRecords: 0, changedRecords: 0, errorRecords: 0, duplicateRecords: 0 } };
      }

      console.log(`ImportMasterCrewList: Importing ${crewMembers.length} crew members`);
      
      // Build the bulk import payload using exact Aptaero format from documentation:
      // - Top level: CarrierCode + MasterCrewList array
      // - Each crew member must have: CarrierCode, ExternalID, BadgeNo, StatusOnBoard (int), 
      //   LastName, FirstName, DOB (YYYY-MM-DD), Gender (M/F), Nationality, Residence,
      //   HomeAddress (nested), BirthplaceAddress (nested), TravelDocument1 (nested), TravelDocument2 (nested),
      //   Telephone, Email, Notes, IsActive (boolean), FileStatus (int), FileTimestamp
      const payload = {
        CarrierCode: carrierCode,
        MasterCrewList: crewMembers.map(crew => {
          // Extract DOB in YYYY-MM-DD format (strip time component if present)
          const dobFormatted = crew.DOB ? crew.DOB.split('T')[0] : '1950-01-01';
          
          // Extract passport expiry in YYYY-MM-DD format
          const passportExpiry = crew.TravelDocument1?.DocExpiry 
            ? crew.TravelDocument1.DocExpiry.split('T')[0] 
            : '2030-01-01';
          
          return {
            CarrierCode: carrierCode,
            ExternalID: crew.ExternalID || '',
            BadgeNo: crew.BadgeNo || '',
            StatusOnBoard: typeof crew.StatusOnBoard === 'number' ? crew.StatusOnBoard : 2, // int: 1=pilot, 2=FA, 4=other
            LastName: (crew.LastName || '').toUpperCase(),
            FirstName: (crew.FirstName || '').toUpperCase(),
            MiddleName: (crew.MiddleName || '').toUpperCase(),
            DOB: dobFormatted, // YYYY-MM-DD format
            Gender: crew.Gender === 'FEMALE' || crew.Gender === 'F' ? 'F' : 'M',
            Nationality: crew.Nationality || 'USA',
            Residence: crew.Residence || 'USA',
            HomeAddress: {
              Street1: crew.HomeAddress?.Street1 || '',
              Street2: crew.HomeAddress?.Street2 || '',
              City: crew.HomeAddress?.City || '',
              StateCode: crew.HomeAddress?.StateCode || '',
              PostalCode: crew.HomeAddress?.PostalCode || '',
              CountryCode: crew.HomeAddress?.CountryCode || 'USA',
            },
            BirthplaceAddress: {
              Street1: '',
              Street2: '',
              City: crew.BirthplaceAddress?.City || '',
              StateCode: crew.BirthplaceAddress?.StateCode || '',
              PostalCode: '',
              CountryCode: crew.BirthplaceAddress?.CountryCode || 'USA',
            },
            TravelDocument1: {
              DocCode: crew.TravelDocument1?.DocCode || 'P',
              DocNo: crew.TravelDocument1?.DocNo || '',
              DocExpiry: passportExpiry, // YYYY-MM-DD format
              DocIssue: crew.TravelDocument1?.DocIssue || 'USA',
            },
            TravelDocument2: crew.TravelDocument2 ? {
              DocCode: crew.TravelDocument2.DocCode || 'L',
              DocNo: crew.TravelDocument2.DocNo || '',
              DocExpiry: crew.TravelDocument2.DocExpiry ? crew.TravelDocument2.DocExpiry.split('T')[0] : '',
              DocIssue: crew.TravelDocument2.DocIssue || 'USA',
            } : {
              DocCode: 'L',
              DocNo: '',
              DocExpiry: '',
              DocIssue: '',
            },
            Telephone: crew.Telephone || '',
            Telephone2: '',
            Email: crew.Email || '',
            Notes: '',
            IsActive: crew.IsActive !== false, // boolean, not string
            FileStatus: 0, // int
            FileTimestamp: '',
          };
        }),
      };

      console.log('ImportMasterCrewList request (first 2 crew):', JSON.stringify({
        CarrierCode: payload.CarrierCode,
        MasterCrewList: payload.MasterCrewList.slice(0, 2),
        totalCount: payload.MasterCrewList.length,
      }, null, 2));

      // Use the correct endpoint path (case-sensitive)
      const response = await this.request<any>('/rest/public/v1/MasterCrewMembers/ImportMasterCrewList', payload);
      
      console.log('ImportMasterCrewList response:', JSON.stringify(response, null, 2));

      if (response.Success === true) {
        const importStats = response.ManifestImportResponse?.ImportStatistics || response.ImportStatistics || {};
        const stats = {
          total: crewMembers.length,
          newRecords: importStats.NewRecords || 0,
          changedRecords: importStats.ChangedRecords || 0,
          errorRecords: importStats.ErrorRecords || 0,
          duplicateRecords: importStats.DuplicateRecords || 0,
        };
        console.log(`ImportMasterCrewList success: ${stats.newRecords} new, ${stats.changedRecords} changed, ${stats.errorRecords} errors`);
        return { 
          success: true, 
          message: `Imported ${crewMembers.length} crew members: ${stats.newRecords} new, ${stats.changedRecords} changed`,
          stats,
          data: response,
        };
      }

      const errorMsg = response.Error?.Message || response.Message || 'Failed to import master crew list';
      console.error('ImportMasterCrewList failed:', errorMsg, response);
      return { success: false, message: errorMsg, data: response };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      console.error('ImportMasterCrewList exception:', message);
      return { success: false, message };
    }
  }

  // Deactivate a master crew member (set IsActive = false)
  // This effectively removes them from active duty without deleting them
  async deactivateMasterCrewMember(crewId: string, badgeNo: string, carrierCode: string = 'GI'): Promise<{ success: boolean; message: string }> {
    try {
      console.log(`Deactivating Master Crew Member ID=${crewId}, Badge=${badgeNo}`);
      
      // First fetch the full crew data
      const masterCrew = await this.getMasterCrewMembers(carrierCode);
      const crew = masterCrew.find(c => c.id === crewId);
      
      if (!crew) {
        return { success: false, message: `Crew member ${crewId} not found in Aptaero` };
      }
      
      // Build a complete payload with IsActive = false
      const payload = { 
        ID: crewId,
        CarrierCode: carrierCode,
        BadgeNo: crew.badgeNo,
        FirstName: crew.firstName,
        LastName: crew.lastName,
        MiddleName: crew.middleName || '',
        Gender: crew.gender || 'M',
        Email: crew.email || '',
        Telephone: crew.phone || '',
        DOB: crew.dateOfBirth || '1950-01-01',
        Nationality: crew.nationality || 'USA',
        Residence: crew.residence || 'USA',
        StatusOnBoard: crew.statusOnBoard || 4,
        IsActive: false, // THE KEY CHANGE
        HomeAddress: {
          Street1: crew.homeStreet1 || '',
          Street2: crew.homeStreet2 || '',
          City: crew.homeCity || '',
          StateCode: crew.homeStateCode || '',
          PostalCode: crew.homePostalCode || '',
          CountryCode: crew.homeCountryCode || 'USA',
        },
        BirthplaceAddress: {
          City: crew.birthplaceCity || '',
          StateCode: crew.birthplaceStateCode || '',
          CountryCode: crew.birthplaceCountryCode || 'USA',
        },
        TravelDocument1: {
          DocCode: 'P', // Passport
          DocNo: crew.passportDocNo || 'MISSING',
          DocExpiry: crew.passportDocExpiry || '2035-01-01',
          DocIssue: crew.passportDocIssue || 'USA',
        },
        TravelDocument2: {
          DocCode: 'L', // License
          DocNo: crew.pilotLicenseDocNo || '',
          DocIssue: crew.pilotLicenseDocIssue || '',
        },
      };
      
      console.log('Deactivation payload:', JSON.stringify(payload, null, 2));
      
      // CORRECT FORMAT: Include MasterCrewMemberID at root AND the crew data in a MasterCrewMember wrapper
      // This is per Aptaero's error message: "Master Crew Member ID is required!" when wrapped
      const correctPayload = { 
        MasterCrewMemberID: crewId,  // ID at root level for API routing
        ...payload                   // Full crew data with IsActive: false at same level
      };
      console.log('Using FLAT format with MasterCrewMemberID at root...');
      
      const response = await this.request<any>('/rest/public/v1/mastercrewmembers/UpdateMasterCrewMember', correctPayload, 'PUT');
      
      if (response.Success === true) {
        console.log('DeactivateMasterCrewMember success:', response);
        return { success: true, message: `Master crew member ${badgeNo} deactivated successfully` };
      }
      const errorMsg = response.Error?.Message || response.Message || 'Failed to deactivate master crew member';
      console.error('DeactivateMasterCrewMember failed:', errorMsg, response);
      return { success: false, message: errorMsg };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      console.error('DeactivateMasterCrewMember exception:', message);
      return { success: false, message };
    }
  }

  // DELETE a master crew member from Aptaero's Master Crew List
  // This fully removes them (unlike deactivate which just sets IsActive=false)
  // Use this for crew who are no longer in FL3XX's Master Crew List
  async deleteMasterCrewMember(crewId: string, badgeNo: string, carrierCode: string = 'GI'): Promise<{ success: boolean; message: string }> {
    try {
      // GUARD: Validate that crew ID is present
      if (!crewId || crewId.trim() === '') {
        console.error('BLOCKED: deleteMasterCrewMember called without crewId');
        return { success: false, message: 'Missing required crew ID - delete blocked' };
      }
      
      console.log(`Deleting Master Crew Member: ID=${crewId}, Badge=${badgeNo}`);
      
      // Try DELETE method first (most APIs use this)
      const url = `${this.baseUrl}/rest/public/v1/mastercrewmembers/DeleteMasterCrewMember`;
      const payload = { 
        ID: crewId,
        CarrierCode: carrierCode,
        BadgeNo: badgeNo
      };
      
      const response = await fetch(url, {
        method: 'DELETE',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json',
          'Authorization': `Basic ${Buffer.from(`${this.username}:${this.password}`).toString('base64')}`,
        },
        body: JSON.stringify(payload),
      });
      
      if (!response.ok) {
        // If DELETE fails, try POST (some APIs use POST for deletions)
        console.log(`DELETE method returned ${response.status}, trying POST...`);
        const postResponse = await this.request<any>('/rest/public/v1/mastercrewmembers/DeleteMasterCrewMember', payload, 'POST');
        
        if (postResponse.Success === true) {
          console.log(`DeleteMasterCrewMember (POST) success: ID=${crewId}, Badge=${badgeNo}`);
          return { success: true, message: `Master crew member ${badgeNo} deleted successfully` };
        }
        
        // If POST also fails, the endpoint might not exist - fall back to deactivate
        console.warn(`DeleteMasterCrewMember endpoint not available, falling back to deactivate`);
        return await this.deactivateMasterCrewMember(crewId, badgeNo, carrierCode);
      }
      
      const result = (await response.json()) as {
        Success?: boolean;
        Error?: {
          Message?: string;
        };
        Message?: string;
      };

      console.log('DeleteMasterCrewMember response:', result);

      if (result.Success === true) {
        console.log(`DeleteMasterCrewMember success: ID=${crewId}, Badge=${badgeNo}`);
        return { success: true, message: `Master crew member ${badgeNo} deleted successfully` };
      }
      
      const errorMsg = result.Error?.Message || result.Message || 'Failed to delete master crew member';
      console.error(`DeleteMasterCrewMember failed for ID=${crewId}:`, errorMsg);
      
      // If delete fails, fall back to deactivate
      console.log('Falling back to deactivate...');
      return await this.deactivateMasterCrewMember(crewId, badgeNo, carrierCode);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      console.error(`DeleteMasterCrewMember exception for ID=${crewId}:`, message);
      
      // Fall back to deactivate on exception
      console.log('Exception occurred, falling back to deactivate...');
      return await this.deactivateMasterCrewMember(crewId, badgeNo, carrierCode);
    }
  }

  // Activate a master crew member (set IsActive = true)
  // Used to reactivate previously deactivated crew
  async activateMasterCrewMember(crewId: string, badgeNo: string, carrierCode: string = 'GI'): Promise<{ success: boolean; message: string }> {
    try {
      console.log(`Activating Master Crew Member ID=${crewId}, Badge=${badgeNo}`);
      
      // Update with IsActive = true
      const payload = { 
        ID: crewId,
        CarrierCode: carrierCode,
        BadgeNo: badgeNo,
        IsActive: true
      };
      
      const response = await this.request<any>('/rest/public/v1/mastercrewmembers/UpdateMasterCrewMember', payload, 'PUT');
      
      if (response.Success === true) {
        console.log('ActivateMasterCrewMember success:', response);
        return { success: true, message: `Master crew member ${badgeNo} activated successfully` };
      }
      const errorMsg = response.Error?.Message || response.Message || 'Failed to activate master crew member';
      console.error('ActivateMasterCrewMember failed:', errorMsg, response);
      return { success: false, message: errorMsg };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      console.error('ActivateMasterCrewMember exception:', message);
      return { success: false, message };
    }
  }

  // Delete ALL active (non-strikethrough) crew from a flight segment
  // Used for full roster replacement strategy - clear everything, then ImportCrewManifest
  async deleteAllFlightCrew(flightSegmentId: string, carrierCode: string = 'GI'): Promise<{ 
    success: boolean; 
    deleted: string[]; 
    skippedDeleted: string[]; 
    failed: Array<{ badge: string; error: string }> 
  }> {
    console.log(`Deleting ALL crew from flight segment ${flightSegmentId}`);
    
    const deleted: string[] = [];
    const skippedDeleted: string[] = [];
    const failed: Array<{ badge: string; error: string }> = [];
    
    try {
      // Get all crew on this flight
      const crewMembers = await this.getFlightCrewMembers(flightSegmentId, carrierCode);
      console.log(`Found ${crewMembers.length} crew members on flight`);
      
      for (const crew of crewMembers) {
        const badge = crew.badgeNo || 'unknown';
        
        // Skip already-deleted (strikethrough) crew - DeleteCrewMember doesn't work on them
        if (crew.isDeleted) {
          console.log(`Skipping already-deleted crew ${badge}`);
          skippedDeleted.push(badge);
          continue;
        }
        
        // Delete active crew
        const result = await this.deleteCrewMember(flightSegmentId, crew.id, crew);
        if (result.success) {
          deleted.push(badge);
        } else {
          failed.push({ badge, error: result.message });
        }
      }
      
      console.log(`Delete all results: ${deleted.length} deleted, ${skippedDeleted.length} skipped (already deleted), ${failed.length} failed`);
      return { 
        success: failed.length === 0, 
        deleted, 
        skippedDeleted, 
        failed 
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      console.error('deleteAllFlightCrew exception:', message);
      return { success: false, deleted, skippedDeleted, failed: [{ badge: 'all', error: message }] };
    }
  }

  // Delete a crew member from a flight segment
  // Uses DELETE method with query params (matching original Python implementation)
  async deleteCrewMember(flightSegmentId: string, crewMemberId: string, crewMemberData?: any): Promise<{ success: boolean; message: string }> {
    try {
      console.log(`Deleting crew member ${crewMemberId} from flight ${flightSegmentId}`);
      
      // Build URL with query parameters (matching original Python: params={"flightSegmentID": ..., "crewMemberID": ...})
      const url = `${this.baseUrl}/rest/public/v1/crewmembers/DeleteCrewMember?flightSegmentID=${encodeURIComponent(flightSegmentId)}&crewMemberID=${encodeURIComponent(crewMemberId)}`;
      
      const authHeader = this.getBasicAuthHeader();
      
      // Use DELETE method with crew member data as body (matching original Python: data=crew_member)
      const response = await fetch(url, {
        method: 'DELETE',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': authHeader,
        },
        body: crewMemberData ? JSON.stringify(crewMemberData) : undefined,
      });
      
      if (!response.ok) {
        const text = await response.text();
        console.error(`DeleteCrewMember HTTP error ${response.status}:`, text);
        return { success: false, message: `Aptaero API error ${response.status}: ${text}` };
      }
      
      const result = (await response.json()) as {
        Success?: boolean;
        Error?: {
          Message?: string;
        };
        Message?: string;
      };

      console.log('DeleteCrewMember response:', result);

      if (result.Success === true) {
        return { success: true, message: 'Crew member removed from flight' };
      }
      const errorMsg = result.Error?.Message || 'Failed to delete crew member';
      console.error('DeleteCrewMember failed:', errorMsg);
      return { success: false, message: errorMsg };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      console.error('DeleteCrewMember exception:', message);
      return { success: false, message };
    }
  }

  // Undelete a deleted crew member's journey on a flight (flip IsDeleted from true to false)
  // This is step 1 of the 3-step reactivation: undelete → delete → add
  // Uses PUT /rest/public/v1/crewmembers/UpdateCrewMemberJourney with Journey object
  async undeleteCrewMemberJourney(
    crewMemberId: string, 
    flightSegmentId: string, 
    carrierCode: string = 'GI'
  ): Promise<{ success: boolean; message: string; journeyId?: string }> {
    try {
      console.log(`=== UNDELETE: Crew ${crewMemberId} on flight ${flightSegmentId} ===`);
      
      // Get crew member with their journeys
      const crewResponse = await this.request<any>('/rest/public/v1/crewmembers/GetCrewMembers', {
        CarrierCode: carrierCode,
        FlightSegmentID: flightSegmentId,
      });
      
      if (!crewResponse.Success) {
        return { success: false, message: 'Failed to fetch crew members' };
      }
      
      const crewMembers = crewResponse.CrewMembers || [];
      const crewMember = crewMembers.find((m: any) => 
        m.ID === crewMemberId || m.CrewMemberID === crewMemberId
      );
      
      if (!crewMember) {
        return { success: false, message: `Crew member ${crewMemberId} not found on flight` };
      }
      
      // Find the deleted journey for this flight
      const journeys = crewMember.Journeys || [];
      const deletedJourney = journeys.find((j: any) => 
        j.FlightSegmentID === flightSegmentId && j.IsDeleted === true
      );
      
      if (!deletedJourney) {
        console.log('No deleted journey found - crew is already active');
        return { success: true, message: 'Journey is not deleted', journeyId: undefined };
      }
      
      console.log('Found deleted journey:', JSON.stringify(deletedJourney, null, 2));
      
      // Build the EXACT journey payload - only journey fields, IsDeleted=false
      // This is what Aptaero UI sends when clicking "undelete"
      const journeyPayload = {
        ID: deletedJourney.ID,
        CrewMemberID: crewMember.ID,
        FlightSegmentID: flightSegmentId,
        StatusOnBoard: deletedJourney.StatusOnBoard,
        DutyID: deletedJourney.DutyID || null,
        IsDeleted: false,  // THE KEY CHANGE
      };
      
      console.log('Journey payload for undelete:', JSON.stringify(journeyPayload, null, 2));
      
      // Try multiple endpoint variations - Aptaero desktop app uses one of these
      const endpointsToTry = [
        { path: '/rest/public/v1/crewmembers/UndeleteCrewMember', method: 'POST' as const },
        { path: '/rest/public/v1/crewmembers/UndeleteCrewMember', method: 'PUT' as const },
        { path: '/rest/public/v1/crewmemberjourneys/Undelete', method: 'POST' as const },
        { path: '/rest/public/v1/crewmembers/UpdateCrewMemberJourney', method: 'POST' as const },
      ];
      
      for (const endpoint of endpointsToTry) {
        try {
          console.log(`Trying ${endpoint.method} ${endpoint.path}`);
          const response = await this.request<any>(endpoint.path, journeyPayload, endpoint.method);
          console.log(`Response from ${endpoint.path}:`, JSON.stringify(response, null, 2));
          
          if (response.Success === true) {
            console.log(`UNDELETE SUCCEEDED via ${endpoint.path}`);
            return { success: true, message: `Undeleted via ${endpoint.path}`, journeyId: deletedJourney.ID };
          }
        } catch (e: any) {
          console.log(`${endpoint.path} failed:`, e.message || e);
        }
      }
      
      // Try with different payload structure - maybe it needs CrewMemberID at top level
      const altPayload = {
        CrewMemberID: crewMember.ID,
        FlightSegmentID: flightSegmentId,
        JourneyID: deletedJourney.ID,
      };
      
      console.log('Trying alternate payload:', JSON.stringify(altPayload, null, 2));
      
      try {
        const altResponse = await this.request<any>(
          '/rest/public/v1/crewmembers/UndeleteCrewMember',
          altPayload,
          'POST'
        );
        console.log('Alternate payload response:', JSON.stringify(altResponse, null, 2));
        
        if (altResponse.Success === true) {
          return { success: true, message: 'Undeleted with alt payload', journeyId: deletedJourney.ID };
        }
      } catch (e) {
        console.log('Alternate payload also failed');
      }
      
      console.error('All undelete attempts failed');
      return { success: false, message: 'No working undelete endpoint found' };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      console.error('ReactivateCrewMemberJourney exception:', message);
      return { success: false, message };
    }
  }

  // Get Crew Members assigned to a specific flight segment
  // Returns isDeleted status from Journey to detect crew that need reactivation
  async getFlightCrewMembers(flightSegmentId: string, carrierCode: string = 'GI'): Promise<AptaeroFlightCrewMember[]> {
    try {
      const response = await this.request<any>('/rest/public/v1/crewmembers/GetCrewMembers', {
        CarrierCode: carrierCode,
        FlightSegmentID: flightSegmentId,
      });
      
      if (!response.Success) {
        console.error('Aptaero GetCrewMembers error:', response);
        return [];
      }

      const members = response.CrewMembers || [];
      
      if (members.length > 0) {
        console.log('Sample Flight Crew Member structure:', JSON.stringify(members[0], null, 2));
      }
      
      return members.map((m: any) => {
        // Check Journeys array for IsDeleted status on this specific flight segment
        // A crew member can have multiple journeys, we need the one for this flight
        const journeys = m.Journeys || [];
        const journeyForFlight = journeys.find((j: any) => 
          j.FlightSegmentID === flightSegmentId
        );
        const isDeletedOnFlight = journeyForFlight?.IsDeleted === true;
        
        return {
          id: m.ID || m.CrewMemberID || String(Math.random()),
          masterCrewMemberId: m.MasterCrewMemberID || '',
          flightSegmentId: m.FlightSegmentID || flightSegmentId,
          badgeNo: m.BadgeNo || m.EmployeeNumber || '',
          firstName: m.FirstName || '',
          lastName: m.LastName || '',
          gender: m.Gender || '',
          email: m.Email || '',
          statusOnBoard: m.StatusOnBoard ?? 0,
          position: m.Position || m.CrewType || m.Role || '',
          isDeleted: isDeletedOnFlight,  // Track deleted status for reactivation logic
        };
      });
    } catch (error) {
      console.error('Error fetching Flight Crew Members:', error);
      throw error;
    }
  }

  // Get crew for a flight with full response
  async getFlightCrewData(flightSegmentId: string, carrierCode: string = 'GI'): Promise<AptaeroFlightCrewResponse> {
    try {
      const crewMembers = await this.getFlightCrewMembers(flightSegmentId, carrierCode);
      
      return {
        crewMembers,
        flightSegmentId,
        lastSync: new Date().toISOString(),
        connectionStatus: 'connected',
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      return {
        crewMembers: [],
        flightSegmentId,
        lastSync: new Date().toISOString(),
        connectionStatus: 'error',
        error: message,
      };
    }
  }
}

export function createAptaeroClient(baseUrl: string, username: string, password: string): AptaeroClient {
  return new AptaeroClient({ baseUrl, username, password });
}
