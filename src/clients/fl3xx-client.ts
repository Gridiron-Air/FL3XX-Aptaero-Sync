import type { Environment } from "../../shared/schema.js";

export type FlightLifecycleState = 'open' | 'cancelled' | 'closed';

export interface FL3XXFlight {
  id: string;
  flightNumber: string;
  departure: string;
  arrival: string;
  departureTime: string;
  arrivalTime: string;
  // Raw FL3XX time fields - passed through exactly as received from API
  blockOffEstLocal?: string;  // Estimated departure (local)
  blockOnEstLocal?: string;   // Estimated arrival (local)
  blockOffActLocal?: string;  // Actual departure (local)
  blockOnActLocal?: string;   // Actual arrival (local)
  dateFrom?: string;          // Scheduled departure (local)
  dateTo?: string;            // Scheduled arrival (local)
  etd?: string;
  eta?: string;
  realDateIn?: string;        // Actual landing time (when populated, flight has landed)
  aircraft: string;
  status: string;
  flightStatus?: string;
  postFlightClosed?: boolean;
  isCancelled?: boolean;
  lifecycleState: FlightLifecycleState;
  passengers?: number;
  crew?: string[];
  workflowCustomName?: string; // Flight type: Charter, Public, Owner Private, etc.
  accountId?: number;          // FL3XX account ID (customer/operator)
  accountName?: string;        // FL3XX account name (for Aptaero customer mapping)
}

export interface FL3XXAircraft {
  id: string;
  tailNumber: string;
  type: string;
  seats: number;
  homeBase: string;
  status: string;
}

export interface FL3XXCrew {
  id: string;
  pilotId?: number;  // FL3XX crew ID from flight/crew API
  internalId?: string;  // FL3XX User internalId - for supplemental data lookup only
  name: string;
  role: string;
  licenseNumber: string;
  certifications: string[];
  status: string;
  badgeNo?: string;
}

export interface FL3XXConnectionConfig {
  baseUrl: string;
  authToken: string;
}

export interface FL3XXDataResponse {
  flights: FL3XXFlight[];
  crew: FL3XXCrew[];
  aircraft: FL3XXAircraft[];
  lastSync: string;
  connectionStatus: "connected" | "disconnected" | "error";
  error?: string;
}

export class FL3XXClient {
  private baseUrl: string;
  private authToken: string;
  private cachedUserMap: Map<string, any> | null = null;  // Cache users map for supplemental data lookups

  constructor(config: FL3XXConnectionConfig) {
    this.baseUrl = config.baseUrl.replace(/\/$/, '');
    // Only strip trailing newlines/carriage returns - don't trim spaces that might be part of token
    this.authToken = config.authToken.replace(/[\r\n]+$/, '');
  }
  
  // Get or fetch the users map for internalId lookups
  public async getUserMap(): Promise<Map<string, any>> {
    if (!this.cachedUserMap) {
      this.cachedUserMap = await this.getUsers();
    }
    return this.cachedUserMap;
  }
  
  // Clear the cached user map (call when you want fresh data)
  public clearUserCache(): void {
    this.cachedUserMap = null;
  }

  private sanitizeError(status: number, errorText: string): string {
    if (status === 401) return 'Authentication failed - check API key';
    if (status === 403) return 'Access denied - insufficient permissions';
    if (status === 404) return 'Endpoint not found';
    if (status >= 500) return 'FL3XX server error - try again later';
    const maxLen = 100;
    const sanitized = errorText.replace(/[\n\r]/g, ' ').substring(0, maxLen);
    return sanitized.length < errorText.length ? sanitized + '...' : sanitized;
  }

  private async request<T>(endpoint: string, options: RequestInit = {}): Promise<T> {
    const url = `${this.baseUrl}${endpoint}`;
    
    // Debug log for troubleshooting
    console.log(`FL3XX Request: ${url}`);
    console.log(`FL3XX Auth Token (first 10 chars): ${this.authToken.substring(0, 10)}...`);
    
    // Only send X-Auth-Token - exactly matching Python
    const response = await fetch(url, {
      ...options,
      headers: {
        'X-Auth-Token': this.authToken,
        ...options.headers,
      },
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.log(`FL3XX Error Response: ${errorText.substring(0, 200)}`);
      throw new Error(`FL3XX API error ${response.status}: ${this.sanitizeError(response.status, errorText)}`);
    }

    return (await response.json()) as T;
  }

  async testConnection(): Promise<{ success: boolean; message: string }> {
    try {
      await this.request('/api/external/aircraft');
      return { success: true, message: 'FL3XX API connection successful' };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      return { success: false, message: `FL3XX connection failed: ${message}` };
    }
  }

  async getFlights(startDate?: Date, endDate?: Date): Promise<FL3XXFlight[]> {
    try {
      const params = new URLSearchParams();
      params.append('include', 'ALL');
      params.append('timeZone', 'UTC');
      if (startDate) {
        params.append('from', startDate.toISOString().split('T')[0]);
      }
      if (endDate) {
        params.append('to', endDate.toISOString().split('T')[0]);
      }
      
      const queryString = `?${params.toString()}`;
      const response = await this.request<any>(`/api/external/flight/flights${queryString}`);
      
      const flights = Array.isArray(response) ? response : (response.data || response.flights || []);
      
      // Log first flight structure for debugging - capture lifecycle-relevant fields
      if (flights.length > 0) {
        console.log('=== FL3XX RAW FLIGHT DEBUG ===');
        console.log('FL3XX Flight sample keys:', Object.keys(flights[0]).join(', '));
        // Log lifecycle-relevant fields specifically
        const sample = flights[0];
        console.log('FL3XX Status-related fields:', JSON.stringify({
          flightId: sample.flightId,
          status: sample.status,
          flightStatus: sample.flightStatus,
          flightState: sample.flightState,
          statusCategory: sample.statusCategory,
          postFlightClosed: sample.postFlightClosed,
          postFlightClosedAt: sample.postFlightClosedAt,
          archived: sample.archived,
          flagArchived: sample.flagArchived,
          flagCancelled: sample.flagCancelled,
          cancelled: sample.cancelled,
          blockOnActual: sample.blockOnActual,
          blockOnActUTC: sample.blockOnActUTC,
          blockOffActual: sample.blockOffActual,
          blockOffActUTC: sample.blockOffActUTC,
          // Flight type fields - check all possible names
          workflowCustomName: sample.workflowCustomName,
          flightType: sample.flightType,
          type: sample.type,
          category: sample.category,
          serviceType: sample.serviceType,
          charterType: sample.charterType,
        }));
        console.log('=== END FL3XX DEBUG ===');
      }
      
      const now = new Date();
      
      return flights.map((f: any) => {
        // FL3XX actual field mappings based on API response
        const depAirport = f.airportFrom || f.realAirportFrom || 'N/A';
        const arrAirport = f.airportTo || f.realAirportTo || 'N/A';
        
        // Times - use LOCAL times as requested (blockOffEstLocal/blockOnEstLocal)
        const depTime = f.blockOffEstLocal || f.blockOffEstUTC || f.etd || new Date().toISOString();
        const arrTime = f.blockOnEstLocal || f.blockOnEstUTC || f.eta || new Date().toISOString();
        
        // Aircraft registration
        const aircraft = f.registrationNumber || 'N/A';
        
        // Check for cancelled status from various possible fields
        const statusLower = String(f.status || '').toLowerCase();
        const flightStatusLower = String(f.flightStatus || '').toLowerCase();
        const isCancelled = 
          f.cancelled === true || 
          f.flagCancelled === true ||
          statusLower === 'cancelled' || 
          statusLower === 'canceled' ||
          flightStatusLower === 'cancelled' || 
          flightStatusLower === 'canceled';
        
        // Determine if flight is closed using multiple indicators:
        // PRIMARY: flightStatus field - common values: OPEN, CLOSED, COMPLETED, etc.
        // 1. postFlightClosed = true (explicit post-flight completion)
        // 2. postFlightClosedAt has a value (timestamp when closed)
        // 3. blockOnActual or blockOnActUTC exists (flight has landed with actual time)
        // 4. flightState.state is 'CLOSED' or 'ARCHIVED'
        // 5. archived or flagArchived is true
        // 6. Fallback: arrival time is in the past (more than 24h ago)
        const postFlightClosed = f.postFlightClosed === true;
        const hasPostFlightTimestamp = !!f.postFlightClosedAt;
        const hasActualLanding = !!(f.blockOnActual || f.blockOnActUTC);
        const flightStateStr = String(f.flightState?.state || f.flightState || '').toUpperCase();
        const isStateClosedOrArchived = flightStateStr === 'CLOSED' || flightStateStr === 'ARCHIVED';
        const isArchived = f.archived === true || f.flagArchived === true;
        
        // Check flightStatus field for closed indicators
        // FL3XX actual values: "On Block" = landed/closed, "Off Block" = departed, "Scheduled" = open
        const flightStatusUpper = String(f.flightStatus || '').toUpperCase();
        const isFlightStatusClosed = 
          flightStatusUpper === 'CLOSED' || 
          flightStatusUpper === 'COMPLETED' ||
          flightStatusUpper === 'FINISHED' ||
          flightStatusUpper === 'ARCHIVED' ||
          flightStatusUpper === 'POST_FLIGHT' ||
          flightStatusUpper === 'POSTFLIGHT' ||
          flightStatusUpper === 'ON BLOCK' ||  // FL3XX: flight has landed
          flightStatusUpper === 'ONBLOCK';
        
        // Parse arrival time and check if it's more than 24 hours in the past
        const arrivalDate = new Date(arrTime);
        const hoursAgo = (now.getTime() - arrivalDate.getTime()) / (1000 * 60 * 60);
        const isPastFlight = hoursAgo > 24;
        
        // Determine lifecycle state with priority order
        let lifecycleState: FlightLifecycleState = 'open';
        if (isCancelled) {
          lifecycleState = 'cancelled';
        } else if (isFlightStatusClosed || postFlightClosed || hasPostFlightTimestamp || hasActualLanding || isStateClosedOrArchived || isArchived) {
          lifecycleState = 'closed';
        } else if (isPastFlight) {
          // Flights that landed more than 24h ago but don't have explicit closed status
          lifecycleState = 'closed';
        }
        
        // Use original status for display, but lifecycleState for filtering
        const status = f.status || f.flightStatus || 'scheduled';
        
        return {
          id: String(f.flightId || f.id || Math.random()),
          flightNumber: f.flightNumber || f.flightNumberCompany || 'N/A',
          departure: depAirport,
          arrival: arrAirport,
          departureTime: depTime,
          arrivalTime: arrTime,
          // Pass through raw FL3XX LOCAL time fields as requested - no UTC conversion
          blockOffEstLocal: f.blockOffEstLocal || undefined,
          blockOnEstLocal: f.blockOnEstLocal || undefined,
          blockOffActLocal: f.blockOffActLocal || f.realDateOFF || undefined,
          blockOnActLocal: f.blockOnActLocal || f.realDateON || undefined,
          dateFrom: f.dateFrom || undefined,  // Scheduled departure
          dateTo: f.dateTo || undefined,      // Scheduled arrival
          etd: f.etd || undefined,
          eta: f.eta || undefined,
          realDateIn: f.realDateIn || f.realDateON || undefined,  // Actual landing time
          aircraft: aircraft,
          status: status,
          flightStatus: f.flightStatus || undefined,
          postFlightClosed: postFlightClosed,
          isCancelled: isCancelled,
          lifecycleState: lifecycleState,
          passengers: f.paxReferences?.length || 0,
          crew: [],
          // Flight type for Aptaero mapping (Charter, Public, Owner Private, etc.)
          workflowCustomName: f.workflowCustomName || f.flightType || f.type || f.category || f.serviceType || '',
          // Customer/account info for Aptaero customer mapping
          accountId: f.accountId || f.account?.id || undefined,
          accountName: f.accountName || f.account?.name || f.customerName || undefined,
        };
      });
    } catch (error) {
      console.error('Error fetching FL3XX flights:', error);
      throw error;
    }
  }

  async getAircraft(): Promise<FL3XXAircraft[]> {
    try {
      const response = await this.request<any>('/api/external/aircraft');
      
      const aircraft = Array.isArray(response) ? response : (response.data || response.aircraft || []);
      
      return aircraft.map((a: any) => ({
        id: a.id || a.uuid || String(Math.random()),
        tailNumber: a.tailNumber || a.registration || a.tail_number || 'N/A',
        type: a.type || a.aircraftType || a.model || 'N/A',
        seats: a.seats || a.seatCapacity || a.paxCapacity || 0,
        homeBase: a.homeBase || a.base || a.homeAirport || 'N/A',
        status: a.status || 'active',
      }));
    } catch (error) {
      console.error('Error fetching FL3XX aircraft:', error);
      throw error;
    }
  }

  async getCrew(): Promise<FL3XXCrew[]> {
    try {
      // Use correct endpoint path from Python: /api/external/staff/crew
      // Add modifiedSince param like Python does
      const params = new URLSearchParams();
      params.append('modifiedSince', '2025-01-01T00:00');
      
      const response = await this.request<any>(`/api/external/staff/crew?${params.toString()}`);
      
      const crew = Array.isArray(response) ? response : (response.data || response.crew || []);
      
      // Master Crew List identification based on uploaded Python code:
      // Master crew = crew with "Pilot" or "Flight Attendant" in roles.Staff array
      let masterCrewCount = 0;
      let activeMasterCrewCount = 0;
      let pilotCount = 0;
      let activePilotCount = 0;
      let flightAttendantCount = 0;
      let activeFlightAttendantCount = 0;
      let officeCount = 0;
      let otherRolesCount = 0;
      
      crew.forEach((c: any) => {
        const staffRoles = c.roles?.Staff || [];
        const isPilot = staffRoles.includes('Pilot');
        const isFlightAttendant = staffRoles.includes('Flight Attendant');
        const isOffice = staffRoles.includes('Office') || staffRoles.includes('Manager');
        const isActive = String(c.status || '').toUpperCase() === 'ACTIVE';
        
        if (isPilot) {
          pilotCount++;
          if (isActive) activePilotCount++;
        }
        if (isFlightAttendant) {
          flightAttendantCount++;
          if (isActive) activeFlightAttendantCount++;
        }
        if (isOffice) officeCount++;
        if (!isPilot && !isFlightAttendant && !isOffice) otherRolesCount++;
        
        if (isPilot || isFlightAttendant) {
          masterCrewCount++;
          if (isActive) activeMasterCrewCount++;
        }
      });
      
      console.log(`FL3XX Master Crew List Analysis:`);
      console.log(`  - Total crew: ${crew.length}`);
      console.log(`  - Pilots (all): ${pilotCount}, ACTIVE: ${activePilotCount}`);
      console.log(`  - Flight Attendants (all): ${flightAttendantCount}, ACTIVE: ${activeFlightAttendantCount}`);
      console.log(`  - Office/Manager staff: ${officeCount}`);
      console.log(`  - Other roles: ${otherRolesCount}`);
      console.log(`  - Master Crew List (Pilot + FA) - ALL: ${masterCrewCount}, ACTIVE: ${activeMasterCrewCount}`);
      
      // Log crew count before filtering
      console.log(`FL3XX Crew: ${crew.length} total from API`);
      
      // Log all keys from first crew member to find badge and masterCrewList fields
      if (crew.length > 0) {
        console.log('FL3XX Crew ALL keys:', JSON.stringify(Object.keys(crew[0])));
        const sample = crew[0];
        
        // Look for badge-related fields
        const badgeFields = ['badge', 'badgeNo', 'badgeNumber', 'employeeId', 'employeeNo', 'empId', 'empNo', 'staffId', 'staffNo', 'crewId', 'crewNo'];
        badgeFields.forEach(field => {
          if (sample[field] !== undefined) {
            console.log(`FL3XX Found badge field: ${field} = ${sample[field]}`);
          }
        });
        
        // Look for master crew list flag
        const masterCrewFields = ['masterCrewList', 'tsaMasterCrewList', 'isMasterCrew', 'masterCrew', 'onMasterCrewList', 'tsaEnrolled', 'tsa', 'crewListFlag', 'masterFlag'];
        masterCrewFields.forEach(field => {
          if (sample[field] !== undefined) {
            console.log(`FL3XX Found masterCrewList field: ${field} = ${sample[field]}`);
          }
        });
        
        // Count how many have masterCrewList flag set to true (check various field names)
        let masterCrewCount = 0;
        crew.forEach((c: any) => {
          if (c.masterCrewList === true || c.tsaMasterCrewList === true || c.isMasterCrew === true || c.masterCrew === true) {
            masterCrewCount++;
          }
        });
        console.log(`FL3XX Crew with masterCrewList=true: ${masterCrewCount} out of ${crew.length}`);
        
        // Log full first record to find field names
        console.log('FL3XX First crew full:', JSON.stringify(crew[0]).substring(0, 2000));
      }
      
      // Filter for ACTIVE status only and deduplicate by badge number (or pilotId as fallback)
      const seenBadges = new Set<string>();
      const activeCrew = crew.filter((c: any) => {
        // Only include ACTIVE crew members
        const status = String(c.status || '').toUpperCase();
        if (status !== 'ACTIVE') {
          return false;
        }
        
        // Deduplicate by badge number (badgeNo field) or pilotId as fallback
        const badge = c.badgeNo || c.badge || c.badgeNumber || c.employeeId || String(c.pilotId || '');
        if (badge && seenBadges.has(badge)) {
          return false;
        }
        if (badge) {
          seenBadges.add(badge);
        }
        return true;
      });
      
      console.log(`FL3XX Crew: ${activeCrew.length} active after filtering and deduplication`);
      
      return activeCrew.map((c: any) => ({
        id: String(c.pilotId || c.id || c.uuid),
        name: c.name || c.fullName || `${c.firstName || ''} ${c.lastName || ''}`.trim() || 'N/A',
        role: c.role || c.position || c.crewType || 'N/A',
        licenseNumber: c.licenseNumber || c.license || c.atplNumber || 'N/A',
        certifications: c.certifications || c.qualifications || c.ratings || [],
        status: c.status || 'ACTIVE',
      }));
    } catch (error) {
      console.error('Error fetching FL3XX crew:', error);
      throw error;
    }
  }

  /**
   * Get Users from FL3XX - provides address data not available in crew endpoint
   * Based on: /api/external/user from fl3xx_helpers.py
   */
  async getUsers(): Promise<Map<string, any>> {
    try {
      const params = new URLSearchParams();
      params.set('limit', '1000');
      
      console.log(`FL3XX Users Request: ${this.baseUrl}/api/external/user`);
      const response = await this.request<any>(`/api/external/user?${params.toString()}`);
      
      const users = Array.isArray(response) ? response : (response.data || response.users || []);
      console.log(`FL3XX Users: ${users.length} total`);
      
      // Log first user structure for debugging
      if (users.length > 0) {
        console.log('FL3XX User sample keys:', Object.keys(users[0]).join(', '));
        console.log('FL3XX User sample:', JSON.stringify(users[0], null, 2).substring(0, 1000));
      }
      
      // Create lookup map by internalId and ALL tokens in externalReference
      // Per Python code: crew['pilotId'] == user['internalId'] or pilotId in user['externalReference']
      const userMap = new Map<string, any>();
      
      // Debug: Check if internalId and id are different on first user
      if (users.length > 0 && users[0].internalId !== users[0].id) {
        console.log(`FL3XX Users: internalId (${users[0].internalId}) ≠ id (${users[0].id}) - using BOTH as keys`);
      }
      
      users.forEach((u: any) => {
        // Primary key: internalId (the STABLE unique ID)
        if (u.internalId) {
          userMap.set(String(u.internalId), u);
        }
        // Also index by user 'id' if different from internalId (for compatibility)
        if (u.id && u.id !== u.internalId) {
          userMap.set(String(u.id), u);
        }
        // Secondary keys: all tokens from externalReference (comma/semicolon separated)
        // This may include personnelNumber, employeeId, and other identifiers
        if (u.externalReference) {
          const refs = String(u.externalReference).split(/[,;]/);
          refs.forEach((ref: string) => {
            const trimmed = ref.trim();
            if (trimmed) {
              userMap.set(trimmed, u);
            }
          });
        }
        // Also index by personnelNumber if present
        if (u.personnelNumber) {
          userMap.set(String(u.personnelNumber), u);
        }
        // Also index by employeeId if present
        if (u.employeeId) {
          userMap.set(String(u.employeeId), u);
        }
      });
      
      console.log(`FL3XX User map created with ${userMap.size} lookup keys from ${users.length} users`);
      
      return userMap;
    } catch (error) {
      console.error('Error fetching FL3XX users:', error);
      return new Map(); // Return empty map on error, don't fail the whole operation
    }
  }

  /**
   * Get Master Crew List - TSA Compliance Requirement
   * Returns only ACTIVE crew members with Pilot or Flight Attendant roles
   * Merges data from /api/external/staff/crew AND /api/external/user endpoints
   * Based on the uploaded Python code (every_day.py, data_mapping.py)
   */
  async getMasterCrewList(): Promise<{
    masterCrew: any[];
    stats: {
      totalCrew: number;
      activePilots: number;
      activeFlightAttendants: number;
      activeRamp: number;
      masterCrewTotal: number;
      skippedStats: {
        notActive: number;
        noValidRole: number;
      };
    };
  }> {
    try {
      // Fetch both crew and user data in parallel
      const params = new URLSearchParams();
      params.set('modifiedSince', '2025-01-01T00:00');
      
      console.log(`FL3XX Master Crew Request: ${this.baseUrl}/api/external/staff/crew?${params.toString()}`);
      
      const [crewResponse, userMap] = await Promise.all([
        this.request<any>(`/api/external/staff/crew?${params.toString()}`),
        this.getUsers(),
      ]);
      
      const crew = Array.isArray(crewResponse) ? crewResponse : (crewResponse.data || crewResponse.crew || []);
      
      // Log crew member structure for debugging
      if (crew.length > 0) {
        console.log('FL3XX Crew ALL keys:', Object.keys(crew[0]).join(', '));
      }
      
      // Filter for Master Crew List:
      // 1. crew["status"].lower() == "active"
      // 2. roles.Staff must contain at least one of: "Pilot", "Ramp", or "Flight Attendant"
      // Note: Email requirement removed - all active crew with valid roles are included
      const seenIds = new Set<string>();
      
      let activePilots = 0;
      let activeFlightAttendants = 0;
      let activeRamp = 0;
      let skippedNoValidRole = 0;
      let skippedNotActive = 0;
      
      const masterCrew = crew.filter((c: any) => {
        // Check 1: Must be ACTIVE
        const status = String(c.status || '').toLowerCase();
        if (status !== 'active') {
          skippedNotActive++;
          return false;
        }
        
        // Check 2: Must have valid role (Pilot, Flight Attendant, or Ramp)
        const staffRoles = c.roles?.Staff || [];
        const isPilot = staffRoles.includes('Pilot');
        const isFlightAttendant = staffRoles.includes('Flight Attendant');
        const isRamp = staffRoles.includes('Ramp');
        
        if (!isPilot && !isFlightAttendant && !isRamp) {
          skippedNoValidRole++;
          return false;
        }
        
        // Deduplicate by pilotId
        const id = String(c.pilotId || c.id || '');
        if (seenIds.has(id)) return false;
        seenIds.add(id);
        
        // Count by role
        if (isPilot) activePilots++;
        if (isFlightAttendant) activeFlightAttendants++;
        if (isRamp) activeRamp++;
        
        return true;
      });
      
      console.log(`FL3XX Master Crew Filter Stats:`);
      console.log(`  - Skipped (not ACTIVE): ${skippedNotActive}`);
      console.log(`  - Skipped (no valid role): ${skippedNoValidRole}`);
      
      console.log(`FL3XX Master Crew List: ${masterCrew.length} members (${activePilots} Pilots, ${activeFlightAttendants} FAs, ${activeRamp} Ramp)`);
      console.log(`FL3XX Users available for merge: ${userMap.size} lookup keys`);
      
      // Merge user data into crew data (per concat_users_and_master_crew in every_day.py)
      // Match by: pilotId, OR personnelNumber to get supplemental data (address, residence, etc.)
      // NOTE: pilotId is the PRIMARY key for FL3XX - used directly as Aptaero ExternalID
      let matchedByPilotId = 0;
      let matchedByPersonnelNumber = 0;
      let unmatchedCrew: string[] = [];
      
      const mergedCrew = masterCrew.map((c: any) => {
        const pilotId = String(c.pilotId || c.id || '');
        const personnelNumber = String(c.personnelNumber || '');
        
        // Try to find user by pilotId first, then personnelNumber
        let user = userMap.get(pilotId);
        let matchedBy = '';
        
        if (user) {
          matchedBy = 'pilotId';
          matchedByPilotId++;
        } else if (personnelNumber && userMap.has(personnelNumber)) {
          user = userMap.get(personnelNumber);
          matchedBy = 'personnelNumber';
          matchedByPersonnelNumber++;
        }
        
        if (user) {
          // Store user.internalId for validation/monitoring only (not used as ExternalID)
          // FL3XX documentation confirms pilotId is the PRIMARY key for flight crew endpoints
          c._internalId = String(user.internalId || '');
          
          // Merge address from user if crew doesn't have it
          if (!c.address && user.address) {
            c.address = user.address;
          }
          // Merge residence from user's issueCountry
          if (!c.residence && user.issueCountry) {
            c.residence = user.issueCountry;
          }
          // Merge birthPlace from user if crew doesn't have it
          if (!c.birthPlace && user.birthPlace) {
            c.birthPlace = user.birthPlace;
          }
          // Merge birthCountry from user if crew doesn't have it
          if (!c.birthCountry && user.birthCountry) {
            c.birthCountry = user.birthCountry;
          }
          c._userMatchedBy = matchedBy;
        } else {
          // Log unmatched for debugging
          unmatchedCrew.push(`${c.firstName} ${c.lastName} (pilotId=${pilotId}, personnelNumber=${personnelNumber})`);
          c._userMatchedBy = 'none';
        }
        
        return c;
      });
      
      console.log(`FL3XX Crew-User Merge Stats:`);
      console.log(`  - Matched by pilotId: ${matchedByPilotId}`);
      console.log(`  - Matched by personnelNumber: ${matchedByPersonnelNumber}`);
      console.log(`  - Unmatched: ${unmatchedCrew.length}`);
      if (unmatchedCrew.length > 0 && unmatchedCrew.length <= 10) {
        console.log(`  - Unmatched crew: ${unmatchedCrew.join(', ')}`);
      }
      
      // Debug: Show first 3 matched crew with pilotId vs internalId
      const matchedWithInternalId = mergedCrew.filter((c: any) => c._internalId).slice(0, 3);
      if (matchedWithInternalId.length > 0) {
        console.log(`=== PILOTID -> INTERNALID RESOLUTION EXAMPLES ===`);
        matchedWithInternalId.forEach((c: any) => {
          const pilotId = String(c.pilotId || c.id || '');
          const internalId = c._internalId;
          const isDifferent = pilotId !== internalId ? ' [DIFFERENT!]' : ' [SAME]';
          console.log(`  - ${c.firstName} ${c.lastName}: pilotId=${pilotId} -> internalId=${internalId}${isDifferent}`);
        });
        console.log(`=== END INTERNALID RESOLUTION ===`);
      } else {
        console.log(`WARNING: No crew matched with internalId - ExternalID may use pilotId instead!`);
      }
      
      // Map to a standardized format with ALL Aptaero-required fields
      const formattedCrew = mergedCrew.map((c: any) => {
        const staffRoles = c.roles?.Staff || [];
        
        // Get CrewPosition.role code if available (from flight crew assignments)
        // This comes from the crew position on flights, e.g., CMD, FO, FA, MED1, MEDIC, GROUND_INSTRUCTOR
        const crewPositionRole = c.crewPosition?.role || c.role || c.dutyCode || '';
        
        // StatusOnBoard mapping based on CrewPosition.role code:
        // CMD or FO → 1 (Pilot)
        // FA or MED1 or MEDIC → 2 (Flight Attendant/Medical)
        // GROUND_INSTRUCTOR → 4 (Other)
        // Fallback to Staff roles if no CrewPosition.role
        let statusOnBoard = 4; // Default to Other
        const roleCode = crewPositionRole.toUpperCase();
        
        if (roleCode === 'CMD' || roleCode === 'FO') {
          statusOnBoard = 1;
        } else if (roleCode === 'FA' || roleCode === 'MED1' || roleCode === 'MEDIC') {
          statusOnBoard = 2;
        } else if (roleCode === 'GROUND_INSTRUCTOR') {
          statusOnBoard = 4;
        } else if (staffRoles.includes('Pilot')) {
          statusOnBoard = 1;
        } else if (staffRoles.includes('Flight Attendant')) {
          statusOnBoard = 2;
        }
        
        // Store the role code for display - use the filtering criteria roles
        let roleCodeDisplay = 'Other';
        if (staffRoles.includes('Pilot')) {
          roleCodeDisplay = 'Pilot';
        } else if (staffRoles.includes('Flight Attendant')) {
          roleCodeDisplay = 'Flight Attendant';
        } else if (staffRoles.includes('Ramp')) {
          roleCodeDisplay = 'Ramp';
        }
        
        // Email priority: direct email field > logName (if it looks like email) > account.email
        const directEmail = c.email || '';
        const logNameEmail = (c.logName && c.logName.includes('@')) ? c.logName : '';
        const accountEmail = c.account?.email || '';
        const email = directEmail || logNameEmail || accountEmail;
        
        // Extract passport from idCards array
        // Prefer passport where issue country matches nationality
        const idCards = c.idCards || [];
        let passport: any = null;
        let pilotLicense: any = null;
        const nationality = c.nationality || '';
        
        // First pass: find pilot license (don't break early)
        for (const card of idCards) {
          const cardType = String(card.type || '').toLowerCase();
          if (cardType === 'pilot_license' || cardType === 'license' || cardType === 'l') {
            pilotLicense = card;
          }
        }
        
        // Second pass: find passport (prefer one matching nationality)
        for (const card of idCards) {
          const cardType = String(card.type || '').toLowerCase();
          if (cardType === 'passport' || cardType === 'p') {
            if (!passport) {
              passport = card;
            }
            if (nationality && card.issueCountry && card.issueCountry.toUpperCase() === nationality.toUpperCase()) {
              passport = card; // Override with matching passport
              break; // Can break now since pilot license already found
            }
          }
        }
        // If no passport found, use first card as fallback
        if (!passport && idCards.length > 0) {
          passport = idCards[0];
        }
        
        // Extract home address - prioritize Home type, fallback to any other type
        // FL3XX may return single address object or addresses array
        const addresses = c.addresses || (c.address ? [c.address] : []);
        
        // Priority: 1) Home address, 2) Any other address type (Other, Work, etc.)
        let selectedAddress: any = null;
        for (const addr of addresses) {
          const addrType = String(addr.type || '').toUpperCase();
          if (addrType === 'HOME') {
            selectedAddress = addr;
            break; // Home found, use it
          }
          if (!selectedAddress) {
            selectedAddress = addr; // Store first non-Home as fallback
          }
        }
        // If no addresses array, try single address field
        if (!selectedAddress && c.address) {
          selectedAddress = c.address;
        }
        
        const homeAddress = selectedAddress ? {
          street1: selectedAddress.street || selectedAddress.street1 || selectedAddress.addressLine1 || '',
          city: selectedAddress.city || '',
          stateCode: selectedAddress.state || selectedAddress.stateCode || selectedAddress.province || '',
          postalCode: selectedAddress.zip || selectedAddress.postalCode || selectedAddress.zipCode || '',
          countryCode: selectedAddress.country || selectedAddress.countryCode || '',
          _addressType: selectedAddress.type || 'unknown', // Track which type was used
        } : null;
        
        // Residence from id_cards issueCountry (per data_mapping.py get_issue_country)
        const residence = c.residence || (idCards[0]?.issueCountry) || '';
        
        // CRITICAL: Use pilotId directly as the ExternalID for Aptaero
        // FL3XX documentation confirms: pilotId is the PRIMARY key for flight crew assignments
        // The /flight/{flightId}/crew endpoints return pilotId, NOT internalId
        // Using pilotId ensures crew manifest assignments match the MCL ExternalID
        const externalId = String(c.pilotId || c.id || '');
        
        // Validation: Log if internalId differs from pilotId (for monitoring)
        if (c._internalId && c._internalId !== externalId) {
          console.warn(`Note: internalId (${c._internalId}) differs from pilotId (${externalId}) for ${c.firstName} ${c.lastName}`);
        }
        
        return {
          id: externalId,  // This will be used as Aptaero ExternalID
          badgeNo: c.personnelNumber || c.badgeNo || String(c.pilotId || ''),
          firstName: c.firstName || '',
          lastName: c.lastName || '',
          middleName: c.middleName || '',
          gender: c.gender || '',
          weight: c.weight ? String(Math.round(parseFloat(c.weight) * 2.20462)) : '', // Convert FL3XX weight from KG to lbs
          email,
          phone: c.phone || c.mobile || '',
          roleCode: roleCodeDisplay,
          statusOnBoard,
          roles: staffRoles,
          isActive: true,
          status: 'ACTIVE',
          
          // Additional Aptaero-required fields
          dob: c.birthDate || c.dateOfBirth || '',
          nationality: c.nationality || '',
          residence: residence,
          birthCountry: c.birthCountry || '',
          birthCity: c.birthCity || '',
          birthPlace: c.birthPlace || '', // Maps to BirthplaceAddress.City (FL3XX stores city name here)
          
          // Home Address
          homeAddress,
          
          // Travel Document 1 (Passport)
          passport: passport ? {
            docCode: String(passport.type || 'P')[0].toUpperCase(),
            docNo: passport.number || '',
            docExpiry: passport.expirationDate || passport.expiry || '',
            docIssue: passport.issueCountry || '',
          } : null,
          
          // Travel Document 2 (Pilot License) - only for pilots
          pilotLicense: pilotLicense ? {
            docCode: 'L',
            docNo: pilotLicense.number || '',
            docIssue: pilotLicense.issueCountry || '',
          } : null,
          
          // Raw data for debugging
          _raw: {
            hasIdCards: idCards.length > 0,
            idCardTypes: idCards.map((card: any) => card.type),
            hasAddress: !!selectedAddress,
            addressType: selectedAddress?.type || 'none',
            addressCount: addresses.length,
          },
        };
      });
      
      return {
        masterCrew: formattedCrew,
        stats: {
          totalCrew: crew.length,
          activePilots,
          activeFlightAttendants,
          activeRamp,
          masterCrewTotal: masterCrew.length,
          skippedStats: {
            notActive: skippedNotActive,
            noValidRole: skippedNoValidRole,
          }
        }
      };
    } catch (error) {
      console.error('Error fetching FL3XX Master Crew List:', error);
      throw error;
    }
  }

  async getFlightCrew(flightId: string): Promise<FL3XXCrew[]> {
    try {
      // Use correct endpoint path from Python: /api/external/flight/{flightId}/crew
      const response = await this.request<any>(`/api/external/flight/${flightId}/crew`);
      
      // Response has "crews" array according to Python code
      const crews = response.crews || response.crew || (Array.isArray(response) ? response : []);
      
      // Log first crew member structure for debugging
      if (crews.length > 0) {
        console.log('FL3XX Flight Crew sample keys:', Object.keys(crews[0]));
        console.log('FL3XX Flight Crew sample:', JSON.stringify(crews[0], null, 2).substring(0, 500));
      }
      
      // Get cached user map for supplemental data (address, etc.)
      // NOTE: pilotId is the PRIMARY key per FL3XX docs - used directly for ExternalID
      // We use the user map to enrich crew data, NOT to change the ExternalID
      const userMap = await this.getUserMap();
      
      return crews.map((c: any) => {
        const pilotId = String(c.pilotId || '');
        const personnelNumber = String(c.personnelNumber || '');
        
        // Look up user by pilotId (should match user.internalId) or personnelNumber
        let user = userMap.get(pilotId);
        if (!user && personnelNumber) {
          user = userMap.get(personnelNumber);
        }
        
        // pilotId is the PRIMARY key per FL3XX docs - used directly for ExternalID
        // internalId is stored for monitoring/validation only
        const internalId = user ? String(user.internalId || '') : '';
        
        return {
          id: c.id || c.crewId || c.uuid || String(Math.random()),
          pilotId: c.pilotId,  // PRIMARY key - used for Aptaero ExternalID
          internalId: internalId || pilotId,  // For monitoring only
          name: c.name || c.fullName || `${c.firstName || ''} ${c.lastName || ''}`.trim() || 'N/A',
          role: c.role || c.position || c.crewType || c.dutyCode || 'Crew',
          licenseNumber: c.licenseNumber || c.license || c.atplNumber || '',
          certifications: c.certifications || c.qualifications || c.ratings || [],
          status: c.status || 'active',
          badgeNo: c.personnelNumber || c.badgeNo || c.pilotId?.toString() || '',
          gender: c.gender || '',
          email: c.email || '',
        };
      });
    } catch (error) {
      console.error(`Error fetching FL3XX flight crew for flight ${flightId}:`, error);
      throw error;
    }
  }

  async getAllData(): Promise<FL3XXDataResponse> {
    const now = new Date();
    // FL3XX API has a limit on flights per request - paginate by 3-month chunks
    // Fetch 1 year back and 1 year forward in chunks, then combine
    const chunks: { start: Date; end: Date }[] = [];
    const chunkSize = 90 * 24 * 60 * 60 * 1000; // 90 days in ms
    
    // 1 year back in 3-month chunks
    for (let i = 4; i > 0; i--) {
      const end = new Date(now.getTime() - (i - 1) * chunkSize);
      const start = new Date(now.getTime() - i * chunkSize);
      chunks.push({ start, end });
    }
    // 1 year forward in 3-month chunks
    for (let i = 0; i < 4; i++) {
      const start = new Date(now.getTime() + i * chunkSize);
      const end = new Date(now.getTime() + (i + 1) * chunkSize);
      chunks.push({ start, end });
    }

    // Fetch all flight chunks in parallel
    const flightPromises = chunks.map(chunk => this.getFlights(chunk.start, chunk.end));
    const flightResults = await Promise.allSettled(flightPromises);
    
    // Combine all successful flight results
    const allFlights: FL3XXFlight[] = [];
    const flightErrors: string[] = [];
    const seenIds = new Set<string>();
    
    for (const result of flightResults) {
      if (result.status === 'fulfilled') {
        for (const flight of result.value) {
          if (!seenIds.has(flight.id)) {
            seenIds.add(flight.id);
            allFlights.push(flight);
          }
        }
      } else {
        flightErrors.push(result.reason?.message || 'Failed');
      }
    }

    const otherResults = await Promise.allSettled([
      this.getCrew(),
      this.getAircraft(),
    ]);

    const crew = otherResults[0].status === 'fulfilled' ? otherResults[0].value : [];
    const aircraft = otherResults[1].status === 'fulfilled' ? otherResults[1].value : [];

    const errors: string[] = [];
    if (flightErrors.length > 0) errors.push(`Flights: ${flightErrors.join(', ')}`);
    if (otherResults[0].status === 'rejected') errors.push(`Crew: ${otherResults[0].reason?.message || 'Failed'}`);
    if (otherResults[1].status === 'rejected') errors.push(`Aircraft: ${otherResults[1].reason?.message || 'Failed'}`);

    // Log flight counts by lifecycle
    const openCount = allFlights.filter(f => f.lifecycleState === 'open').length;
    const cancelledCount = allFlights.filter(f => f.lifecycleState === 'cancelled').length;
    const closedCount = allFlights.filter(f => f.lifecycleState === 'closed').length;
    console.log(`FL3XX Flights Total: ${allFlights.length} (Open: ${openCount}, Cancelled: ${cancelledCount}, Closed: ${closedCount})`);

    const hasAnyData = allFlights.length > 0 || crew.length > 0 || aircraft.length > 0;
    const hasAllErrors = allFlights.length === 0 && crew.length === 0 && aircraft.length === 0;

    return {
      flights: allFlights,
      crew,
      aircraft,
      lastSync: now.toISOString(),
      connectionStatus: hasAllErrors ? 'error' : 'connected',
      error: errors.length > 0 ? errors.join('; ') : undefined,
    };
  }
}

export function createFL3XXClient(baseUrl: string, authToken: string): FL3XXClient {
  return new FL3XXClient({ baseUrl, authToken });
}
