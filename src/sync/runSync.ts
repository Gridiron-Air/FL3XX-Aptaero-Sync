import type { Environment } from "../../shared/schema.js";
import { storage, initializeStorage } from "../storage/storage.js";
import { createFL3XXClient } from "../clients/fl3xx-client.js";
import {
  createAptaeroClient,
  type AddMasterCrewMemberRequest
} from "../clients/aptaero-client.js";
import {
  createAzureTableClient,
  type FlightMapping
} from "../storage/azure-table-client.js";
import {
  buildAptaeroFlightPayload,
  buildCrewManifestPayload
} from "../transformations/data-transformations.js";
import type { RunSyncOptions, SyncResult } from "./types.js";

let storageInitialized = false;

async function ensureStorageInitialized(): Promise<void> {
  if (!storageInitialized) {
    await initializeStorage();
    storageInitialized = true;
  }
}

// Helper to convert 2-letter country code to 3-letter.
function toAlpha3Country(code: string): string {
  const map: Record<string, string> = {
    US: "USA",
    CA: "CAN",
    MX: "MEX",
    GB: "GBR",
    UK: "GBR",
    FR: "FRA",
    DE: "DEU",
    IT: "ITA",
    ES: "ESP",
    BR: "BRA",
    AU: "AUS",
    JP: "JPN",
    CN: "CHN",
    IN: "IND",
    RU: "RUS",
    KR: "KOR",
    NL: "NLD",
    BE: "BEL",
    CH: "CHE",
    AT: "AUT",
    SE: "SWE",
    NO: "NOR",
    DK: "DNK",
    FI: "FIN",
    IE: "IRL",
    PL: "POL",
    PT: "PRT",
    GR: "GRC",
    TR: "TUR",
    ZA: "ZAF",
    AR: "ARG",
    CL: "CHL",
    CO: "COL",
    PE: "PER",
    VE: "VEN"
  };

  const upper = (code || "").toUpperCase().trim();
  if (upper.length === 3) return upper;
  return map[upper] || upper;
}

const MISSING_DATE_PAST = "1950-01-01";
const MISSING_DATE_PAST_ISO = "1950-01-01T00:00:00";
const MISSING_DATE_FUTURE = "2035-01-01";
const MISSING_DATE_FUTURE_ISO = "2035-01-01T00:00:00";
const MAX_PASSPORT_EXPIRY_YEAR = 2050;
const DEFAULT_POSTAL_CODE = "75001";

function normalizePostalCode(postalCode: string | undefined): string {
  if (!postalCode) return DEFAULT_POSTAL_CODE;
  const base = postalCode.split("-")[0].trim();
  return base || DEFAULT_POSTAL_CODE;
}

function validatePassportExpiry(expiryDate: string | undefined): string {
  if (!expiryDate) return MISSING_DATE_FUTURE_ISO;

  try {
    const date = new Date(expiryDate);
    if (Number.isNaN(date.getTime())) return MISSING_DATE_FUTURE_ISO;

    if (date.getFullYear() > MAX_PASSPORT_EXPIRY_YEAR) {
      return `${MAX_PASSPORT_EXPIRY_YEAR}-12-31T00:00:00`;
    }

    return `${expiryDate}T00:00:00`;
  } catch {
    return MISSING_DATE_FUTURE_ISO;
  }
}

interface CrewPayloadResult {
  payload: AddMasterCrewMemberRequest;
  defaultedFields: string[];
}

/**
 * Copy this function body exactly from the old server/routes.ts:
 *
 * function buildMasterCrewPayloadWithDefaults(fl3xxCrew: any, carrierCode: string = 'GI'): CrewPayloadResult {
 *   ...
 * }
 *
 * It starts near the top of routes.ts and is required by the sync.
 */
function buildMasterCrewPayloadWithDefaults(fl3xxCrew: any, carrierCode: string = 'GI'): CrewPayloadResult {
  const defaultedFields: string[] = [];
  
  const badgeNo = fl3xxCrew.badgeNo || '';
  const statusOnBoard = fl3xxCrew.statusOnBoard || 4; // Default: 4=Other
  const isPilot = statusOnBoard === 1;
  
  // Helper to get nationality/residence with default
  const getNationalityOrDefault = (value: string | undefined, fieldName: string): string => {
    const converted = toAlpha3Country(value || '');
    if (!converted) {
      defaultedFields.push(fieldName);
      return 'USA'; // Default to USA
    }
    return converted;
  };
  
  // DOB - use default date if missing (past date since it's a birth date)
  let dob = fl3xxCrew.dob;
  if (!dob) {
    defaultedFields.push('DOB');
    dob = MISSING_DATE_PAST;
  }
  
  // Nationality (Residence is NOT required - derived from home address country if needed)
  const nationality = getNationalityOrDefault(fl3xxCrew.nationality, 'Nationality');
  const residence = toAlpha3Country(fl3xxCrew.residence || '') || 'USA'; // Default without flagging as missing
  
  // Home Address - validate Address.Type == "HOME" and check required fields
  // Required: street, city, zip, country
  // State is ONLY required if country == "US"
  const homeAddr = fl3xxCrew.homeAddress || {};
  const isHomeAddressType = (homeAddr._addressType || '').toUpperCase() === 'HOME';
  let homeAddress: AddMasterCrewMemberRequest['HomeAddress'];
  
  // Determine if this is a US address
  const homeCountryCode = (homeAddr.countryCode || '').toUpperCase();
  const isUSAddress = homeCountryCode === 'US' || homeCountryCode === 'USA';
  
  // Validate required fields
  const hasStreet = !!homeAddr.street1;
  const hasCity = !!homeAddr.city;
  const hasZip = !!homeAddr.postalCode;
  const hasCountry = !!homeAddr.countryCode;
  const hasState = !!homeAddr.stateCode;
  
  // State is only required for US addresses
  const stateValid = isUSAddress ? hasState : true;
  const homeAddressComplete = isHomeAddressType && hasStreet && hasCity && hasZip && hasCountry && stateValid;
  
  if (!homeAddressComplete) {
    defaultedFields.push('HomeAddress');
    // Track specific missing components for detailed reporting
    if (!hasStreet) defaultedFields.push('HomeAddress.Street');
    if (!hasCity) defaultedFields.push('HomeAddress.City');
    if (!hasZip) defaultedFields.push('HomeAddress.Zip');
    if (!hasCountry) defaultedFields.push('HomeAddress.Country');
    if (isUSAddress && !hasState) defaultedFields.push('HomeAddress.State');
  }
  
  // Build the address with defaults where needed
  homeAddress = {
    Street1: homeAddr.street1 || 'Needs validation',
    City: homeAddr.city || 'Missing',
    StateCode: homeAddr.stateCode || 'TX',
    PostalCode: normalizePostalCode(homeAddr.postalCode),
    CountryCode: toAlpha3Country(homeAddr.countryCode || '') || 'USA',
  };
  
  // Birthplace Address
  const birthCountry = fl3xxCrew.birthCountry;
  const birthCity = fl3xxCrew.birthPlace || fl3xxCrew.birthCity;
  let birthplaceAddress: AddMasterCrewMemberRequest['BirthplaceAddress'];
  
  if (!birthCountry && !birthCity) {
    defaultedFields.push('BirthplaceAddress');
    birthplaceAddress = {
      CountryCode: 'USA',
      City: 'Missing',
    };
  } else {
    if (!birthCountry) defaultedFields.push('BirthCountry');
    if (!birthCity) defaultedFields.push('BirthCity');
    birthplaceAddress = {
      CountryCode: toAlpha3Country(birthCountry || '') || 'USA',
      City: birthCity || 'Missing',
    };
  }
  
  // Passport (TravelDocument1) - required by Aptaero
  const passport = fl3xxCrew.passport || {};
  let travelDocument1: AddMasterCrewMemberRequest['TravelDocument1'];
  
  if (!passport.docNo) {
    defaultedFields.push('Passport');
    travelDocument1 = {
      DocCode: 'P',
      DocNo: 'MISSING',
      DocExpiry: MISSING_DATE_FUTURE_ISO,  // Far future for expiry
      DocIssue: 'USA',
    };
  } else {
    if (!passport.docExpiry) defaultedFields.push('PassportExpiry');
    if (!passport.docIssue) defaultedFields.push('PassportIssue');
    
    // Validate and cap passport expiry (handles invalid dates like 2330-12-31)
    const validatedExpiry = validatePassportExpiry(passport.docExpiry);
    if (passport.docExpiry && validatedExpiry !== `${passport.docExpiry}T00:00:00`) {
      defaultedFields.push('PassportExpiryCapped');
    }
    
    travelDocument1 = {
      DocCode: passport.docCode || 'P',
      DocNo: passport.docNo,
      DocExpiry: validatedExpiry,
      DocIssue: toAlpha3Country(passport.docIssue || '') || 'USA',
    };
  }
  
  // Pilot License (TravelDocument2) - only for pilots
  let travelDocument2: AddMasterCrewMemberRequest['TravelDocument2'];
  const pilotLicense = fl3xxCrew.pilotLicense || {};
  
  if (isPilot) {
    if (!pilotLicense.docNo) {
      defaultedFields.push('PilotLicense');
      travelDocument2 = {
        DocCode: 'L',
        DocNo: 'MISSING',
        DocIssue: 'USA',
      };
    } else {
      if (!pilotLicense.docIssue) defaultedFields.push('PilotLicenseIssue');
      travelDocument2 = {
        DocCode: 'L',
        DocNo: pilotLicense.docNo,
        DocIssue: toAlpha3Country(pilotLicense.docIssue || '') || 'USA',
      };
    }
  }
  
  // ExternalID = FL3XX pilotId for stable correlation
  // FL3XX docs confirm: pilotId is the PRIMARY key for flight crew assignments
  // This enables crew manifest matching to work with MCL ExternalID
  const externalId = String(fl3xxCrew.id || '');
  
  const payload: AddMasterCrewMemberRequest = {
    CarrierCode: carrierCode,
    ExternalID: externalId || undefined,  // FL3XX pilotId for stable MCL correlation
    BadgeNo: badgeNo,
    FirstName: fl3xxCrew.firstName || 'Missing',
    LastName: fl3xxCrew.lastName || 'Missing',
    MiddleName: fl3xxCrew.middleName || undefined,
    Gender: fl3xxCrew.gender === 'MALE' ? 'M' : fl3xxCrew.gender === 'FEMALE' ? 'F' : 'M',
    Email: fl3xxCrew.email || '',
    Weight: fl3xxCrew.weight ? parseInt(fl3xxCrew.weight) : undefined,
    StatusOnBoard: statusOnBoard,
    IsActive: fl3xxCrew.isActive !== false,
    DOB: dob,
    Nationality: nationality,
    Residence: residence,
    HomeAddress: homeAddress,
    BirthplaceAddress: birthplaceAddress,
    TravelDocument1: travelDocument1,
    TravelDocument2: travelDocument2,
  };
  
  return { payload, defaultedFields };
}

export async function runFl3xxAptaeroSync(options: RunSyncOptions): Promise<SyncResult> {
  await ensureStorageInitialized();

  const environment: Environment = options.environment;

  try {
    if (environment === "production" && !options.confirmProduction) {
      return {
        success: false,
        message: "Production sync requires explicit confirmation.",
        environment,
        error: "Production sync requires explicit confirmation."
      };
    }

    const fl3xxApiKey = process.env.FL3XX_API_KEY;
    const aptaeroUsername = process.env.APTAERO_USERNAME;
    const aptaeroPassword = process.env.APTAERO_PASSWORD;

    if (!fl3xxApiKey || !aptaeroUsername || !aptaeroPassword) {
      const missing: string[] = [];
      if (!fl3xxApiKey) missing.push("FL3XX_API_KEY");
      if (!aptaeroUsername) missing.push("APTAERO_USERNAME");
      if (!aptaeroPassword) missing.push("APTAERO_PASSWORD");

      await storage.createLog({
        eventType: options.triggeredBy === "timer" ? "Automatic Sync" : "Manual Sync",
        status: "error",
        source: "system",
        environment,
        details: `Sync failed: Missing API credentials: ${missing.join(", ")}`,
        metadata: { missingCredentials: missing }
      });

      return {
        success: false,
        message: `Missing API credentials: ${missing.join(", ")}`,
        environment,
        error: `Missing API credentials: ${missing.join(", ")}`
      };
    }

    const syncEventType = options.triggeredBy === "timer" ? "Automatic Sync" : "Manual Sync";

    await storage.createLog({
      eventType: syncEventType,
      status: "syncing",
      source: "system",
      environment,
      details: `${syncEventType} started (${environment})`,
      metadata: { triggeredBy: options.triggeredBy, source: options.source }
    });

    const fl3xxBaseUrl = process.env.FL3XX_BASE_URL || "https://app.fl3xx.us";
    const aptaeroBaseUrl =
      process.env.APTAERO_BASE_URL || "https://api-5cc.public.aptaero.com";
    const azureTableConnectionString = process.env.AZURE_TABLE_STORAGE_CONNECTION_STRING;

    const fl3xxClient = createFL3XXClient(fl3xxBaseUrl, fl3xxApiKey);
    const aptaeroClient = createAptaeroClient(
      aptaeroBaseUrl,
      aptaeroUsername,
      aptaeroPassword
    );
    const azureTableClient = azureTableConnectionString
      ? createAzureTableClient(azureTableConnectionString)
      : null;

    console.log("=== FL3XX → APTAERO SYNC STARTED ===");
    console.log(`Environment: ${environment}`);
    console.log(`Triggered by: ${options.triggeredBy}`);
    console.log(`Allow new flights: ${options.allowNewFlights}`);

    // ============= PHASE 1: Master Crew List Sync =============
    // Uses bulk ImportMasterCrewList for efficient sync with ExternalID persistence
    // Bulk import performs FULL REPLACE: crew not in payload are deactivated
    console.log('=== PHASE 1: Master Crew List Sync (Bulk Import) ===');
    
    let crewAdded = 0, crewUpdated = 0, crewDeactivated = 0, crewErrors = 0;
    
    try {
      // Get FL3XX Master Crew
      const fl3xxResponse = await fl3xxClient.getMasterCrewList();
      const fl3xxMasterCrew = fl3xxResponse.masterCrew;
      console.log(`FL3XX Master Crew: ${fl3xxMasterCrew.length} members`);
      
      // Get current Aptaero crew count for comparison
      const aptaeroMasterCrewBefore = await aptaeroClient.getMasterCrewMembers('GI');
      const activeCrewBefore = aptaeroMasterCrewBefore.filter(c => c.isActive).length;
      console.log(`Aptaero Active Crew (before): ${activeCrewBefore}`);
      
      // Build payloads for ALL FL3XX crew
      const crewPayloads = fl3xxMasterCrew.map(fl3xxCrew => {
        const { payload } = buildMasterCrewPayloadWithDefaults(fl3xxCrew, 'GI');
        payload.IsActive = true;
        return payload;
      });
      
      console.log(`Sending ${crewPayloads.length} crew to Aptaero bulk import...`);
      
      // Call bulk import - this does a FULL REPLACE
      const result = await aptaeroClient.importMasterCrewList(crewPayloads, 'GI');
      
      if (result.success && result.data?.ImportStatisticsResponse) {
        const stats = result.data.ImportStatisticsResponse;
        crewAdded = stats.NewRecords || 0;
        crewUpdated = stats.ChangedRecords || 0;
        crewDeactivated = stats.DeletedRecords || 0;
        crewErrors = stats.ErrorRecords || 0;
        
        console.log(`Bulk import result: ${crewAdded} new, ${crewUpdated} changed, ${crewDeactivated} deactivated, ${crewErrors} errors`);
        
        // Update Azure Table mappings for all synced crew
        if (azureTableClient) {
          const aptaeroCrewAfter = await aptaeroClient.getMasterCrewMembers('GI');
          const aptaeroByBadge = new Map(aptaeroCrewAfter.map(c => [c.badgeNo, c]));
          
          for (const fl3xxCrew of fl3xxMasterCrew) {
            const aptaeroCrew = aptaeroByBadge.get(fl3xxCrew.badgeNo);
            if (aptaeroCrew) {
              await azureTableClient.upsertCrewMapping({
                fl3xxId: String(fl3xxCrew.pilotId || fl3xxCrew.id),
                aptaeroCrewId: aptaeroCrew.id,
                badgeNoLastSynced: fl3xxCrew.badgeNo || '',
                firstName: fl3xxCrew.firstName || '',
                lastName: fl3xxCrew.lastName || '',
                lastSyncedAt: new Date().toISOString(),
              });
            }
          }
        }
      } else {
        crewErrors = fl3xxMasterCrew.length;
        console.error('Bulk import failed:', result);
      }
      
      await storage.createLog({
        eventType: "Crew Sync",
        status: crewErrors > 0 ? "warning" : "success",
        source: "fl3xx",
        environment,
        details: `Bulk crew sync: ${crewAdded} added, ${crewUpdated} updated, ${crewDeactivated} deactivated, ${crewErrors} errors`,
        metadata: { added: crewAdded, updated: crewUpdated, deactivated: crewDeactivated, errors: crewErrors, method: 'bulk' },
      });
    } catch (err) {
      console.error('Phase 1 crew sync failed:', err);
      await storage.createLog({
        eventType: "Crew Sync",
        status: "error",
        source: "system",
        environment,
        details: `Crew sync failed: ${err instanceof Error ? err.message : 'Unknown error'}`,
        metadata: null,
      });
    }

    // ============= PHASE 2: Flights Sync =============
    console.log('=== PHASE 2: Flights Sync ===');
    
    let flightsCreated = 0, flightsUpdated = 0, flightsSkipped = 0, flightErrors = 0;
    let flightsNeedingCreation = 0;
    const allowNewFlights = options.allowNewFlights;
    
    // Declare eligibleFlights at function scope so it's accessible in PHASE 3
    let eligibleFlights: any[] = [];
    
    try {
      // Get eligible flights from FL3XX (excludes MOCK aircraft)
      const today = new Date();
      const sixtyDaysAgo = new Date(today);
      sixtyDaysAgo.setDate(today.getDate() - 30);
      const sixtyDaysAhead = new Date(today);
      sixtyDaysAhead.setDate(today.getDate() + 60);
      
      const fl3xxData = await fl3xxClient.getAllData();
      
      // Filter flights based on arrival time (realDateIn):
      // - Include if arrival is in future OR within last 24 hours
      // - Aptaero keeps flights active for 24 hrs after arrival, then complete, then archive after 5 days
      // - Exclude archived flights (>5 days after completion)
      const now = new Date();
      const twentyFourHoursAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);
      
      eligibleFlights = fl3xxData.flights.filter(f => {
        // Exclude mock/test tail numbers
        const tailNo = (f.aircraft || '').toUpperCase();
        if (tailNo.includes('MOCK') || tailNo === 'N777AZ') return false;
        
        // Skip closed flights (FL3XX lifecycle, not completion status)
        if (f.lifecycleState === 'closed') return false;
        
        // Check arrival time (realDateIn) - this is when the flight actually landed
        // If no arrival time, use scheduled arrival (arrivalTime)
        const arrivalTimeStr = f.realDateIn || f.arrivalTime;
        
        if (!arrivalTimeStr) {
          // No arrival time at all - include if it has a future departure
          const departureDate = f.departureTime ? new Date(f.departureTime) : null;
          return departureDate && departureDate > now;
        }
        
        const arrivalTime = new Date(arrivalTimeStr);
        
        // Include if:
        // 1. Arrival is in the future (flight hasn't landed yet), OR
        // 2. Arrival was within the last 24 hours (Aptaero still considers it active)
        return arrivalTime > now || arrivalTime >= twentyFourHoursAgo;
      });
      
      console.log(`Eligible flights: ${eligibleFlights.length} (filtered from ${fl3xxData.flights.length})`);
      
      // Get existing flight mappings from Azure Table Storage
      let flightMappings: FlightMapping[] = [];
      if (azureTableClient) {
        try {
          flightMappings = await azureTableClient.getFlightMappings();
          console.log(`Azure Table: Retrieved ${flightMappings.length} flight mappings`);
        } catch (err) {
          console.error('Failed to get flight mappings:', err);
        }
      }
      // CRITICAL: Normalize all keys to strings for consistent lookup
      const mappingsByFL3XXId = new Map(flightMappings.map(m => [String(m.fl3xxFlightId), m]));
      
      // Fetch Aptaero flights ONCE for duplicate detection (not per-flight)
      let aptaeroFlightsForDupeCheck: any[] = [];
      try {
        aptaeroFlightsForDupeCheck = await aptaeroClient.getFlightSegments();
        console.log(`Loaded ${aptaeroFlightsForDupeCheck.length} Aptaero flights for duplicate detection`);
      } catch (err) {
        console.warn('Could not fetch Aptaero flights for duplicate check:', err);
      }
      
      // Pre-scan to count how many flights would be created
      for (const flight of eligibleFlights) {
        const fl3xxId = String(flight.id);
        const hasMapping = mappingsByFL3XXId.has(fl3xxId);
        if (!hasMapping) {
          flightsNeedingCreation++;
        }
      }
      console.log(`Pre-scan result: ${flightsNeedingCreation} flights need creation`);
      
      if (flightsNeedingCreation > 0 && !allowNewFlights) {
        console.log(`SAFETY BLOCK: ${flightsNeedingCreation} flights would be created, but allowNewFlights=false`);
      }
      
      // ORPHAN DETECTION: Find mappings for flights not in current FL3XX dataset
      // These are "ghost mappings" that could cause issues if other code paths use them
      const eligibleFlightIds = new Set(eligibleFlights.map(f => String(f.id)));
      const orphanedMappings: Array<{fl3xxId: string; aptaeroId: string}> = [];
      
      for (const mapping of flightMappings) {
        const fl3xxId = String(mapping.fl3xxFlightId);
        if (!eligibleFlightIds.has(fl3xxId)) {
          orphanedMappings.push({
            fl3xxId,
            aptaeroId: mapping.airlineChoiceFlightId
          });
        }
      }
      
      if (orphanedMappings.length > 0) {
        console.log(`=== ORPHAN DETECTION ===`);
        console.log(`Found ${orphanedMappings.length} mappings for FL3XX flights not in current eligible set:`);
        for (const orphan of orphanedMappings.slice(0, 10)) { // Log first 10
          console.log(`  - FL3XX ${orphan.fl3xxId} -> Aptaero ${orphan.aptaeroId} (ORPHANED)`);
        }
        if (orphanedMappings.length > 10) {
          console.log(`  ... and ${orphanedMappings.length - 10} more`);
        }
        console.log(`These mappings will NOT cause updates (we only iterate through eligibleFlights)`);
      }
      
      // Process each eligible flight
      for (const flight of eligibleFlights) {
        try {
          // CRITICAL: Always convert to string for consistent lookup
          const fl3xxFlightId = String(flight.id);
          const existingMapping = mappingsByFL3XXId.get(fl3xxFlightId);
          
          // Look up Aptaero customer from FL3XX accountName
          let aptaeroCustomerID: string | undefined;
          const fl3xxAccountName = (flight as any).accountName;
          if (fl3xxAccountName) {
            const customer = await aptaeroClient.findCustomerByName(fl3xxAccountName, 'GI');
            if (customer) {
              aptaeroCustomerID = customer.id;
            }
          }
          
          // Build Aptaero flight payload with customer mapping
          const flightPayload = buildAptaeroFlightPayload(flight, 'GI', aptaeroCustomerID);
          
          if (existingMapping?.airlineChoiceFlightId) {
            // UPDATE existing segment in Aptaero
            const aptaeroSegmentId = existingMapping.airlineChoiceFlightId;
            
            // ARCHIVED PROTECTION: Check if Aptaero segment is archived - skip if so
            const aptaeroSegment = aptaeroFlightsForDupeCheck.find((f: any) => 
              (f.ID || f.id) === aptaeroSegmentId
            );
            if (aptaeroSegment) {
              const segmentStatus = String(aptaeroSegment.Status || aptaeroSegment.status || '').toLowerCase();
              if (segmentStatus === 'archived' || segmentStatus === '4') {
                console.log(`SKIPPED: Aptaero segment ${aptaeroSegmentId} is ARCHIVED - not overwriting`);
                flightsSkipped++;
                continue;
              }
            }
            
            // Build update payload with segment ID
            const updatePayload = {
              ...flightPayload,
              ID: aptaeroSegmentId,
              FlightSegmentID: aptaeroSegmentId,
            };
            
            try {
              const updateResult = await aptaeroClient.updateFlightSegment(updatePayload);
              if (updateResult.success) {
                flightsUpdated++;
                console.log(`Updated flight ${fl3xxFlightId} (Aptaero segment ${aptaeroSegmentId})`);
                // Update mapping in Azure Table with latest departure time
                if (azureTableClient) {
                  const depTime = flight.departureTime ? new Date(flight.departureTime) : undefined;
                  await azureTableClient.upsertFlightMapping(fl3xxFlightId, aptaeroSegmentId, depTime);
                }
              } else {
                flightErrors++;
                console.error(`Failed to update flight ${fl3xxFlightId}: ${updateResult.message}`);
              }
            } catch (updateErr) {
              flightErrors++;
              console.error(`Error updating flight ${fl3xxFlightId}:`, updateErr);
            }
          } else {
            // SAFETY: Block new flight creation unless explicitly allowed
            if (!allowNewFlights) {
              console.log(`BLOCKED: Would create flight ${fl3xxFlightId} but allowNewFlights=false`);
              flightsSkipped++;
              continue;
            }
            
            // DUPLICATE PREVENTION: Check if flight already exists in Aptaero before creating
            // This prevents duplicates when mapping wasn't saved due to bugs
            const flightNo = flightPayload.FlightNo;
            const depDate = flightPayload.ScheduledDepartureTime?.split('T')[0];
            const origin = flightPayload.OriginIATA;
            
            // Check against pre-fetched Aptaero segments for duplicate
            const existingInAptaero = aptaeroFlightsForDupeCheck.find((f: any) => {
              const fNum = f.FlightNo || f.flightNo || f.flightNumber;
              const fDate = (f.ScheduledDepartureDate || f.scheduledDepartureDate || f.ScheduledDepartureTime || '').split('T')[0];
              const fOrigin = f.OriginIATA || f.originIATA || f.origin;
              return fNum === flightNo && fDate === depDate && fOrigin === origin;
            });
            
            if (existingInAptaero) {
              // Flight already exists in Aptaero but we don't have mapping - save mapping and skip
              const existingSegmentId = existingInAptaero.ID || existingInAptaero.id;
              console.warn(`DUPLICATE PREVENTION: Flight ${fl3xxFlightId} already exists in Aptaero as ${existingSegmentId}`);
              if (azureTableClient && existingSegmentId) {
                await azureTableClient.upsertFlightMapping(fl3xxFlightId, existingSegmentId);
                console.log(`Saved orphan mapping: ${fl3xxFlightId} -> ${existingSegmentId}`);
              }
              flightsSkipped++;
              continue;
            }
            
            // CREATE new segment - only if all safety checks passed
            console.log(`CREATING NEW FLIGHT: ${fl3xxFlightId} -> ${flightNo} on ${depDate}`);
            const result = await aptaeroClient.addFlightSegment(flightPayload);
            // Check for FlightSegmentID in various possible locations in the response
            const segmentId = result.data?.FlightSegment?.ID || 
                              result.data?.FlightSegment?.FlightSegmentID ||
                              result.data?.FlightSegmentID ||
                              result.data?.ID;
            
            if (result.success && segmentId) {
              flightsCreated++;
              console.log(`Created flight ${fl3xxFlightId} -> Aptaero segment ${segmentId}`);
              // Save mapping to Azure Table
              if (azureTableClient) {
                await azureTableClient.upsertFlightMapping(
                  fl3xxFlightId,
                  segmentId
                );
              }
            } else if (result.success) {
              // Success but no segment ID returned - still count as created but log warning
              flightsCreated++;
              console.warn(`Created flight ${fl3xxFlightId} but no segment ID returned in response`);
            } else {
              flightErrors++;
              console.error(`Failed to create flight ${fl3xxFlightId}: ${result.message}`);
            }
          }
        } catch (err) {
          flightErrors++;
          console.error(`Error processing flight:`, err);
        }
      }
      
      await storage.createLog({
        eventType: "Flight Sync",
        status: flightErrors > 0 ? "warning" : "success",
        source: "fl3xx",
        environment,
        details: `Flight sync: ${flightsCreated} created, ${flightsUpdated} updated, ${flightsSkipped} skipped, ${flightErrors} errors`,
        metadata: { created: flightsCreated, updated: flightsUpdated, skipped: flightsSkipped, errors: flightErrors },
      });
    } catch (err) {
      console.error('Phase 2 flight sync failed:', err);
      await storage.createLog({
        eventType: "Flight Sync",
        status: "error",
        source: "system",
        environment,
        details: `Flight sync failed: ${err instanceof Error ? err.message : 'Unknown error'}`,
        metadata: null,
      });
    }

    // ============= PHASE 3: Crew Manifest Assignment =============
    console.log('=== PHASE 3: Crew Manifest Assignment ===');
    
    let crewManifestsAssigned = 0, crewManifestsSkipped = 0, crewManifestErrors = 0;
    
    try {
      // Re-fetch updated mappings after any new flights were created
      let updatedMappings: FlightMapping[] = [];
      if (azureTableClient) {
        updatedMappings = await azureTableClient.getFlightMappings();
        console.log(`Azure Table: ${updatedMappings.length} flight mappings for crew assignment`);
      }
      const updatedMappingsByFL3XXId = new Map(updatedMappings.map(m => [String(m.fl3xxFlightId), m]));
      
      // Get FL3XX data again to access flight crew
      const fl3xxDataForCrew = await fl3xxClient.getAllData();
      
      // For each eligible flight with a mapping, assign crew
      for (const flight of eligibleFlights) {
        const fl3xxFlightId = String(flight.id);
        const mapping = updatedMappingsByFL3XXId.get(fl3xxFlightId);
        
        if (!mapping?.airlineChoiceFlightId) {
          console.log(`Skipping crew assignment for ${fl3xxFlightId}: No Aptaero segment ID`);
          crewManifestsSkipped++;
          continue;
        }
        
        const aptaeroSegmentId = mapping.airlineChoiceFlightId;
        
        try {
          // Get crew assigned to this flight in FL3XX
          const fl3xxFlightCrew = await fl3xxClient.getFlightCrew(String(flight.id));
          
          if (!fl3xxFlightCrew || fl3xxFlightCrew.length === 0) {
            console.log(`No crew assigned in FL3XX for flight ${flight.id}`);
            crewManifestsSkipped++;
            continue;
          }
          
          // Build crew manifest payload using ExternalID for stable matching
          // CRITICAL: Use pilotId - FL3XX docs confirm it's the PRIMARY key for flight crew
          const crewManifestPayload = buildCrewManifestPayload(
            {
              flightNumber: flight.flightNumber || `FL-${flight.id}`,
              departure: flight.departure,
              departureTime: flight.departureTime,
            },
            fl3xxFlightCrew.map((c: any) => ({
              badgeNo: c.badgeNo,
              externalId: String(c.pilotId || ''),  // FL3XX pilotId = Aptaero ExternalID
              role: c.role,
            })),
            aptaeroSegmentId,
            'GI',
            'BadgeNo'  // Use BadgeNo for crew matching (proven legacy approach)
          );
          
          // Import crew manifest to Aptaero
          const manifestResult = await aptaeroClient.importCrewManifest(crewManifestPayload);
          
          if (manifestResult.success) {
            crewManifestsAssigned++;
            console.log(`Assigned ${fl3xxFlightCrew.length} crew to flight ${flight.flightNumber || flight.id}`);
          } else {
            crewManifestErrors++;
            console.error(`Failed to assign crew to flight ${flight.id}: ${manifestResult.message}`);
          }
        } catch (err) {
          crewManifestErrors++;
          console.error(`Error assigning crew to flight ${flight.id}:`, err);
        }
      }
      
      await storage.createLog({
        eventType: "Crew Manifest Assignment",
        status: crewManifestErrors > 0 ? "warning" : "success",
        source: "fl3xx",
        environment,
        details: `Crew manifests: ${crewManifestsAssigned} assigned, ${crewManifestsSkipped} skipped, ${crewManifestErrors} errors`,
        metadata: { assigned: crewManifestsAssigned, skipped: crewManifestsSkipped, errors: crewManifestErrors },
      });
    } catch (err) {
      console.error('Phase 3 crew manifest assignment failed:', err);
      await storage.createLog({
        eventType: "Crew Manifest Assignment",
        status: "error",
        source: "system",
        environment,
        details: `Crew manifest assignment failed: ${err instanceof Error ? err.message : 'Unknown error'}`,
        metadata: null,
      });
    }

    console.log('=== MANUAL SYNC COMPLETED ===');
    
    await storage.createLog({
      eventType: "Manual Sync",
      status: "success",
      source: "system",
      environment,
      details: `Sync completed: ${crewAdded + crewUpdated} crew synced, ${flightsCreated} flights created, ${crewManifestsAssigned} manifests assigned`,
      metadata: { 
        crew: { added: crewAdded, updated: crewUpdated, deactivated: crewDeactivated, errors: crewErrors },
        flights: { created: flightsCreated, updated: flightsUpdated, skipped: flightsSkipped, errors: flightErrors },
        crewManifests: { assigned: crewManifestsAssigned, skipped: crewManifestsSkipped, errors: crewManifestErrors }
      },
    });

    const flightWarning = flightsNeedingCreation > 0 && !allowNewFlights
      ? `${flightsNeedingCreation} flights blocked from creation (use allowNewFlights: true to create)`
      : null;

    // Save sync status to Azure Table Storage for persistence across app restarts
    if (azureTableClient) {
      const syncSource = options.source;
      await azureTableClient.saveSyncStatus({
        timestamp: new Date().toISOString(),
        success: true,
        environment,
        crew: { added: crewAdded, updated: crewUpdated, deactivated: crewDeactivated, errors: crewErrors },
        flights: { created: flightsCreated, updated: flightsUpdated, skipped: flightsSkipped, errors: flightErrors },
        crewManifests: { assigned: crewManifestsAssigned, skipped: crewManifestsSkipped, errors: crewManifestErrors },
        source: syncSource,
      });
    }
      
    return { 
      success: true, 
      message: flightWarning ? "Sync completed with blocked flights" : "Sync completed successfully",
      environment,
      synced: {
        crew: { added: crewAdded, updated: crewUpdated, deactivated: crewDeactivated, errors: crewErrors },
        flights: { created: flightsCreated, updated: flightsUpdated, skipped: flightsSkipped, errors: flightErrors, blocked: flightsNeedingCreation > 0 && !allowNewFlights ? flightsNeedingCreation : 0 },
        crewManifests: { assigned: crewManifestsAssigned, skipped: crewManifestsSkipped, errors: crewManifestErrors }
      },
      warning: flightWarning || undefined
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);

    await storage.createLog({
      eventType: options.triggeredBy === "timer" ? "Automatic Sync" : "Manual Sync",
      status: "error",
      source: "system",
      environment,
      details: `Sync failed: ${message}`,
      metadata: { error: message, source: options.source }
    });

    return {
      success: false,
      message: "Sync failed",
      environment,
      error: message
    };
  }
}
