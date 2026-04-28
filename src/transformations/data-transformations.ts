/**
 * Shared data transformation helpers for FL3XX → Aptaero sync
 * These functions implement the validated data mapping requirements
 * and should be used by ALL sync-related endpoints to ensure consistency.
 */

import type { AddFlightSegmentRequest, ImportCrewManifestRequest } from '../clients/aptaero-client.js';

/**
 * Aircraft registration overrides for Aptaero sync
 * Maps FL3XX tail numbers to Aptaero tail numbers when they differ
 */
const AIRCRAFT_OVERRIDES: Record<string, string> = {
  'N862DA': 'N866DA',
};

/**
 * Apply aircraft registration override if configured
 * Returns the Aptaero-compatible tail number
 */
export function applyAircraftOverride(tailNo: string): string {
  const upperTail = (tailNo || '').toUpperCase().trim();
  return AIRCRAFT_OVERRIDES[upperTail] || tailNo;
}

/**
 * Normalize flight number by stripping all non-digit characters
 * Aptaero only accepts numeric flight numbers
 * Examples: "GI9015" -> "9015", "RZN123" -> "123", "9015" -> "9015"
 */
export function normalizeFlightNumber(flightNo: string | undefined): string {
  return (flightNo || '').replace(/\D/g, '');
}

/**
 * ICAO → IATA airport mappings for ICAO codes that cannot be safely converted
 * by dropping the first character.
 *
 * Do NOT add a generic `slice(1)` fallback for all 4-character ICAO codes.
 * That caused bad airport matches, for example SGAS → GAS, where GAS is
 * Garissa, Kenya. SGAS should map to ASU in Paraguay.
 */
const ICAO_TO_IATA_OVERRIDES: Record<string, string> = {
  SGAS: 'ASU', // Asunción / Silvio Pettirossi, Paraguay
  SBGR: 'GRU', // São Paulo / Guarulhos, Brazil
  HKGA: 'GAS', // Garissa, Kenya
};

/**
 * Convert an airport identifier to the IATA code expected by Aptaero.
 *
 * Rules:
 * - 3-letter codes are already IATA and are returned unchanged.
 * - Known ICAO codes are converted through explicit ICAO → IATA mappings.
 * - Contiguous U.S. K-prefixed ICAO codes usually map as KXXX → XXX
 *   (KAFW → AFW, KACT → ACT). This limited fallback preserves existing
 *   U.S. behavior without corrupting international airports.
 * - Unknown 4-letter ICAO codes are returned unchanged and logged instead
 *   of being silently truncated to a potentially wrong airport.
 */
export function toIATA(code: string): string {
  const cleaned = (code || '').toUpperCase().trim();

  if (/^[A-Z]{3}$/.test(cleaned)) {
    return cleaned;
  }

  if (/^[A-Z]{4}$/.test(cleaned)) {
    const mapped = ICAO_TO_IATA_OVERRIDES[cleaned];
    if (mapped) {
      return mapped;
    }

    if (cleaned.startsWith('K')) {
      return cleaned.slice(1);
    }

    console.warn(`[Airport Mapping] No ICAO→IATA mapping found for ${cleaned}. Leaving value unchanged; do not truncate this value.`);
    return cleaned;
  }

  return cleaned;
}

/**
 * Map FL3XX CrewPosition.role to Aptaero StatusOnBoard value
 * 1 = Pilot (CMD or FO)
 * 2 = Flight Attendant (MEDIC, MED1, or FA)
 * 4 = Other (any other role)
 */
export function roleToStatusOnBoard(role: string): number {
  const r = (role || '').toUpperCase();
  if (r === 'CMD' || r === 'FO') return 1;
  if (r === 'MEDIC' || r === 'MED1' || r === 'FA') return 2;
  return 4;
}

/**
 * Get Aptaero flight type from FL3XX workflowCustomName
 * 1 = Public or Owner Private
 * 2 = Charter
 * 0 = Anything else (default)
 */
export function getFlightType(workflowType: string | undefined): number {
  const typeStr = (workflowType || '').toLowerCase();
  if (typeStr === 'public' || typeStr === 'owner private') return 1;
  if (typeStr === 'charter') return 2;
  return 0; // Default for anything else
}

/**
 * Determine flight status from FL3XX data
 * Returns: 'cancelled' | 'concluded' | 'open'
 * 
 * Logic:
 * - Cancelled: flightStatus == "Canceled"
 * - Concluded: postFlightClosed == true OR realDateIN has a value
 * - Open: otherwise
 */
export function getFlightStatus(flight: {
  flightStatus?: string;
  postFlightClosed?: boolean;
  realDateIN?: string;
}): 'cancelled' | 'concluded' | 'open' {
  if (flight.flightStatus === 'Canceled') return 'cancelled';
  if (flight.postFlightClosed === true || (flight.realDateIN && flight.realDateIN.trim() !== '')) return 'concluded';
  return 'open';
}

/**
 * Build Aptaero flight segment payload from FL3XX flight data
 * Uses all validated transformations from sync preview requirements
 * 
 * For concluded flights (postFlightClosed == true OR realDateIN has value):
 * - Maps actual timestamps: realDateOUT→OffBlocksTime, realDateOFF→TakeOffTime, 
 *   realDateON→TouchDownTime, realDateIN→OnBlocksTime
 * 
 * CustomerID mapping: FL3XX accountName is matched to Aptaero customers via findCustomerByName()
 */
export function buildAptaeroFlightPayload(
  flight: {
    flightNumber?: string;
    departure?: string;
    arrival?: string;
    departureTime?: string;
    arrivalTime?: string;
    aircraft?: string;
    workflowCustomName?: string;
    flightStatus?: string;
    postFlightClosed?: boolean;
    realDateOUT?: string;
    realDateOFF?: string;
    realDateON?: string;
    realDateIN?: string;
    accountName?: string;  // FL3XX account name for customer mapping
  },
  carrierCode: string = 'GI',
  customerID?: string  // Aptaero customer GUID (looked up from accountName)
): AddFlightSegmentRequest {
  const flightNo = normalizeFlightNumber(flight.flightNumber);
  const originIATA = toIATA(flight.departure || '');
  const destinationIATA = toIATA(flight.arrival || '');
  const type = getFlightType(flight.workflowCustomName);
  
  const scheduledDep = flight.departureTime || '';
  const scheduledArr = flight.arrivalTime || '';
  const estimatedDep = flight.departureTime || scheduledDep;
  const estimatedArr = flight.arrivalTime || scheduledArr;
  
  // Determine flight status
  const status = getFlightStatus(flight);
  
  // For concluded flights, map actual timestamps
  const isConcluded = status === 'concluded';
  
  return {
    CarrierCode: carrierCode,
    FlightNo: flightNo,
    Type: type,
    TailNo: applyAircraftOverride(flight.aircraft || ''),
    OriginIATA: originIATA,
    DestinationIATA: destinationIATA,
    ScheduledDepartureTime: scheduledDep,
    EstimatedDepartureTime: estimatedDep,
    ScheduledArrivalTime: scheduledArr,
    EstimatedArrivalTime: estimatedArr,
    OffBlocksTime: isConcluded ? flight.realDateOUT : undefined,
    TakeOffTime: isConcluded ? flight.realDateOFF : undefined,
    TouchDownTime: isConcluded ? flight.realDateON : undefined,
    OnBlocksTime: isConcluded ? flight.realDateIN : undefined,
    Message: flight.flightNumber || '',
    Status: 0, // Status field for cancelled is handled by CancelFlightSegment endpoint
    CustomerID: customerID,  // Aptaero customer GUID mapped from FL3XX accountName
  };
}

/**
 * Build Aptaero crew manifest payload from FL3XX crew data
 * Uses all validated transformations from sync preview requirements
 * CRITICAL: Requires FlightSegmentID (Aptaero's GUID) to link crew to the flight
 * 
 * @param referenceType - "ExternalID" to match by FL3XX pilotId (stable across MCL reimports),
 *                        "BadgeNo" to match by badge number
 * 
 * Strategy: 
 * - If ExternalID mode is requested AND all crew have valid externalIds, use ExternalID mode
 * - If any crew are missing externalId, the entire payload falls back to BadgeNo mode
 * - Crew must have at least one valid identifier (externalId or badgeNo) to be included
 */
export function buildCrewManifestPayload(
  flight: {
    flightNumber?: string;
    departure?: string;
    departureTime?: string;
  },
  crewMembers: Array<{
    badgeNo?: string;
    externalId?: string;  // FL3XX pilotId - primary key for ExternalID matching
    role: string;
  }>,
  flightSegmentId: string,  // Aptaero's flight segment GUID - REQUIRED
  carrierCode: string = 'GI',
  referenceType: 'ExternalID' | 'BadgeNo' = 'ExternalID'  // Default to ExternalID for stable matching
): ImportCrewManifestRequest {
  if (!flightSegmentId || flightSegmentId.trim() === '') {
    throw new Error('FlightSegmentID is required for crew manifest import. Without it, Aptaero returns "Nullable object must have a value".');
  }
  
  const flightNo = normalizeFlightNumber(flight.flightNumber);
  const originIATA = toIATA(flight.departure || '');
  const departureDate = (flight.departureTime || '').split('T')[0];
  
  // Helper to check if crew has valid externalId
  const hasValidExternalId = (c: typeof crewMembers[0]) => 
    c.externalId && c.externalId.toString().trim() !== '' && c.externalId.toString().trim() !== '0';
  
  // Helper to check if crew has valid badgeNo
  const hasValidBadgeNo = (c: typeof crewMembers[0]) => 
    c.badgeNo && c.badgeNo.trim() !== '';
  
  // First, check if ALL crew have valid externalIds (for ExternalID mode)
  const allHaveExternalId = crewMembers.length > 0 && crewMembers.every(hasValidExternalId);
  
  // Determine effective reference type
  const effectiveReferenceType = (referenceType === 'ExternalID' && allHaveExternalId) 
    ? 'ExternalID' 
    : 'BadgeNo';
  
  // Filter crew based on effective reference type
  const validCrewMembers = effectiveReferenceType === 'ExternalID'
    ? crewMembers.filter(hasValidExternalId)  // For ExternalID mode, include all with externalId
    : crewMembers.filter(hasValidBadgeNo);     // For BadgeNo mode, include all with badge
  
  console.log(`Crew manifest: ${crewMembers.length} input, ${validCrewMembers.length} valid for ${effectiveReferenceType} mode (allHaveExternalId=${allHaveExternalId})`);
  
  return {
    FlightSegmentID: flightSegmentId,  // Link crew to this specific flight segment
    CarrierCode: carrierCode,
    FlightNo: flightNo,
    ScheduledDepartureDate: departureDate,
    OriginIATA: originIATA,
    CrewRecordReferenceType: effectiveReferenceType,  // ExternalID for stable matching, BadgeNo as fallback
    CrewMembers: validCrewMembers.map(crew => {
      const statusOnBoard = roleToStatusOnBoard(crew.role);
      
      // Build crew member payload based on effective reference type
      const crewPayload: any = {
        StatusOnBoard: statusOnBoard,
        CrewMemberJourneys: [{
          StatusOnBoard: statusOnBoard,
          IsDeleted: false,
        }],
      };
      
      // Add the appropriate identifier field based on effective reference type
      if (effectiveReferenceType === 'ExternalID') {
        crewPayload.ExternalID = crew.externalId!.toString().trim();
      } else {
        crewPayload.BadgeNo = crew.badgeNo!.trim();
      }
      
      return crewPayload;
    }),
  };
}
