"""Small synthetic OPDI dataset (same file names and schemas as OPDI v0.0.2 in 2026), with known defects.

Scope of the synthetic run: [2026-06-09, 2026-06-19[, i.e. one 10-day window. The generator returns the
numbers the pipeline must find, so the end-to-end test can check them.
"""
import csv
import datetime
import json
import random
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

START, END = datetime.date(2026, 6, 9), datetime.date(2026, 6, 19)

AIRPORTS = [  # ident, type, name, lat, lon, elevation, continent, country, municipality, iata
    ("LFBO", "large_airport", "Toulouse-Blagnac Airport", 43.6291, 1.3638, 499, "EU", "FR", "Toulouse", "TLS"),
    ("ENZV", "large_airport", "Stavanger Airport, Sola", 58.8767, 5.6378, 29, "EU", "NO", "Stavanger", "SVG"),
    ("LFPG", "large_airport", "Paris Charles de Gaulle Airport", 49.0097, 2.5479, 392, "EU", "FR", "Paris", "CDG"),
    ("ENGM", "large_airport", "Oslo Airport, Gardermoen", 60.1939, 11.1004, 681, "EU", "NO", "Oslo", "OSL"),
    ("EGLL", "large_airport", "London Heathrow Airport", 51.4706, -0.4619, 83, "EU", "GB", "London", "LHR"),
    ("00AK", "small_airport", "Lowell Field", 59.9478, -151.6959, 450, "NA", "US", "Anchor Point", ""),
]
RUNWAYS = {"LFBO": [("14L", "32R"), ("14R", "32L")], "ENZV": [("18", "36"), ("11", "29")],
           "LFPG": [("09L", "27R"), ("08R", "26L")], "ENGM": [("01L", "19R"), ("01R", "19L")],
           "EGLL": [("09L", "27R"), ("09R", "27L")]}
ROUTES = [("LFBO", "LFPG"), ("LFPG", "LFBO"), ("ENZV", "ENGM"), ("ENGM", "ENZV"), ("LFBO", "ENZV"),
          ("ENZV", "LFBO"), ("EGLL", "LFBO"), ("LFPG", "EGLL"), ("EGLL", "ENGM")]

FL_SCHEMA = pa.schema([("id", pa.int64()), ("icao24", pa.string()), ("flt_id", pa.string()), ("dof", pa.string()),
                       ("adep", pa.string()), ("ades", pa.string()), ("adep_p", pa.string()), ("ades_p", pa.string()),
                       ("registration", pa.string()), ("model", pa.string()), ("typecode", pa.string()),
                       ("icao_aircraft_class", pa.string()), ("icao_operator", pa.string()),
                       ("first_seen", pa.string()), ("last_seen", pa.string()), ("version", pa.string())])
EV_SCHEMA = pa.schema([("id", pa.int64()), ("flight_id", pa.int64()), ("type", pa.string()),
                       ("event_time", pa.string()), ("longitude", pa.float64()), ("latitude", pa.float64()),
                       ("altitude", pa.float64()), ("source", pa.string()), ("version", pa.string()),
                       ("info", pa.string())])
MS_SCHEMA = pa.schema([("id", pa.int64()), ("event_id", pa.int64()), ("type", pa.string()),
                       ("value", pa.float64()), ("version", pa.string())])


def osm_info(aeroway, ref, airport):
    """`info` JSON of an OPDI airport event (same keys as the real data)."""
    return json.dumps({"osm_id": 1000 + len(ref), "osm_aeroway": aeroway, "osm_ref": ref, "osm_airport": airport,
                       "opdi_time_in_use_s": 120, "osn_icao24": "4ca000", "osn_flight_id": "AFR1    "})


def ts(t):
    return t.strftime("%Y-%m-%d %H:%M:%S")


def write_site(site: Path, n_flights: int = 300, seed: int = 535):
    """Write the synthetic files under `site` (served as the EUROCONTROL / OurAirports URLs)."""
    rnd = random.Random(seed)
    coords = {a[0]: (a[3], a[4]) for a in AIRPORTS}
    flights, events, measurements = [], [], []
    expected = dict(valid_flights=0, quarantine_flights={}, out_of_scope_flights=0, duplicate_flights=0,
                    orphan_events=0, invalid_coordinate_events=0, duplicate_events=0, out_of_scope_events=0)
    event_id = [1000]

    def add_event(fid, kind, t, apt, alt, info="nan", north_km=0.0, flown_nm=None):
        """One event (position: the airport, moved `north_km` to the north) + its measurements."""
        event_id[0] += 1
        lat, lon = coords.get(apt, (50.0, 5.0))
        events.append(dict(id=event_id[0], flight_id=fid, type=kind, event_time=ts(t), longitude=lon,
                           latitude=lat + north_km / 111.2, altitude=alt, source="OSN", version="v0.0.2", info=info))
        measurements.append(dict(id=event_id[0] * 10, event_id=event_id[0], type="Time Passed (s)",
                                 value=float(rnd.randint(0, 9000)), version="v0.0.2"))
        if flown_nm is not None:
            measurements.append(dict(id=event_id[0] * 10 + 1, event_id=event_id[0], type="Distance flown (NM)",
                                     value=float(flown_nm), version="v0.0.2"))
        return event_id[0]

    def add_flight(fid, adep, ades, start, minutes, icao24=None, first_seen=None, ground_events=True):
        end = start + datetime.timedelta(minutes=minutes)
        flights.append(dict(id=fid, icao24=icao24 if icao24 is not None else f"4ca{fid:03x}",
                            flt_id=f"  AFR{fid} ", dof=str(start.date()), adep=adep, ades=ades, adep_p=None,
                            ades_p=None, registration=f"F-H{fid:03d}", model="Airbus A320", typecode="A320",
                            icao_aircraft_class="L2J", icao_operator=rnd.choice(["AFR", "SAS", "NAX", "BAW", ""]),
                            first_seen=first_seen if first_seen is not None else ts(start), last_seen=ts(end),
                            version="v0.0.2"))
        if not ground_events:
            return
        takeoff = start + datetime.timedelta(minutes=12)
        landing = end - datetime.timedelta(minutes=10)
        if adep in RUNWAYS:
            rw = "/".join(rnd.choice(RUNWAYS[adep]))          # OSM references: "14L/32R" or "14L-32R"
            add_event(fid, "exit-parking_position", start + datetime.timedelta(minutes=2), adep, 500.0,
                      osm_info("parking_position", "A12", adep))
            add_event(fid, "entry-runway", start + datetime.timedelta(minutes=5), adep, 500.0,
                      osm_info("runway", rw, adep))                # runway crossed while taxiing: not the line-up
            add_event(fid, "entry-runway", takeoff - datetime.timedelta(minutes=1), adep, 500.0,
                      osm_info("runway", rw, adep))
        add_event(fid, "take-off", takeoff, adep, 600.0)
        add_event(fid, "first-xing-fl100", takeoff + datetime.timedelta(minutes=5), adep, 10000.0)
        add_event(fid, "top-of-climb", takeoff + datetime.timedelta(minutes=10), adep, 36000.0)

        # Cruise event outside 100 NM circle (240 km north) to test ring entry interpolation
        add_event(fid, "first-xing-fl245", landing - datetime.timedelta(minutes=38), ades, 36000.0,
                  north_km=240.0, flown_nm=950.0)

        # Descent to destination:
        holding = fid % 3 == 0
        tod = landing - datetime.timedelta(minutes=31 if holding else 25)
        add_event(fid, "top-of-descent", tod, ades, 36000.0, north_km=150.0, flown_nm=1000.0)

        # Plant a fake top-of-descent on some flights at FL160 to verify that true descent start (FL360) is picked
        if fid % 5 == 2:
            add_event(fid, "top-of-descent", tod + datetime.timedelta(minutes=4), ades, 16000.0,
                      north_km=70.0, flown_nm=1030.0)

        if holding:
            if fid % 15 == 0:
                # 2 chunked segments of 10s and 345s (gap 5s <= 60s) at FL120: fused into 1 level of 6 min
                add_event(fid, "level-start", tod + datetime.timedelta(minutes=5), ades, 12000.0, north_km=80.0,
                          flown_nm=1037.5)
                add_event(fid, "level-end", tod + datetime.timedelta(minutes=5, seconds=10), ades, 12000.0, north_km=80.0,
                          flown_nm=1038.5)
                add_event(fid, "level-start", tod + datetime.timedelta(minutes=5, seconds=15), ades, 12000.0, north_km=80.0,
                          flown_nm=1039.0)
                add_event(fid, "level-end", tod + datetime.timedelta(minutes=11), ades, 12000.0, north_km=80.0,
                          flown_nm=1061.5)
            else:
                add_event(fid, "level-start", tod + datetime.timedelta(minutes=5), ades, 12000.0, north_km=80.0,
                          flown_nm=1037.5)
                add_event(fid, "level-end", tod + datetime.timedelta(minutes=11), ades, 12000.0, north_km=80.0,
                          flown_nm=1061.5)

        add_event(fid, "last-xing-fl100", landing - datetime.timedelta(minutes=12), ades, 10000.0, north_km=50.0,
                  flown_nm=1000.0 + (24 if holding else 0) + 13 * 7.5)
        if ades in RUNWAYS:
            add_event(fid, "entry-runway", landing - datetime.timedelta(seconds=30), ades, 350.0,
                      osm_info("runway", "-".join(RUNWAYS[ades][0]), ades),
                      flown_nm=1000.0 + (24 if holding else 0) + 24.5 * 7.5)
        add_event(fid, "landing", landing, ades, 300.0)
        if ades in RUNWAYS:
            rw = "-".join(rnd.choice(RUNWAYS[ades]))
            add_event(fid, "exit-runway", landing + datetime.timedelta(minutes=1), ades, 300.0,
                      osm_info("runway", rw, ades))
            # Runway crossed during rollout (after landing): must NOT be selected as landing entry
            add_event(fid, "entry-runway", landing + datetime.timedelta(minutes=2), ades, 300.0,
                      osm_info("runway", rw, ades))
            add_event(fid, "exit-runway", landing + datetime.timedelta(minutes=3), ades, 300.0,
                      osm_info("runway", rw, ades))
            add_event(fid, "entry-parking_position", landing + datetime.timedelta(minutes=6), ades, 300.0,
                      osm_info("parking_position", "B7", ades))

        # Ground level segment while parked at destination (altitude 350 ft < elevation + 1000 ft): must be excluded
        if fid % 50 == 0:
            add_event(fid, "level-start", landing + datetime.timedelta(minutes=7), ades, 350.0, north_km=0.0)
            add_event(fid, "level-end", landing + datetime.timedelta(minutes=9), ades, 350.0, north_km=0.0)

    # 1) Valid flights in scope (first_seen between June 9 and June 17, landing before June 19)
    t0 = datetime.datetime(2026, 6, 9, 5, 0)
    for i in range(n_flights):
        adep, ades = ROUTES[i % len(ROUTES)]
        if i % 10 == 0:
            adep = rnd.choice(["", None, "nan"])          # unknown departure airport: kept, flagged
        start = t0 + datetime.timedelta(minutes=rnd.randint(0, 8 * 24 * 60))
        duration = rnd.randint(75, 150)
        # Plant an incomplete traffic hour on June 9 14:00 UTC by shifting any flight away so NO movement lands or takes off in hour 14
        while True:
            takeoff = start + datetime.timedelta(minutes=12)
            landing = takeoff + datetime.timedelta(minutes=duration)
            if (takeoff.date() == datetime.date(2026, 6, 9) and takeoff.hour == 14) or \
               (landing.date() == datetime.date(2026, 6, 9) and landing.hour == 14):
                start += datetime.timedelta(hours=2)
            else:
                break
        add_flight(i + 1, adep, ades, start, duration)
        expected["valid_flights"] += 1

    # 2) Defective flights (-> quarantine) and flights out of scope (-> dropped)
    base = datetime.datetime(2026, 6, 12, 10, 0)
    add_flight(9001, "LFBO", "ENZV", base, -30)                                   # last_seen before first_seen
    add_flight(9002, "LFBO", "ENZV", base, 90, first_seen="not a timestamp")      # unparsable
    add_flight(9003, "ENZV", "LFBO", base, 90, icao24="")                         # no transponder address
    add_flight(9004, "LFPG", "LFBO", base, 1600)                                  # > 24 h
    expected["quarantine_flights"] = {"INVALID_CHRONOLOGY": 1, "UNPARSABLE_TIMESTAMP": 1,
                                      "MISSING_ICAO24": 1, "DURATION_GT_24H": 1}
    add_flight(9101, "LFBO", "LFPG", datetime.datetime(2026, 6, 3, 8), 80, ground_events=False)   # before scope
    add_flight(9102, "LFPG", "LFBO", datetime.datetime(2026, 6, 25, 8), 80, ground_events=False)  # after scope
    expected["out_of_scope_flights"] = 2

    # 3) Duplicated flight rows (same id, e.g. a flight present in two files)
    for k in range(3):
        flights.append(dict(flights[k]))
    expected["duplicate_flights"] = 3

    # 4) Defective events
    quarantined_ids = {9001, 9002, 9003, 9004}
    expected["orphan_events"] = sum(1 for e in events if e["flight_id"] in quarantined_ids)
    for k in range(4):                                                     # flight unknown in flight_list
        add_event(777000 + k, "take-off", base, "LFBO", 600.0)
    expected["orphan_events"] += 4
    bad = add_event(1, "level-start", base, "LFBO", 30000.0)               # latitude out of range
    events[-1]["latitude"] = 95.0
    expected["invalid_coordinate_events"] = 1
    for k in range(5):                                                     # exact duplicates
        events.append(dict(events[k], id=events[k]["id"] + 500000))
    expected["duplicate_events"] = 5
    add_event(1, "level-end", datetime.datetime(2026, 6, 8, 23, 0), "LFBO", 30000.0)   # before scope
    expected["out_of_scope_events"] = 1

    expected["bronze_flights"] = len(flights)
    expected["bronze_events"] = len(events)
    expected["valid_events"] = (len(events) - expected["orphan_events"] - expected["invalid_coordinate_events"]
                                - expected["duplicate_events"] - expected["out_of_scope_events"])
    expected["arrivals"] = expected["valid_flights"]
    expected["measured_arrivals"] = expected["valid_flights"]
    expected["holdings"] = sum(1 for i in range(n_flights) if (i + 1) % 3 == 0)
    expected["fused_level_offs"] = expected["holdings"]

    # 5) Files with the EUROCONTROL names, under the same URL layout
    w = f"{START:%Y%m%d}_{END:%Y%m%d}"
    for table, rows, schema, name in [("flight_list", flights, FL_SCHEMA, f"flight_list_{START:%Y%m}.parquet"),
                                      ("flight_events", events, EV_SCHEMA, f"flight_events_{w}.parquet"),
                                      ("measurements", measurements, MS_SCHEMA, f"measurements_{w}.parquet")]:
        (site / "opdi" / table).mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), site / "opdi" / table / name)

    (site / "ourairports").mkdir(parents=True, exist_ok=True)
    with open(site / "ourairports" / "airports.csv", "w", newline="") as f:
        out = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
        out.writerow(["id", "ident", "type", "name", "latitude_deg", "longitude_deg", "elevation_ft", "continent",
                      "iso_country", "iso_region", "municipality", "scheduled_service", "icao_code", "iata_code",
                      "gps_code", "local_code", "home_link", "wikipedia_link", "keywords"])
        for k, a in enumerate(AIRPORTS):
            out.writerow([k + 1, a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], f"{a[7]}-XX", a[8], "yes",
                          a[0], a[9], a[0], "", "", "", "test, \"quoted\", multi\nline"])
    with open(site / "ourairports" / "runways.csv", "w", newline="") as f:
        out = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
        out.writerow(["id", "airport_ref", "airport_ident", "length_ft", "width_ft", "surface", "lighted", "closed",
                      "le_ident", "le_latitude_deg", "le_longitude_deg", "le_elevation_ft", "le_heading_degT",
                      "le_displaced_threshold_ft", "he_ident", "he_latitude_deg", "he_longitude_deg",
                      "he_elevation_ft", "he_heading_degT", "he_displaced_threshold_ft"])
        k = 0
        for apt, rws in RUNWAYS.items():
            for le, he in rws:
                k += 1
                out.writerow([k, k, apt, 10000, 150, "ASP", 1, 0, le, "", "", "", 140, "", he, "", "", "", 320, ""])
    return expected
