column_groups = {
    "id": ["EPISODE_ID", "EVENT_ID", "FATALITY_ID"],
    "time": [
        "BEGIN_DAY", "END_DAY", "YEAR", "FAT_DAY",
        "BEGIN_MONTH", "END_MONTH", "BEGIN_MONTH_NAME", "DURATION_DAYS"
    ],
    "location": [
        "STATE", "CZ_TYPE", "CZ_NAME", "BEGIN_LOCATION", "BEGIN_LAT",
        "BEGIN_LON", "LOCATION_LABEL"
    ],
    "damage": [
        "INJURIES_DIRECT", "INJURIES_INDIRECT", "DEATHS_DIRECT",
        "DEATHS_INDIRECT", "DAMAGE_PROPERTY", "DAMAGE_CROPS"
    ],
    "severity": [
        "MAGNITUDE", "MAGNITUDE_TYPE", "TOR_F_SCALE", "TOR_LENGTH", "TOR_WIDTH"
    ],
    "event_type": ["EVENT_TYPE"]
}