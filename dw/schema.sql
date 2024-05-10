CREATE TABLE IF NOT EXISTS ResultsFactsFT(
    final_position UInt8 NOT NULL,
    points_earned UInt8 NOT NULL,
    DriverID UInt8,
    RaceID UInt16,
    WeatherID UInt16,
    date Date NOT NULL
) ENGINE = MergeTree()
ORDER BY (DriverID, RaceID, date)
PRIMARY KEY (DriverID, RaceID, date);

CREATE TABLE IF NOT EXISTS PitStopsFT (
    lap_number UInt8 NOT NULL,
    stop_duration Float32 NOT NULL,
    date Date NOT NULL,
    DriverID UInt8,
    RaceID UInt16
) ENGINE = MergeTree()
ORDER BY (DriverID, RaceID, date)
PRIMARY KEY (DriverID, RaceID, date);

CREATE TABLE IF NOT EXISTS DriverDT (
  DriverID UInt8,
  full_name String,
  acronym String NOT NULL,
  country_code String,
  TeamID UInt8 NOT NULL
) ENGINE = MergeTree()
PRIMARY KEY (DriverID);

CREATE TABLE IF NOT EXISTS TeamDT (
  TeamID UInt8,
  name String,
  country_code String,
  acronym String NOT NULL
) ENGINE = MergeTree()
PRIMARY KEY (TeamID);

CREATE TABLE IF NOT EXISTS RaceDT (
  RaceID UInt16,
  name String NOT NULL,
  location String,
  type String NOT NULL,
  country_code String,
  gmt_offset UInt8,
  total_laps UInt8,
  date_start Date NOT NULL,
  date_end Date NOT NULL
) ENGINE = MergeTree()
PRIMARY KEY (RaceID);

CREATE TABLE IF NOT EXISTS WeatherDT (
  WeatherID UInt16,
  humidity Float32 NOT NULL,
  pressure Float32 NOT NULL,
  rainfall Float32 NOT NULL,
  track_temperature Float32 NOT NULL,
  wind_speed Float32 NOT NULL,
  wind_direction Float32 NOT NULL
) ENGINE = MergeTree()
PRIMARY KEY (WeatherID);

CREATE TABLE IF NOT EXISTS TyreContextDT (
  TyreContextID UInt8,
  compound String NOT NULL,
  age_at_start Float32 NOT NULL,
  lap_start UInt8 NOT NULL,
  lap_end UInt8 NOT NULL,
  stint_number UInt8 NOT NULL
) ENGINE = MergeTree()
PRIMARY KEY (TyreContextID);