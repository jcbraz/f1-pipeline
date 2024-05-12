CREATE TABLE IF NOT EXISTS ResultsFactsFT(
    final_position UInt8 NOT NULL,
    points_earned UInt8,
    driver_id UInt8 NOT NULL,
    race_id UInt16 NOT NULL,
    weather_id Nullable(UInt16),
    date Date NOT NULL
) ENGINE = MergeTree()
ORDER BY (driver_id, race_id, date)
PRIMARY KEY (driver_id, race_id, date);

CREATE TABLE IF NOT EXISTS PitStopsFT (
    lap_number UInt8 NOT NULL,
    stop_duration Float32 NOT NULL,
    date Date NOT NULL,
    driver_id UInt8 NOT NULL,
    race_id UInt16 NOT NULL
) ENGINE = MergeTree()
ORDER BY (driver_id, race_id, date)
PRIMARY KEY (driver_id, race_id, date);

CREATE TABLE IF NOT EXISTS DriversDT (
  driver_id UInt8,
  full_name Nullable(String),
  acronym String NOT NULL,
  country_code Nullable(String),
  team_id Nullable(UInt8)
) ENGINE = MergeTree()
PRIMARY KEY (driver_id);

CREATE TABLE IF NOT EXISTS TeamsDT (
  team_id UInt8,
  name Nullable(String),
  country_code Nullable(String),
  acronym String NOT NULL
) ENGINE = MergeTree()
PRIMARY KEY (team_id);

CREATE TABLE IF NOT EXISTS RacesDT (
  race_id UInt16,
  name String NOT NULL,
  location Nullable(String),
  type String NOT NULL,
  country_code Nullable(String),
  gmt_offset Nullable(UInt8),
  total_laps Nullable(UInt8),
  date_start Date NOT NULL,
  date_end Date NOT NULL
) ENGINE = MergeTree()
PRIMARY KEY (race_id);

CREATE TABLE IF NOT EXISTS WeathersDT (
  weather_id UInt16,
  humidity Float32 NOT NULL,
  pressure Float32 NOT NULL,
  rainfall Float32 NOT NULL,
  track_temperature Float32 NOT NULL,
  wind_speed Float32 NOT NULL,
  wind_direction Float32 NOT NULL
) ENGINE = MergeTree()
PRIMARY KEY (weather_id);

CREATE TABLE IF NOT EXISTS TyreContextsDT (
  tyre_context_id UInt8,
  compound String NOT NULL,
  age_at_start Float32 NOT NULL,
  lap_start UInt8 NOT NULL,
  lap_end UInt8 NOT NULL,
  stint_number UInt8 NOT NULL
) ENGINE = MergeTree()
PRIMARY KEY (tyre_context_id);