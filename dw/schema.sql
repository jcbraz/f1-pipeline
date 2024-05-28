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
    pit_duration Float32 NOT NULL,
    date DateTime NOT NULL,
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
  race_name String NOT NULL,
  circuit_name String NOT NULL,
  location Nullable(String),
  type String NOT NULL,
  country_name Nullable(String),
  total_laps Nullable(UInt8),
  timestamp_start DateTime NOT NULL,
  timestamp_end DateTime NOT NULL
) ENGINE = MergeTree()
PRIMARY KEY (race_id);

CREATE TABLE IF NOT EXISTS WeatherDT (
  weather_id UInt16,
  humidity Float32 NOT NULL,
  pressure Float32 NOT NULL,
  rainfall Float32 NOT NULL,
  air_temperature Float32 NOT NULL,
  track_temperature Float32 NOT NULL,
  wind_speed Float32 NOT NULL,
  wind_direction Float32 NOT NULL,
  timestamp DateTime NOT NULL
) ENGINE = MergeTree()
PRIMARY KEY (weather_id, timestamp);

CREATE TABLE IF NOT EXISTS DailyWeatherDT (
  weather_id UInt16,
  humidity Float32 NOT NULL,
  pressure Float32 NOT NULL,
  rainfall Float32 NOT NULL,
  air_temperature Float32 NOT NULL,
  track_temperature Float32 NOT NULL,
  wind_speed Float32 NOT NULL,
  wind_direction Float32 NOT NULL,
  date Date NOT NULL
) ENGINE = MergeTree()
PRIMARY KEY (weather_id, date);

CREATE TABLE IF NOT EXISTS StintsDT (
  stint_id UInt16,
  stint_number UInt8 NOT NULL,
  compound String NOT NULL,
  tyre_age_at_start Float32 NOT NULL,
  lap_start UInt16 NOT NULL,
  lap_end UInt16 NOT NULL,
  race_id UInt16 NOT NULL,
  driver_id UInt8 NOT NULL
) ENGINE = MergeTree()
PRIMARY KEY (stint_id);
