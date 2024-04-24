from pydantic import BaseModel
from datetime import datetime
from typing import List

class CarInfoSchema(BaseModel):
    driver_number: int
    rpm: int
    speed: int
    n_gear: int
    throttle: int
    brake: int
    drs: int
    date: datetime
    session_key: int
    meeting_key: int


class DriverInfoSchema(BaseModel):
    broadcast_name: str
    country_code: str
    driver_number: int
    first_name: str
    full_name: str
    headshot_url: str
    last_name: str
    meeting_key: int
    name_acronym: str
    session_key: int
    team_colour: str
    team_name: str


class GapInfoSchema(BaseModel):
    date: datetime
    driver_number: int
    gap_to_leader: float
    interval: float
    meeting_key: int
    session_key: int


class LapInfoSchema(BaseModel):
    date_start: datetime
    driver_number: int
    duration_sector_1: float
    duration_sector_2: float
    duration_sector_3: float
    i1_speed: int
    i2_speed: int
    is_pit_out_lap: bool
    lap_duration: float
    lap_number: int
    meeting_key: int
    segments_sector_1: List[int]
    segments_sector_2: List[int]
    segments_sector_3: List[int]
    session_key: int
    st_speed: int


class PitStopInfo(BaseModel):
    date: datetime
    driver_number: int
    lap_number: int
    meeting_key: int
    pit_duration: float
    session_key: int

