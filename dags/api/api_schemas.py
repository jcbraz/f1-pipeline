from pydantic import BaseModel
from typing import Optional


class SessionKeysRange(BaseModel):
    start: int
    end: int


class DriverNumbersRange(BaseModel):
    start: int
    end: int


class UrlDetailsSchema(BaseModel):
    base_url: str
    attributes_to_remove: list[str]
    session_keys_range: Optional[SessionKeysRange] = None
    driver_numbers_range: Optional[DriverNumbersRange] = None
