from dataclasses import dataclass
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame


@dataclass
class Job(ABC):
    spark: SparkSession
    parquet_file_paths: dict[str, str]

    @abstractmethod
    def elaborate(self) -> DataFrame | None:
        pass

    @abstractmethod
    def execute(self) -> None:
        pass
