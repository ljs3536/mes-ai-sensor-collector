from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel


class Schema(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class ScalarOut(Schema):
    machine: str
    sensor: str
    value: float
    unit: str = ""
    time: datetime | None = None


class WaveformOut(Schema):
    machine: str
    channel: str
    ts: datetime
    sample_rate: int
    n: int
    rpm: float
    unit: str
    samples: list[float]
    freqs: list[float] = Field(default_factory=list)
    magnitudes: list[float] = Field(default_factory=list)


class FeatureOut(Schema):
    ts: datetime
    machine: str
    channel: str
    rms: float | None = None
    peak: float | None = None
    crest: float | None = None
    kurtosis: float | None = None
    band1x: float | None = Field(default=None, alias="band1x")
    band2x: float | None = Field(default=None, alias="band2x")
