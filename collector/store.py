from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from .config import get_settings

KST = ZoneInfo("Asia/Seoul")


def parse_ts(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=KST)
    return ts.astimezone(timezone.utc)


class SensorStore:
    def __init__(self) -> None:
        s = get_settings()
        self.bucket = s.influx_bucket
        self.org = s.influx_org
        self.client = InfluxDBClient(url=s.influx_url, token=s.influx_token, org=s.influx_org, timeout=10_000)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()

    def ping(self) -> bool:
        return bool(self.client.ping())

    def write_scalars(self, machine: str, ts: datetime, values: list[dict]) -> None:
        points = []
        for item in values:
            code = item.get("code")
            if not code:
                continue
            try:
                value = float(item["value"])
            except (KeyError, TypeError, ValueError):
                continue
            p = (
                Point("scalar")
                .tag("machine", machine)
                .tag("sensor", str(code))
                .field("value", value)
                .field("unit", str(item.get("unit") or ""))
                .time(ts, WritePrecision.MS)
            )
            points.append(p)
        if points:
            self.write_api.write(bucket=self.bucket, org=self.org, record=points)

    def write_waveform(self, machine: str, payload: dict) -> None:
        ts = parse_ts(payload.get("ts"))
        p = (
            Point("waveform")
            .tag("machine", machine)
            .tag("channel", str(payload["channel"]))
            .field("sample_rate", int(payload["sampleRate"]))
            .field("n", int(payload["n"]))
            .field("rpm", float(payload.get("rpm") or 0))
            .field("unit", str(payload.get("unit") or "g"))
            .field("samples_b64", str(payload["samples"]))
            .time(ts, WritePrecision.MS)
        )
        self.write_api.write(bucket=self.bucket, org=self.org, record=p)

    def write_features(self, machine: str, channel: str, ts: datetime, feats: dict[str, float]) -> None:
        p = Point("features").tag("machine", machine).tag("channel", channel).time(ts, WritePrecision.MS)
        for key, value in feats.items():
            p = p.field(key, float(value))
        self.write_api.write(bucket=self.bucket, org=self.org, record=p)

    def write_label(self, machine: str, payload: dict) -> None:
        ts = parse_ts(payload.get("ts"))
        p = (
            Point("label")
            .tag("machine", machine)
            .field("fault", str(payload.get("fault") or "none"))
            .field("severity", float(payload.get("severity") or 0))
            .field("preset", str(payload.get("preset") or "none"))
            .time(ts, WritePrecision.MS)
        )
        self.write_api.write(bucket=self.bucket, org=self.org, record=p)

    def write_anomaly(self, machine: str, payload: dict) -> None:
        ts = parse_ts(payload.get("ts"))
        p = (
            Point("anomaly")
            .tag("machine", machine)
            .tag("model", str(payload.get("model") or "unknown"))
            .field("score", float(payload.get("score") or 0))
            .field("is_anomaly", int(bool(payload.get("isAnomaly"))))
            .field("version", str(payload.get("version") or ""))
            .time(ts, WritePrecision.MS)
        )
        self.write_api.write(bucket=self.bucket, org=self.org, record=p)

    def _rows(self, flux: str) -> list[dict]:
        tables = self.query_api.query(flux, org=self.org)
        out: list[dict] = []
        for table in tables:
            for rec in table.records:
                row = {k: rec.values[k] for k in rec.values if not k.startswith("_") or k in ("_time", "_value", "_field", "_measurement")}
                row["time"] = rec.get_time()
                out.append(row)
        return out

    def latest_scalars(self, minutes: int = 5) -> list[dict]:
        flux = f'''
from(bucket: "{self.bucket}")
  |> range(start: -{int(minutes)}m)
  |> filter(fn: (r) => r._measurement == "scalar" and r._field == "value")
  |> group(columns: ["machine", "sensor"])
  |> last()
'''
        rows = []
        for rec in self._query_records(flux):
            rows.append(
                {
                    "machine": rec["machine"],
                    "sensor": rec["sensor"],
                    "value": rec.get_value(),
                    "time": rec.get_time(),
                }
            )
        units = {
            (r["machine"], r["sensor"]): r.get_value()
            for r in self._query_records(
                f'''
from(bucket: "{self.bucket}")
  |> range(start: -{int(minutes)}m)
  |> filter(fn: (r) => r._measurement == "scalar" and r._field == "unit")
  |> group(columns: ["machine", "sensor"])
  |> last()
'''
            )
        }
        for row in rows:
            row["unit"] = units.get((row["machine"], row["sensor"]), "")
        return rows

    def latest_waveform(self, machine: str, channel: str | None = None) -> dict | None:
        ch = f' and r.channel == "{channel}"' if channel else ""
        # 수집 주기가 1시간이어도 마지막 파형 블록을 찾는다.
        flux = f'''
from(bucket: "{self.bucket}")
  |> range(start: -26h)
  |> filter(fn: (r) => r._measurement == "waveform" and r.machine == "{machine}"{ch})
  |> pivot(rowKey: ["_time", "machine", "channel"], columnKey: ["_field"], valueColumn: "_value")
  |> group()
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: 1)
'''
        recs = list(self._query_records(flux))
        if not recs:
            return None
        rec = recs[0]
        return {
            "machine": rec["machine"],
            "channel": rec["channel"],
            "ts": rec.get_time(),
            "sampleRate": int(rec["sample_rate"]),
            "n": int(rec["n"]),
            "rpm": float(rec["rpm"]),
            "unit": rec["unit"] or "g",
            "samples": rec["samples_b64"],
        }

    def features(self, machine: str, minutes: int = 30, channel: str | None = None) -> list[dict]:
        ch = f' and r.channel == "{channel}"' if channel else ""
        flux = f'''
from(bucket: "{self.bucket}")
  |> range(start: -{int(minutes)}m)
  |> filter(fn: (r) => r._measurement == "features" and r.machine == "{machine}"{ch})
  |> pivot(rowKey: ["_time", "machine", "channel"], columnKey: ["_field"], valueColumn: "_value")
  |> group()
  |> sort(columns: ["_time"])
'''
        out = []
        for rec in self._query_records(flux):
            item = {
                "ts": rec.get_time(),
                "machine": rec["machine"],
                "channel": rec["channel"],
            }
            for key in ("rms", "peak", "crest", "kurtosis", "band1x", "band2x"):
                if key in rec.values:
                    item[key] = rec[key]
            out.append(item)
        return out

    def machines(self) -> list[str]:
        flux = f'''
import "influxdata/influxdb/schema"
schema.tagValues(bucket: "{self.bucket}", tag: "machine", start: -24h)
'''
        return [str(r.get_value()) for r in self._query_records(flux)]

    def _query_records(self, flux: str):
        tables = self.query_api.query(flux, org=self.org)
        for table in tables:
            yield from table.records

    def close(self) -> None:
        self.client.close()


def iso_kst(ts: datetime | None) -> str | None:
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(KST).isoformat(timespec="milliseconds")


def lookback(minutes: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(minutes=minutes)
