from __future__ import annotations

import json
import logging
import socket
import threading
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

from .config import get_settings
from .features import MAX_SAMPLES, compute, decode_samples
from .store import SensorStore, parse_ts

log = logging.getLogger("collector.mqtt")
SHARE = "mes-collector"


class Ingest:
    def __init__(self, store: SensorStore):
        self.store = store
        self.prefix = get_settings().mqtt_topic_prefix
        self.client: mqtt.Client | None = None
        self._lock = threading.Lock()

    def start(self) -> None:
        s = get_settings()
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"mes-collector-{socket.gethostname()}")
        client.on_connect = self._on_connect
        client.on_disconnect = lambda *_: log.warning("MQTT 연결 끊김, 재연결 대기")
        client.on_message = self._on_message
        client.reconnect_delay_set(1, 30)
        client.connect_async(s.mqtt_host, s.mqtt_port, keepalive=30)
        client.loop_start()
        self.client = client

    def stop(self) -> None:
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()

    def connected(self) -> bool:
        return bool(self.client and self.client.is_connected())

    def _on_connect(self, client, _u, _f, reason_code, _p):
        if reason_code.is_failure:
            log.error("MQTT 연결 거부: %s", reason_code)
            return
        topics = [
            (f"$share/{SHARE}/{self.prefix}/machines/+/sensors", 0),
            (f"$share/{SHARE}/{self.prefix}/machines/+/waveform", 0),
            (f"$share/{SHARE}/{self.prefix}/machines/+/anomaly", 1),
            (f"$share/{SHARE}/{self.prefix}/sim/+/labels", 0),
        ]
        client.subscribe(topics)
        log.info("MQTT 구독 시작")

    def _on_message(self, _c, _u, msg):
        parts = msg.topic.split("/")
        try:
            payload = json.loads(msg.payload)
        except ValueError:
            log.warning("JSON 아님 %s", msg.topic)
            return
        try:
            if parts[-1] == "sensors" and parts[-3] == "machines":
                self._sensors(parts[-2], payload)
            elif parts[-1] == "waveform":
                self._waveform(parts[-2], payload)
            elif parts[-1] == "labels":
                self.store.write_label(parts[-2], payload)
            elif parts[-1] == "anomaly":
                self.store.write_anomaly(parts[-2], payload)
        except Exception:
            log.exception("수집 실패 %s", msg.topic)

    def _sensors(self, machine: str, payload: dict) -> None:
        values = payload.get("values") or []
        if not isinstance(values, list):
            return
        self.store.write_scalars(machine, parse_ts(payload.get("ts")), values)

    def _waveform(self, machine: str, payload: dict) -> None:
        n = int(payload.get("n") or 0)
        if n < 8 or n > MAX_SAMPLES:
            log.warning("%s 파형 샘플 수 거부 n=%s", machine, n)
            return
        samples = decode_samples(payload["samples"])
        if samples.size != n:
            payload = {**payload, "n": int(samples.size)}
            n = int(samples.size)
        self.store.write_waveform(machine, payload)
        ts = parse_ts(payload.get("ts"))
        feats = compute(samples, int(payload["sampleRate"]), float(payload.get("rpm") or 0))
        channel = str(payload["channel"])
        self.store.write_features(machine, channel, ts, feats)
        self.publish_features(machine, channel, ts, feats)

    def publish_features(self, machine: str, channel: str, ts: datetime, feats: dict) -> None:
        if self.client is None:
            return
        body = {
            "ts": ts.astimezone(timezone.utc).isoformat(timespec="milliseconds"),
            "channel": channel,
            **feats,
        }
        with self._lock:
            self.client.publish(
                f"{self.prefix}/machines/{machine}/features",
                json.dumps(body),
                qos=0,
            )
