# MES Sensor Collector

MQTT로 들어온 센서 값을 InfluxDB에 저장하고, 진동 파형 블록의 특징값과 FFT를 조회 API로 제공합니다.

| 토픽 | 처리 |
| --- | --- |
| `mes/machines/{code}/sensors` | 스칼라 센서 (RPM, 온도 등) |
| `mes/machines/{code}/waveform` | float32 파형 블록 1개 = Influx 포인트 1개. RMS/첨도/1x/2x 계산 후 `features` 발행 |
| `mes/sim/{code}/labels` | 에뮬레이터 정답 라벨 |
| `mes/machines/{code}/anomaly` | 분석 백엔드 판정 |

원시 파형은 샘플마다 포인트를 만들지 않습니다. base64 float32 배열을 한 필드에 넣습니다.

## 실행

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
cp .env.example .env
.venv/bin/uvicorn collector.main:app --port 8001 --reload
```
