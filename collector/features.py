"""시간/주파수 영역 특징값. 파형 블록 하나가 들어올 때마다 계산한다."""

from __future__ import annotations

import base64

import numpy as np

MAX_SAMPLES = 8192


def decode_samples(b64: str) -> np.ndarray:
    raw = base64.b64decode(b64)
    return np.frombuffer(raw, dtype="<f4").copy()


def encode_samples(samples: np.ndarray) -> str:
    x = np.asarray(samples, dtype="<f4")
    return base64.b64encode(x.tobytes()).decode("ascii")


def fft_spectrum(samples: np.ndarray, sample_rate: int) -> tuple[list[float], list[float]]:
    x = np.asarray(samples, dtype=np.float64)
    x = x - x.mean()
    n = x.size
    mag = (np.abs(np.fft.rfft(x)) * 2.0 / n).tolist()
    freqs = np.fft.rfftfreq(n, 1.0 / sample_rate).tolist()
    return freqs, mag


def _band_peak(freqs: np.ndarray, mag: np.ndarray, center: float, width: float) -> float:
    if center <= 0:
        return 0.0
    mask = (freqs >= center - width) & (freqs <= center + width)
    return float(mag[mask].max()) if mask.any() else 0.0


def compute(samples: np.ndarray, sample_rate: int, rpm: float) -> dict[str, float]:
    x = np.asarray(samples, dtype=np.float64)
    n = x.size
    if n < 8:
        raise ValueError("샘플이 너무 적습니다")
    rms = float(np.sqrt(np.mean(x * x)))
    peak = float(np.max(np.abs(x)))
    crest = float(peak / rms) if rms > 1e-12 else 0.0
    centered = x - x.mean()
    m2 = float(np.mean(centered**2))
    kurtosis = float(np.mean(centered**4) / (m2**2) - 3.0) if m2 > 1e-18 else 0.0
    freqs = np.fft.rfftfreq(n, 1.0 / sample_rate)
    mag = np.abs(np.fft.rfft(centered)) * 2.0 / n
    f1 = rpm / 60.0
    width = max(sample_rate / n * 2, 1.0)
    return {
        "rms": round(rms, 6),
        "peak": round(peak, 6),
        "crest": round(crest, 4),
        "kurtosis": round(kurtosis, 4),
        "band1x": round(_band_peak(freqs, mag, f1, width), 6),
        "band2x": round(_band_peak(freqs, mag, 2 * f1, width), 6),
    }
