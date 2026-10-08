FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=Asia/Seoul

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY collector ./collector

RUN useradd --uid 10001 --no-create-home app
USER app

EXPOSE 8001
CMD ["uvicorn", "collector.main:app", "--host", "0.0.0.0", "--port", "8001", "--proxy-headers"]
