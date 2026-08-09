FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    HF_HUB_DISABLE_TELEMETRY=1

WORKDIR /app

RUN groupadd --system driftguard && useradd --system --gid driftguard --home-dir /app driftguard

COPY requirements-worker.txt ./
RUN python -m pip install --no-cache-dir -r requirements-worker.txt

COPY --chown=driftguard:driftguard app_worker ./app_worker
COPY --chown=driftguard:driftguard common_utils ./common_utils
COPY --chown=driftguard:driftguard data/baselines ./data/baselines

ARG EMBEDDING_MODEL_SOURCE=sentence-transformers/all-MiniLM-L6-v2
ARG EMBEDDING_MODEL_REVISION=1110a243fdf4706b3f48f1d95db1a4f5529b4d41
ENV EMBEDDING_MODEL_SOURCE=${EMBEDDING_MODEL_SOURCE} \
    EMBEDDING_MODEL_REVISION=${EMBEDDING_MODEL_REVISION} \
    EMBEDDING_MODEL_PATH=/app/models/all-MiniLM-L6-v2

RUN python -m app_worker.preload_model && chown -R driftguard:driftguard /app/models

ENV EMBEDDING_MODEL=/app/models/all-MiniLM-L6-v2 \
    HF_HUB_OFFLINE=1 \
    TRANSFORMERS_OFFLINE=1

USER driftguard
CMD ["python", "-m", "app_worker.main"]
