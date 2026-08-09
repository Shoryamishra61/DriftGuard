FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN groupadd --system driftguard && useradd --system --gid driftguard --home-dir /app driftguard

COPY requirements-api.txt requirements-migrations.txt ./
RUN python -m pip install --no-cache-dir -r requirements-api.txt -r requirements-migrations.txt

COPY --chown=driftguard:driftguard alembic.ini ./
COPY --chown=driftguard:driftguard app_api ./app_api
COPY --chown=driftguard:driftguard common_utils ./common_utils
COPY --chown=driftguard:driftguard migrations ./migrations

USER driftguard
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "import sys,urllib.request; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=3).status == 200 else 1)"

CMD ["uvicorn", "app_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
