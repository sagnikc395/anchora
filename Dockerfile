FROM python:3.13-slim AS deps

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-install-project --no-dev

FROM deps AS runtime

COPY flowforge ./flowforge
COPY main.py ./

FROM runtime AS api

EXPOSE 8000

CMD ["uvicorn", "flowforge.api.app:app", "--host", "0.0.0.0", "--port", "8000"]

FROM runtime AS worker

CMD ["python", "-m", "flowforge.worker.worker"]

FROM runtime AS starter

CMD ["python", "-m", "flowforge.api.starter"]

FROM deps AS tests

RUN uv sync --frozen --no-install-project --group dev

COPY flowforge ./flowforge
COPY main.py ./

CMD ["pytest", "-q"]
