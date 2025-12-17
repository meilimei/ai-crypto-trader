FROM python:3.12-slim

ENV POETRY_VERSION=1.8.3
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir "poetry==${POETRY_VERSION}"

COPY pyproject.toml poetry.lock* ./
RUN poetry config virtualenvs.create false \
    && poetry install --no-root --no-interaction --no-ansi

COPY src ./src

EXPOSE 8000

CMD ["uvicorn", "ai_crypto_trader.main:app", "--host", "0.0.0.0", "--port", "8000"]
