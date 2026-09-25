FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install the application (src layout, hatchling build)
COPY pyproject.toml README.md ./
COPY src/ ./src/
RUN pip install --upgrade pip && pip install .

# The concrete entrypoint is chosen per service in docker-compose.yml, e.g.:
#   python -m quickbite.api.main            (FastAPI service)
#   python -m quickbite.workers.payment     (payment worker)
#   python -m quickbite.workers.restaurant   (restaurant worker)
#   python -m quickbite.workers.delivery     (delivery worker)
#   python -m quickbite.workers.notification (notification worker)
CMD ["python", "-m", "quickbite.api.main"]
