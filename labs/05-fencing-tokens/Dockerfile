# One image for both worker.py and storage.py.
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/ scripts/

# Workers must be PID 1 so `docker kill -s STOP` hits them directly (no shell
# in between swallowing signals). Exec-form commands in compose guarantee that.
