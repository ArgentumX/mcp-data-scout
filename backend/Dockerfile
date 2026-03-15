FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY connectors/ ./connectors/
COPY index/ ./index/
COPY search/ ./search/
COPY server/ ./server/
COPY scripts/ ./scripts/
COPY scripts/entrypoint.sh /app/scripts/entrypoint.sh

EXPOSE 8000

RUN chmod +x /app/scripts/entrypoint.sh

ENTRYPOINT ["/app/scripts/entrypoint.sh"]
