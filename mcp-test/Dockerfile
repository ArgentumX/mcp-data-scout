FROM python:3.12-slim

RUN pip install --no-cache-dir fastmcp anyio

WORKDIR /app

COPY mcp_test.py .

CMD ["python", "mcp_test.py"]