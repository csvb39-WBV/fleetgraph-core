FROM python:3.11.9-slim

WORKDIR /app

ENV PYTHONPATH=/app/src

COPY src/ /app/src

RUN pip install --no-cache-dir fastapi==0.115.12 uvicorn==0.30.6

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "fleetgraph_core.runtime.runtime_http_api:app", "--host", "0.0.0.0", "--port", "8000"]