FROM python:3.9-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UPLOAD_FOLDER=/app/data/uploads
ENV REPORT_FOLDER=/app/data/reports

RUN mkdir -p /app/data/uploads /app/data/reports

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

RUN adduser --disabled-password --gecos '' appuser
RUN chown -R appuser:appuser /app
RUN chmod -R 755 /app/data

USER appuser

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]