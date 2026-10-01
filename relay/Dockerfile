FROM python:3.14-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
ENV RELAY_CONFIG_FILE_PATH="/var/config/config.yaml"
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY config.yaml /var/config/config.yaml
COPY app/ ./app
EXPOSE 8080
CMD ["python3", "-m", "app"]
