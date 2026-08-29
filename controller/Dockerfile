FROM python:3.11-slim

ARG TF_VERSION=1.16.0
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl unzip ca-certificates \
    && curl -fsSL "https://releases.hashicorp.com/terraform/${TF_VERSION}/terraform_${TF_VERSION}_linux_amd64.zip" -o /tmp/tf.zip \
    && unzip /tmp/tf.zip -d /usr/local/bin \
    && rm /tmp/tf.zip \
    && apt-get purge -y curl unzip \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY controller/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY controller/controller ./controller
COPY infra/contract/ /app/infra/contract/
COPY infra/race/*.tf /app/infra/race/
COPY infra/race/.terraform.lock.hcl /app/infra/race/

WORKDIR /app/infra/race
RUN terraform init -input=false -backend=false

WORKDIR /app
CMD ["python3", "-m", "controller"]
