# syntax=docker/dockerfile:1.4
FROM mcr.microsoft.com/devcontainers/python:1-3.11-bullseye

ENV PYTHONUNBUFFERED=1

# OS 依存のライブラリ
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        git \
        curl \
        vim \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# 依存パッケージはキャッシュマンとしてインストール
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt 


# ユーザー設定
USER vscode


