# =========================================================
# Stage 0: base（共通の最小Pythonベース）
# =========================================================
FROM python:3.12-slim AS base

# ログ即時出力 / キャッシュ無効化（サイズ削減）
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive

# 最小ユーティリティのみ
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates curl git \
 && rm -rf /var/lib/apt/lists/*

# 依存を先に入れてレイヤを安定化（キャッシュ促進）
WORKDIR /tmp
COPY requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip \
 && pip install -r /tmp/requirements.txt

# =========================================================
# Stage 1: app（本番向け：最小）
# =========================================================
FROM base AS app

ARG USERNAME=dev
ARG USER_UID=1000
ARG USER_GID=1000

# ランタイムユーザ（開発ツールは入れない）
RUN groupadd --gid ${USER_GID} ${USERNAME} \
 && useradd --create-home --shell /bin/bash --uid ${USER_UID} --gid ${USER_GID} ${USERNAME}

WORKDIR /workspace/project
COPY . /workspace/project
RUN chown -R ${USER_UID}:${USER_GID} /workspace

USER ${USERNAME}
CMD ["python", "main.py"]

# =========================================================
# Stage 2: app-gui（開発用：GUI+noVNC+Playwright等はこちら）
# =========================================================
FROM base AS app-gui

ARG USERNAME=dev
ARG USER_UID=1000
ARG USER_GID=1000
ARG NOVNC_PASSWORD=
ARG INSTALL_PLAYWRIGHT=true  # true で Playwright ブラウザ導入（重い）

# dev 用ユーザ
RUN groupadd --gid ${USER_GID} ${USERNAME} \
 && useradd --create-home --shell /bin/bash --uid ${USER_UID} --gid ${USER_GID} ${USERNAME}

# GUI/NoVNC に必要なランタイム（最小セット）＋診断用 iproute2（不要なら削除可）
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential zsh \
    xvfb x11vnc fluxbox supervisor \
    libnss3 libatk-bridge2.0-0 libgtk-3-0 libgbm1 libasound2 \
    libxshmfence1 libxrandr2 libxdamage1 libxcomposite1 libxext6 libxfixes3 libxrender1 libxtst6 libgl1 \
    fonts-liberation libappindicator3-1 libcups2 xdg-utils \
    iproute2 \
    tree \
 && rm -rf /var/lib/apt/lists/*

# noVNC / websockify を配置し、noVNC の utils から websockify へ symlink を張る
RUN set -eux; \
    mkdir -p /opt/novnc /opt/websockify; \
    curl -fsSL https://github.com/novnc/noVNC/archive/refs/tags/v1.4.0.tar.gz \
      | tar xz -C /opt/novnc --strip-components=1; \
    curl -fsSL https://github.com/novnc/websockify/archive/refs/tags/v0.11.0.tar.gz \
      | tar xz -C /opt/websockify --strip-components=1; \
    ln -sf /opt/websockify /opt/novnc/utils/websockify

# supervisord.conf を配置（/etc/supervisor に統一）
COPY .devcontainer/supervisord.conf /etc/supervisor/supervisord.conf

# VNC パスワード生成（必須チェック）
RUN set -eux; \
    if [ -z "$NOVNC_PASSWORD" ]; then echo "NOVNC_PASSWORD is empty"; exit 1; fi; \
    mkdir -p /home/${USERNAME}/.vnc; \
    x11vnc -storepasswd "${NOVNC_PASSWORD}" /home/${USERNAME}/.vnc/passwd; \
    chown -R ${USER_UID}:${USER_GID} /home/${USERNAME}/.vnc

# アプリ配置（dev は bind mount で上書きされる想定）
WORKDIR /workspace/project
COPY . /workspace/project
RUN chown -R ${USER_UID}:${USER_GID} /workspace

# Playwright（重いので必要時のみ）
ENV DISPLAY=:99
RUN if [ "$INSTALL_PLAYWRIGHT" = "true" ]; then \
      playwright install --with-deps || true; \
    fi

USER ${USERNAME}
# dev-compose で command を supervisord に差し替える想定（ダミー）
CMD ["python", "main.py"]