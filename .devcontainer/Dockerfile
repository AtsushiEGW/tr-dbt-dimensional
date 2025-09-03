# Playwright 公式イメージ（Ubuntu 22.04 / jammy）
FROM mcr.microsoft.com/playwright/python:v1.47.0-jammy

ENV DEBIAN_FRONTEND=noninteractive
ENV DISPLAY=:99
WORKDIR /workspace/project

# root で GUI/VNC 周りとユーティリティを導入
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        xvfb x11vnc fluxbox supervisor \
        wget curl ca-certificates \
        fonts-noto-cjk fonts-noto-color-emoji \
        zsh \
    && chsh -s /usr/bin/zsh pwuser \
    && rm -rf /var/lib/apt/lists/*

# noVNC と websockify(静的配信用) を配置
RUN mkdir -p /opt/novnc /opt/websockify \
    && wget -qO- https://github.com/novnc/noVNC/archive/refs/tags/v1.4.0.tar.gz | tar xz -C /opt/novnc --strip-components=1

# Python パッケージ（requirements）と websockify(Python版) を導入
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /tmp/requirements.txt \
    && pip install --no-cache-dir websockify

# supervisor 設定
COPY .devcontainer/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# 作業フォルダ作成と権限
RUN mkdir -p /workspace/project \
    && chown -R 1001:1001 /workspace

# 以降は Playwright 既定ユーザー（pwuser, uid=1001）で動かす
USER pwuser

# Playwright 公式イメージはブラウザ同梱のため install 不要。
# ただし将来別ブラウザを増やしたい場合は（必要時のみ）:
# RUN playwright install chromium

CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]