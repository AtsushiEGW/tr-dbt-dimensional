FROM python:3.12-slim

ARG USERNAME=dev
ARG USER_UID=1000
ARG USER_GID=1000
# 対話型パッケージインストール時のプロンプト抑制
ENV DEBIAN_FRONTEND=noninteractive

# sudo を含む最低限のパッケージ
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    sudo zsh wget curl ca-certificates\
    # Playwright/Chromiumの動作・GUIに必要な依存
    xvfb x11vnc fluxbox supervisor \
    libnss3 libatk-bridge2.0-0 libgtk-3-0 libgbm1 libasound2 \
    libxshmfence1 libxrandr2 libxdamage1 libxcomposite1 libxext6 libxfixes3 libxrender1 libxtst6 libgl1 \
    fonts-liberation libappindicator3-1 libcups2 lsb-release xdg-utils \

    && rm -rf /var/lib/apt/lists/*

# novnc, websockifyを取得
RUN mkdir -p /opt/novnc /opt/websockify \
    && wget -qO- https://github.com/novnc/noVNC/archive/refs/tags/v1.4.0.tar.gz | tar xz -C /opt/novnc --strip-components=1 \
    && wget -qO- https://github.com/novnc/websockify/archive/refs/tags/v0.11.0.tar.gz | tar xz -C /opt/websockify --strip-components=1

# デフォルトユーザー作成
RUN groupadd --gid ${USER_GID} ${USERNAME} \
 && useradd --create-home --shell /bin/bash --uid ${USER_UID} --gid ${USER_GID} ${USERNAME} \
 && echo "${USERNAME} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/${USERNAME} \
 && chmod 0440 /etc/sudoers.d/${USERNAME}

# 作業ディレクトリ
WORKDIR /workspace/project

# bind mount 時はホスト側の権限が優先されるため chown は不要だが
# イメージだけで試す場合のために一応ディレクトリ作成
RUN mkdir -p /workspace/project \
    && chown -R ${USER_UID}:${USER_GID} /workspace

# Python 依存関係のインストール
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r /tmp/requirements.txt \
 && playwright install --with-deps


# supervisor設定
COPY .devcontainer/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

USER ${USERNAME}

# supervisor起動（Xvfb+fluxbox+x11vnc+novnc）
CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]