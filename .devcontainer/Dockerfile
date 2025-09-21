FROM python:3.12-slim

# ====== Build-time args ======
ARG USERNAME=dev
ARG USER_UID=1000
ARG USER_GID=1000
ARG ENABLE_GUI=false
ARG NOVNC_PASSWORD=

ENV DEBIAN_FRONTEND=noninteractive

# ====== Base packages (common) ======
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    build-essential sudo zsh wget curl ca-certificates git \
 && rm -rf /var/lib/apt/lists/*

# ====== GUI / noVNC (dev only when ENABLE_GUI=true) ======
RUN if [ "$ENABLE_GUI" = "true" ]; then \
      set -eux; \
      apt-get update && apt-get install -y --no-install-recommends \
        xvfb x11vnc fluxbox supervisor \
        libnss3 libatk-bridge2.0-0 libgtk-3-0 libgbm1 libasound2 \
        libxshmfence1 libxrandr2 libxdamage1 libxcomposite1 libxext6 libxfixes3 libxrender1 libxtst6 libgl1 \
        fonts-liberation libappindicator3-1 libcups2 lsb-release xdg-utils \
      && rm -rf /var/lib/apt/lists/* \
      && mkdir -p /opt/novnc /opt/websockify \
      && wget -qO- https://github.com/novnc/noVNC/archive/refs/tags/v1.4.0.tar.gz | tar xz -C /opt/novnc --strip-components=1 \
      && wget -qO- https://github.com/novnc/websockify/archive/refs/tags/v0.11.0.tar.gz | tar xz -C /opt/websockify --strip-components=1 ; \
    fi

# ====== Non-root user ======
RUN groupadd --gid ${USER_GID} ${USERNAME} \
 && useradd --create-home --shell /bin/bash --uid ${USER_UID} --gid ${USER_GID} ${USERNAME} \
 && echo "${USERNAME} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/${USERNAME} \
 && chmod 0440 /etc/sudoers.d/${USERNAME}

WORKDIR /workspace/project
RUN mkdir -p /workspace/project && chown -R ${USER_UID}:${USER_GID} /workspace

# ====== Python deps ======
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r /tmp/requirements.txt

# ====== Bring app code into image (for production) ======
# ※ dev では compose.dev の bind mount が上書きします
COPY . /workspace/project
RUN chown -R ${USER_UID}:${USER_GID} /workspace

# ====== Supervisord config (dev-only runtime will use it) ======
COPY .devcontainer/supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# ====== Playwright browsers (optional; requirements.txtに playwright が必要) ======
USER ${USERNAME}
ENV DISPLAY=:99
RUN playwright install --with-deps || true

# ====== Dev-only: prepare x11vnc password file when GUI is enabled ======
# Dev-only: prepare x11vnc password file when GUI is enabled
RUN if [ "$ENABLE_GUI" = "true" ]; then \
      if [ -z "$NOVNC_PASSWORD" ]; then echo "NOVNC_PASSWORD is empty"; exit 1; fi; \
      mkdir -p /home/${USERNAME}/.vnc && \
      x11vnc -storepasswd "${NOVNC_PASSWORD}" /home/${USERNAME}/.vnc/passwd && \
      chown -R ${USER_UID}:${USER_GID} /home/${USERNAME}/.vnc ; \
    fi
USER ${USERNAME}

# ====== Default command (prod) ======
USER ${USERNAME}
CMD ["python", "main.py"]