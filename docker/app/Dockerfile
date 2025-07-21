FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED 1


# [Optional] Uncomment this section to install additional OS packages.
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends  \
    vim git build-essential zsh tmux curl wget sudo \
    zsh-syntax-highlighting zsh-autosuggestions \
    && rm -rf /var/lib/apt/lists/* 

RUN curl -sS https://starship.rs/install.sh | sh -s -- --yes

# [Optional] If your requirements rarely change, uncomment this section to add them to the image.
COPY requirements.txt /tmp/pip-tmp/
RUN pip --disable-pip-version-check --no-cache-dir install -r /tmp/pip-tmp/requirements.txt \
   && rm -rf /tmp/pip-tmp \
   && pip install --upgrade pip

ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m -s /usr/bin/zsh $USERNAME \
    && echo "$USERNAME ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

ENV HOME=/home/${USERNAME}

WORKDIR ${HOME}

USER $USERNAME




