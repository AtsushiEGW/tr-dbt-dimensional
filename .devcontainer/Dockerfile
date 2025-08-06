FROM python:3.12-slim

ARG USERNAME=dev
ARG USER_UID=1000
ARG USER_GID=1000
# 対話型パッケージインストール時のプロンプト抑制
ENV DEBIAN_FRONTEND=noninteractive

# sudo を含む最低限のパッケージ
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    sudo zsh\
    && rm -rf /var/lib/apt/lists/*

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
 && pip install --no-cache-dir -r /tmp/requirements.txt

# 非 root ユーザーに切り替え
USER ${USERNAME}

# VS Code アタッチ時に即終了しないように
CMD ["sleep", "infinity"]