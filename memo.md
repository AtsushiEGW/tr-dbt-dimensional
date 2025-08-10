chatgpt に質問する用の文章

ファイルの中身は以下を実行して取得



docker で環境構築をしています。docker 関連のファイル等の中身を送りますので、まずは現状を把握してください。

---
フォルダ構成

PROJECT/
    - .devcontainer/
        - devcontainer.json
        - Dockerfile
    - project_folder/
    - data/ # csv ファイル保管
    - ingestion/ # csv 取り込み用
    - .env
    - docker-compose.yml
    - requirements.txt


---
.devcontainer/devcontainer.json ===

{
  "name": "dimensional modeling container",
  "dockerComposeFile": ["../docker-compose.yml"],
  "service": "app",
  "workspaceFolder": "/workspace/project",
  "shutdownAction": "stopCompose",

  "features":{
    "ghcr.io/devcontainers/features/common-utils:2": {
      "installZsh": true,
      "configureZshAsDefaultShell": true,
      "zshTheme": "agnoster",
      "installOhMyZsh": true,
      "installOhMyZshConfig": true,
      "ohMyZshPlugins": "git zsh-autosuggestions zsh-syntax-highlighting",
      "upgradePackages": true
    }
  },

  // VS Code ポートフォワード
  "forwardPorts": [5432, 3000],
  "remoteUser": "dev",

  "customizations": {
    "vscode": {
      "extensions": [
        "ms-python.python",
        "ms-python.vscode-pylance",
        "ms-toolsai.jupyter",
        "ms-toolsai.jupyter-keymap",
        "ms-toolsai.jupyter-renderers",
        "ms-toolsai.vscode-jupyter-cell-tags"
      ],
      "settings": {
        "python.defaultInterpreterPath": "/usr/local/bin/python",
        "python.analysis.typeCheckingMode": "basic",
        "python.analysis.autoSearchPaths": true,
        "python.analysis.useLibraryCodeForTypes": true,
        "python.formatting.provider": "black",
        "editor.formatOnSave": true,
        "terminal.integrated.shell.linux": "/bin/bash/zsh"
      }
    }
  }
}


--- 
.devcontainer/Dockerfile


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


---
docker-compose.yml

version: '3.8'
services:
  app:
    build:
      context: .
      dockerfile: .devcontainer/Dockerfile
    # ホストのUID/GIDをマウント時に合わせる（.envまたはシェル環境でLOCAL_UID/LOCAL_GIDを定義）
    user: "${LOCAL_UID:-1000}:${LOCAL_GID:-1000}"
    volumes:
      # プロジェクトルート全体をコンテナの /workspace/project にマウント
      - ./:/workspace/project:cached
      # dbt 等が参照する .env ファイル
      - ./.env:/workspace/project/.env:ro
    env_file:
      - ./.env
    working_dir: /workspace/project
    command: sleep infinity
    depends_on:
      - db
    networks:
      - metanet1

  db:
    image: postgres:latest
    container_name: postgres
    hostname: postgres
    restart: always
    env_file:
      - ./.env
    volumes:
      - pgdata:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    networks:
      - metanet1

  metabase:
    image: metabase/metabase:latest
    container_name: metabase
    hostname: metabase
    restart: always
    depends_on:
      - db
    environment:
      - MB_DB_TYPE=postgres
      - MB_DB_DBNAME=${POSTGRES_DB:-metabase}
      - MB_DB_HOST=postgres
      - MB_DB_PORT=${POSTGRES_PORT:-5432}
      - MB_DB_USER=${POSTGRES_USER:-postgres}
      - MB_DB_PASS=${POSTGRES_PASSWORD:-postgres}
    volumes:
      # javaのエントロピー供給のためにホストの /dev/urandom をマウント
      - /dev/urandom:/dev/random:ro
    healthcheck:
      test: ["CMD-SHELL", "curl --fail -I http://localhost:3000/api/health || exit 1"]
      interval: 30s
      timeout: 10s
      retries: 5
    networks:
      - metanet1
    ports:
      - "3000:3000"

networks:
  metanet1:
    driver: bridge

volumes:
  pgdata:


---
.env

POSTGRES_USER=admin
POSTGRES_PASSWORD=pass1234
POSTGRES_DB=adventureworks
SUPERSET_ADMIN_PASSWORD=admin1234
POSTGRES_PORT=5432



---
requirements.txt

dbt-core
sqlfluff
sqlfluff-templater-dbt
dbt-postgres
python-dotenv
ruff
psycopg2-binary
requests
pandas





---


# postgres 関連

dbt + postgresql で データ分析基盤の構築をしていきたい。
現在のプロジェクトファイルの構成は以下のようになっています。

- project_root/
  - .devcontianer/
  - dbt_project
  - ingestion/ csv ファイルの取り込み等
  - raw_data/
    - spotify/
      - spotify1.csv
      - spotify2.csv
      :


ここで、一つのテーブルが複数の csv ファイルになっているため、これを統合して postgresql に取り込みたい。
どの様に python を書けばよいでしょうか？
