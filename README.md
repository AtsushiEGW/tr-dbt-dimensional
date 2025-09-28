


# docker 立ち上げ
```bash
docker compose --env-file .env -f docker-compose.yml -f docker-compose.dev.yml up -d db redis app

# postgres コンテナに入り、 postgres に接続してDBを作成する
docker compose --env-file .env -f docker-compose.yml -f docker-compose.dev.yml exec db psql -U admin -d postgres -c "CREATE DATABASE superset;"

# superset web の起動,初期化
docker compose --env-file .env -f docker-compose.yml -f docker-compose.dev.yml up -d superset

docker compose --env-file .env -f docker-compose.yml -f docker-compose.dev.yml exec superset superset db upgrade

# 管理者作成（環境変数はコンテナ内で展開させる）
docker compose --env-file .env -f docker-compose.yml -f docker-compose.dev.yml \
  exec superset sh -lc '/app/.venv/bin/superset fab create-admin \
    --username "$SUPERSET_ADMIN_USERNAME" \
    --firstname "$SUPERSET_ADMIN_FIRST_NAME" \
    --lastname  "$SUPERSET_ADMIN_LAST_NAME" \
    --email     "$SUPERSET_ADMIN_EMAIL" \
    --password  "$SUPERSET_ADMIN_PASSWORD"'

docker compose exec superset superset init

```




-> この後にvscode から Dev Containers: Attach to running Container... -> app を選択

