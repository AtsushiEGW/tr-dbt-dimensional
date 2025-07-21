# docker/metabase/Dockerfile
FROM metabase/metabase:v0.52.4

# プラグイン配置先ディレクトリ
ENV MB_PLUGINS_DIR=/plugins
RUN mkdir -p ${MB_PLUGINS_DIR}

# DuckDB ドライバを公式リリースから取得
RUN curl -fSL \
    https://github.com/MotherDuck-Open-Source/metabase_duckdb_driver/releases/download/0.2.12/duckdb.metabase-driver.jar \
  -o ${MB_PLUGINS_DIR}/duckdb.metabase-driver.jar \
  && chmod 644 ${MB_PLUGINS_DIR}/duckdb.metabase-driver.jar

EXPOSE 3000