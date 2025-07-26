FROM openjdk:19-bullseye

USER root

RUN apt-get update \
 && apt-get install -y --no-install-recommends libstdc++6 curl \
 && rm -rf /var/lib/apt/lists/*

ENV MB_PLUGINS_DIR=/plugins
WORKDIR /app
RUN mkdir -p ${MB_PLUGINS_DIR}

# Metabase 本体と DuckDB プラグインのダウンロード
RUN curl -fSL https://downloads.metabase.com/v0.52.4/metabase.jar -o /app/metabase.jar \
 && curl -fSL \
     https://github.com/MotherDuck-Open-Source/metabase_duckdb_driver/releases/download/0.2.12/duckdb.metabase-driver.jar \
   -o ${MB_PLUGINS_DIR}/duckdb.metabase-driver.jar \
 && chmod 644 ${MB_PLUGINS_DIR}/duckdb.metabase-driver.jar

USER 1000
EXPOSE 3000
CMD ["java","-jar","/app/metabase.jar"]