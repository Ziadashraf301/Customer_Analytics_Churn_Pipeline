FROM apache/superset:latest

USER root

# Install dependencies
RUN apt-get update && \
    apt-get install --no-install-recommends -y firefox-esr wget

ENV GECKODRIVER_VERSION=0.29.1
RUN wget -q https://github.com/mozilla/geckodriver/releases/download/v${GECKODRIVER_VERSION}/geckodriver-v${GECKODRIVER_VERSION}-linux64.tar.gz && \
    tar -xzf geckodriver-v${GECKODRIVER_VERSION}-linux64.tar.gz -C /usr/bin/ && \
    chmod 755 /usr/bin/geckodriver && \
    rm geckodriver-v${GECKODRIVER_VERSION}-linux64.tar.gz

# ✅ Add ClickHouse driver + other Python deps
RUN pip install --no-cache-dir \
      prophet \
      gevent \
      redis \
      psycopg2-binary \
      clickhouse-connect
      
USER superset
