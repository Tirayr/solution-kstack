FROM python:3.13-slim
# Install Java for PySpark
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    curl \
    gcc \
    g++ \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*


RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain=1.65.0 && \
    echo 'source $HOME/.cargo/env' >> $HOME/.bashrc && \
    . $HOME/.cargo/env

# Add Cargo to PATH
ENV PATH="/root/.cargo/bin:${PATH}"

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Set up working directory
WORKDIR /app

# Copy requirements file
COPY application.txt .

# Install Python dependencies
RUN . $HOME/.cargo/env && \
    pip install --no-cache-dir -U pip && \
    pip install --no-cache-dir -r application.txt

# Install Spark (updated to version 3.5.5)
ENV SPARK_VERSION=3.5.5
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
RUN curl -sL --retry 3 "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" | tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    chmod -R 775 ${SPARK_HOME}

# Copy the script
COPY src/minhash_deduplication_flow.py .

# Cutstom env vars added
ENV SPARK_VERSION=3.5
ENV GCP_PROJECT="resounding-keel-378411"

# Configure Prefect
ENV PREFECT_UI_API_URL="http://0.0.0.0:4200/api"
ENV PREFECT_API_URL="http://0.0.0.0:4200/api"
ENV PREFECT_SERVER_API_HOST="0.0.0.0"

# Expose Prefect ports
EXPOSE 4200 8959

# Set environment variables for the script
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-*.zip:$PYTHONPATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Create an entrypoint script
RUN echo '#!/bin/bash\n\
prefect server start --host 0.0.0.0 &\n\
sleep 30\n\
python minhash_deduplication_flow.py "$@"\n\
' > /app/entrypoint.sh && \
    chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
