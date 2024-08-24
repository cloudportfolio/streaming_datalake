FROM apache/airflow:2.10.0

# Switch to root user to install packages
USER root

# Install dependencies
RUN apt-get update && apt-get install -y wget tar ca-certificates \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Download and install OpenJDK 11 for ARM from an alternative source
RUN wget https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.20+8/OpenJDK11U-jdk_aarch64_linux_hotspot_11.0.20_8.tar.gz \
    && mkdir -p /usr/lib/jvm \
    && tar -xzf OpenJDK11U-jdk_aarch64_linux_hotspot_11.0.20_8.tar.gz -C /usr/lib/jvm \
    && mv /usr/lib/jvm/jdk-11.0.20+8 /usr/lib/jvm/java-11-openjdk-arm64 \
    && rm OpenJDK11U-jdk_aarch64_linux_hotspot_11.0.20_8.tar.gz

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64

# Add JAVA_HOME to PATH
ENV PATH $JAVA_HOME/bin:$PATH

# Switch back to the airflow user
USER airflow

# Verify the installation (optional)
RUN java -version


USER airflow
RUN pip install apache-airflow==2.10.0 && \
pip install --no-cache-dir pyspark==3.5.0

COPY ./dags/ ${AIRFLOW_HOME}/dags/
COPY ./plugins/ ${AIRFLOW_HOME}/plugins/
