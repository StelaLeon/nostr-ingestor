FROM sbtscala/scala-sbt:eclipse-temurin-jammy-21.0.1_12_1.9.7_3.3.1 AS builder

WORKDIR /app

# Copy build files
COPY build.sbt .
COPY project ./project
COPY src ./src

# Build the assembly JAR
RUN sbt clean assembly

# Runtime stage - use smaller JRE image
FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

COPY --from=builder /app/target/scala-*/*-assembly*.jar /app/app.jar

RUN groupadd -r appuser && useradd -r -g appuser appuser

# Create directory for Google Cloud credentials
RUN mkdir -p /app/config && chown -R appuser:appuser /app
USER appuser

# Environment variables for Google Cloud and BigQuery
ENV GOOGLE_CLOUD_PROJECT=""
ENV BIGQUERY_DATASET=""
ENV BIGQUERY_TABLE=""
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/config/gcp-key.json"

# Optional: Nostr relay configuration
ENV NOSTR_RELAYS="wss://relay.damus.io,wss://nos.lol"

# Pipeline configuration
ENV BATCH_SIZE="1000"
ENV BATCH_TIMEOUT="30 seconds"
ENV PIPELINE_PARALLELISM="4"
ENV BUFFER_SIZE="100"

# Set JVM options
ENV JAVA_OPTS="-Xmx2G -Xms1G"

# Expose port if your application needs it (adjust as needed)
# EXPOSE 8080

# Run the application
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]
