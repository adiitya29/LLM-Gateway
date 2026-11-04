# ==============================================================================
# Stage 1: Build & Layer Extraction
# ==============================================================================
FROM eclipse-temurin:21-jdk-jammy AS builder
WORKDIR /workspace

# Install Maven
RUN apt-get update && apt-get install -y --no-install-recommends maven && rm -rf /var/lib/apt/lists/*

# Cache dependency layer
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Compile and package production artifact
COPY src src
RUN mvn clean package -DskipTests -B

# Extract Spring Boot layered jar for minimal Docker image layer diffs
WORKDIR /workspace/target
RUN java -Djarmode=layertools -jar llm-gateway-0.0.1-SNAPSHOT.jar extract

# ==============================================================================
# Stage 2: Production Minimal Runtime
# ==============================================================================
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

# Security: Run as dedicated non-root user
RUN groupadd -g 10001 gateway && \
    useradd -u 10001 -g gateway -s /bin/bash -m gateway && \
    apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

# Copy layered jar from builder stage
COPY --from=builder --chown=gateway:gateway /workspace/target/dependencies/ ./
COPY --from=builder --chown=gateway:gateway /workspace/target/spring-boot-loader/ ./
COPY --from=builder --chown=gateway:gateway /workspace/target/snapshot-dependencies/ ./
COPY --from=builder --chown=gateway:gateway /workspace/target/application/ ./

# High-Performance JVM Configuration:
# - ZGC with generational mode for ultra-low latency (<1ms pause times)
# - Container memory awareness using 75% cgroup RAM
# - Exit on OOM for container orchestrator auto-healing
# - Non-blocking secure random source
ENV JAVA_OPTS="-XX:+UseZGC -XX:+ZGenerational -XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError -Djava.security.egd=file:/dev/./urandom -Dfile.encoding=UTF-8"

USER gateway:gateway
EXPOSE 8080

# Container Healthcheck via Spring Boot Actuator
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
  CMD curl -f http://localhost:8080/actuator/health || exit 1

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS org.springframework.boot.loader.launch.JarLauncher"]

