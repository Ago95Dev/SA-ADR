###########################################################
# Stage 1: Build JVM application using Maven
###########################################################
FROM quay.io/quarkus/ubi9-quarkus-mandrel-builder-image:jdk-21 AS build

WORKDIR /code

# Copy Maven wrapper and pom.xml first to leverage Docker cache
COPY risk_detector/mvnw /code/mvnw
COPY risk_detector/.mvn /code/.mvn
COPY risk_detector/pom.xml /code/
COPY risk_detector/src /code/src

# Switch to non-root user for security
USER quarkus

# Pre-download dependencies (cached if pom.xml unchanged)
RUN ./mvnw -B org.apache.maven.plugins:maven-dependency-plugin:3.8.1:go-offline

# Copy source code
COPY risk_detector/src /code/src

# Build JVM artifact (FAST-JAR), skip tests
RUN ./mvnw package -DskipTests

###########################################################
# Stage 2: Runtime image (JVM)
###########################################################
# Stage 2: Runtime image
FROM registry.access.redhat.com/ubi9/openjdk-21-runtime

# Use a directory the runtime user can write to
WORKDIR /home/quarkus/quarkus-app

# Copy Quarkus fast-jar
COPY --from=build /code/target/quarkus-app .

EXPOSE 8080

# Already running as 1001 by default
USER 1001

CMD ["java", "-jar", "quarkus-run.jar", "-Dquarkus.http.host=0.0.0.0"]
