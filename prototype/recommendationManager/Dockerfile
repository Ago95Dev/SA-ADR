###########################################################
# Stage 1: Build JVM application using Maven
###########################################################
FROM quay.io/quarkus/ubi9-quarkus-mandrel-builder-image:jdk-21 AS build
# Builder image: Maven + JDK

WORKDIR /code

# Copy Maven wrapper, .mvn folder, and pom.xml
COPY recommendation_manager/mvnw /code/mvnw
COPY recommendation_manager/.mvn /code/.mvn
COPY recommendation_manager/pom.xml /code/

# Switch to non-root user
USER quarkus

# Pre-download dependencies for faster builds
RUN ./mvnw -B org.apache.maven.plugins:maven-dependency-plugin:3.8.1:go-offline

# Copy source code
COPY recommendation_manager/src /code/src

# Build JVM artifact (FAST-JAR)
RUN ./mvnw package -DskipTests

###########################################################
# Stage 2: Runtime image (JVM)
###########################################################
FROM registry.access.redhat.com/ubi9/openjdk-21-runtime

# Use a directory writable by the default runtime user
WORKDIR /home/quarkus/recommendation-app

# Copy Quarkus fast-jar from build stage
COPY --from=build /code/target/quarkus-app .

# Expose HTTP port
EXPOSE 8080

# Already runs as 1001 by default
USER 1001

# Run Quarkus JVM app
CMD ["java", "-jar", "quarkus-run.jar", "-Dquarkus.http.host=0.0.0.0"]
