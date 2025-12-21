###########################################################
# Stage 1: Build JVM application using Maven
###########################################################
FROM quay.io/quarkus/ubi9-quarkus-mandrel-builder-image:jdk-21 AS build
# Usato solo come builder (Maven + JDK)

WORKDIR /code

# Copy Maven wrapper, .mvn folder, and pom.xml
COPY recommendation_manager/mvnw /code/mvnw
COPY recommendation_manager/.mvn /code/.mvn
COPY recommendation_manager/pom.xml /code/

# Switch to non-root user
USER quarkus

# Pre-download dependencies
RUN ./mvnw -B org.apache.maven.plugins:maven-dependency-plugin:3.8.1:go-offline

# Copy source code
COPY recommendation_manager/src /code/src

# Build JVM artifact (FAST-JAR)
RUN ./mvnw package -DskipTests

###########################################################
# Stage 2: Runtime image (JVM)
###########################################################
FROM registry.access.redhat.com/ubi9/openjdk-21-runtime

WORKDIR /work/

# Copy Quarkus fast-jar structure
COPY --from=build /code/target/quarkus-app /work/quarkus-app

# Fix permissions
RUN chown -R 1001:root /work \
    && chmod -R g+rwX /work

EXPOSE 8080

USER 1001

# Run Quarkus JVM app
CMD ["java", "-jar", "/work/quarkus-app/quarkus-run.jar", "-Dquarkus.http.host=0.0.0.0"]
