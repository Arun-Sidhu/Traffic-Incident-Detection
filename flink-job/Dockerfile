# flink-job/Dockerfile
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml /build/pom.xml
COPY src /build/src
RUN mvn -DskipTests package

FROM flink:1.18.1-scala_2.12-java17
USER root
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY --from=build /build/target/traffic-incident-flink-0.1.0.jar /job.jar
COPY submit-jobs.sh /submit-jobs.sh
RUN sed -i 's/\r$//' /submit-jobs.sh && chmod +x /submit-jobs.sh

ENTRYPOINT ["/bin/bash", "/submit-jobs.sh"]