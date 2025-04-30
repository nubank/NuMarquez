FROM gradle:7.6.2-jdk17-alpine AS builder
WORKDIR /app
COPY ./gradle gradle
COPY ./gradlew gradlew
COPY settings.gradle settings.gradle
COPY build.gradle build.gradle
RUN ./gradlew --version

# Copy project sources
COPY . .

# Build the Shadow JAR
RUN ./gradlew clean :api:shadowJar -x test

FROM eclipse-temurin:17-jre
WORKDIR /app

# Copy the built JAR from the builder stage
COPY --from=builder /app/api/build/libs/marquez-*.jar /app/marquez.jar

EXPOSE 5000
CMD ["java", "-jar", "/app/marquez.jar", "server", "marquez.yml"]