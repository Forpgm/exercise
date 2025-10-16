# Base image
FROM eclipse-temurin:17-jdk-alpine

# Set working directory
WORKDIR /app

# Copy Maven build
COPY target/person-service-0.0.1-SNAPSHOT.jar app.jar

# Expose port Spring Boot
EXPOSE 8080

# Run app
ENTRYPOINT ["java", "-jar", "app.jar"]
