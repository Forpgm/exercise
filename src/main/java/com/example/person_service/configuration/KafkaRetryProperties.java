package com.example.person_service.configuration;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "person.event.kafka.retry")
public record KafkaRetryProperties(
        @NotNull
        @Min(1000)
        @Max(5000)
        long interval,
        @NotNull
        @Min(0)
        @Max(5)
        int attempts
) {
}