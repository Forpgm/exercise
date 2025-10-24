package com.example.person_service.dto.request;

import com.example.person_service.constant.EventType;
import jakarta.persistence.Id;

import java.time.LocalDateTime;
import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class BaseEvent {
    @Id
    private UUID id;
    private UUID correlationId;
    private EventType eventType;
    private int orderNumber;
    private LocalDateTime timestamp;
}