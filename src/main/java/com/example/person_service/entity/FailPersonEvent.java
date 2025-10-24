package com.example.person_service.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FailPersonEvent {
    @Id
    private UUID id;
    private UUID correlationId;
    private String eventType;
    private int orderNumber;
    private LocalDateTime timestamp;
    private String firstName;
    private String lastName;
    private LocalDate dob;
    private String taxNumber;
}