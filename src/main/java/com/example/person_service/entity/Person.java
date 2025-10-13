package com.example.person_service.entity;
import jakarta.persistence.Column;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import lombok.*;
import java.time.LocalDate;
import jakarta.persistence.Entity;
import jakarta.persistence.*;

@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Person {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    String id;
    private String firstName;
    private String lastName;
    private LocalDate dob;
    @Column(unique = true, nullable = false, updatable = false)
    private String taxNumber;
}
