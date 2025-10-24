package com.example.person_service.entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import java.time.LocalDate;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Table(name = "persons")
@Entity
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor

public class Person {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    UUID id;
    String firstName;
    String lastName;
    LocalDate dob;
    String taxNumber;
    @Builder.Default
    BigDecimal taxDebt = BigDecimal.ZERO;

    public void addTaxDebt(BigDecimal amount) {
        this.taxDebt = this.taxDebt.add(amount);
    }
}