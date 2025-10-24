package com.example.person_service.dto.response;

import lombok.*;
import lombok.experimental.FieldDefaults;

import java.math.BigDecimal;
import java.util.UUID;


@Data
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
public class PersonResponse {
    UUID id;
    String firstName;
    String lastName;
    Integer age;
    String taxNumber;
    BigDecimal taxDebt;
}
