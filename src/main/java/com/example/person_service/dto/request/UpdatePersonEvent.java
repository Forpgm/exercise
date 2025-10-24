package com.example.person_service.dto.request;

import java.time.LocalDate;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@EqualsAndHashCode(callSuper = true)
@Data
@SuperBuilder
@ToString(callSuper = true)
public class UpdatePersonEvent extends BaseEvent {
    String firstName;
    String lastName;
    LocalDate dob;
    String taxNumber;

    public UpdatePersonEvent() {
        super();
    }
}