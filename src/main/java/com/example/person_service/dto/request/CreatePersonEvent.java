package com.example.person_service.dto.request;

import lombok.experimental.SuperBuilder;
import java.time.LocalDate;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class CreatePersonEvent extends BaseEvent {
    String firstName;
    String lastName;
    LocalDate dob;
    String taxNumber;

    public CreatePersonEvent() {
        super();
    }
}
