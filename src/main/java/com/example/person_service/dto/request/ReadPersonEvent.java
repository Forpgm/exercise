package com.example.person_service.dto.request;

import lombok.*;
import lombok.experimental.SuperBuilder;

@EqualsAndHashCode(callSuper = true)
@Data
@AllArgsConstructor
@SuperBuilder
@ToString(callSuper = true)
public class ReadPersonEvent extends BaseEvent {
    private String taxNumber;

    public ReadPersonEvent() {
        super();
    }

}
