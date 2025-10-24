package com.example.person_service.utils;

import com.example.person_service.constant.EventType;
import com.example.person_service.dto.request.*;
import com.example.person_service.entity.FailPersonEvent;
import org.springframework.stereotype.Component;


@Component
public class PersonEventMapper {
    public CreatePersonRequest toRequest(CreatePersonEvent event) {
        return CreatePersonRequest.builder()
                .firstName(event.getFirstName())
                .lastName(event.getLastName())
                .dob(event.getDob())
                .taxNumber(event.getTaxNumber())
                .build();
    }

    public UpdatePersonRequest toUpdateRequest(UpdatePersonEvent event) {
        return UpdatePersonRequest.builder()
                .firstName(event.getFirstName())
                .lastName(event.getLastName())
                .dob(event.getDob())
                .taxNumber(event.getTaxNumber())
                .build();
    }

    public FailPersonEvent extractCreatePersonEvent(CreatePersonEvent event) {
        return FailPersonEvent.builder()
                .id(event.getId())
                .correlationId(event.getCorrelationId())
                .eventType(String.valueOf(event.getEventType()))
                .orderNumber(event.getOrderNumber())
                .timestamp(event.getTimestamp())
                .firstName(event.getFirstName())
                .lastName(event.getLastName())
                .dob(event.getDob())
                .taxNumber(event.getTaxNumber())
                .build();
    }

    public CreatePersonEvent extractFailPersonEvent(FailPersonEvent event) {
        return CreatePersonEvent.builder()
                .id(event.getId())
                .correlationId(event.getCorrelationId())
                .eventType(EventType.valueOf(event.getEventType()))
                .orderNumber(event.getOrderNumber())
                .timestamp(event.getTimestamp())
                .firstName(event.getFirstName())
                .lastName(event.getLastName())
                .dob(event.getDob())
                .taxNumber(event.getTaxNumber())
                .build();
    }

    public FailPersonEvent extractReadPersonEvent(ReadPersonEvent event) {
        return FailPersonEvent.builder()
                .id(event.getId())
                .correlationId(event.getCorrelationId())
                .eventType(String.valueOf(event.getEventType()))
                .orderNumber(event.getOrderNumber())
                .timestamp(event.getTimestamp())
                .firstName(null)
                .lastName(null)
                .dob(null)
                .taxNumber(event.getTaxNumber())
                .build();
    }

    public FailPersonEvent extractUpdatePersonEvent(UpdatePersonEvent event) {
        return FailPersonEvent.builder()
                .id(event.getId())
                .correlationId(event.getCorrelationId())
                .eventType(String.valueOf(event.getEventType()))
                .orderNumber(event.getOrderNumber())
                .timestamp(event.getTimestamp())
                .firstName(event.getFirstName() != null ? event.getFirstName() : null)
                .lastName(event.getLastName() != null ? event.getLastName() : null)
                .dob(event.getDob() != null ? event.getDob() : null)
                .taxNumber(event.getTaxNumber())
                .build();
    }

}
