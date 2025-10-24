package com.example.person_service.utils;


import com.example.person_service.constant.EventType;
import com.example.person_service.dto.request.CreatePersonEvent;
import com.example.person_service.dto.request.CreatePersonRequest;
import com.example.person_service.dto.request.ReadPersonEvent;
import com.example.person_service.dto.request.UpdatePersonEvent;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Random;
import java.util.UUID;

public class MockDataGenerator {
    public static final Random random = new Random();

    public static LocalDate generateRandomDob() {
        int year = 1970 + random.nextInt(31);
        int month = 1 + random.nextInt(12);
        int day = 1 + random.nextInt(28);
        return LocalDate.of(year, month, day);
    }

    public static UUID generateRandomUUID() {
        return UUID.randomUUID();
    }

    public static CreatePersonEvent generateRandomCreatePersonEvent(int index) {
        CreatePersonEvent mockEvent = CreatePersonEvent.builder()
                .firstName("firstName_" + index)
                .lastName("lastName_" + index)
                .dob(generateRandomDob())
                .taxNumber(String.format("VN" + "%09d", 900000000 + index))
                .build();
        mockEvent.setId(generateRandomUUID());
        mockEvent.setEventType(EventType.CREATE_PERSON);
        mockEvent.setOrderNumber(index * 2 + 1);
        mockEvent.setTimestamp(LocalDateTime.now());
        return mockEvent;
    }

    public static ReadPersonEvent generateRandomReadPersonRequest(int index) {
        ReadPersonEvent mockEvent = ReadPersonEvent.builder().taxNumber(String.format("VN" + "%09d", 900000000 + index)).build();

        mockEvent.setId(generateRandomUUID());
        mockEvent.setEventType(EventType.READ_PERSON);
        mockEvent.setOrderNumber(index * 2 + 2);
        mockEvent.setTimestamp(LocalDateTime.now());
        return mockEvent;
    }

    public static UpdatePersonEvent generateRandomUpdatePersonRequest(int index) {

        UpdatePersonEvent mockEvent = UpdatePersonEvent.builder()
                .firstName("updatefirstName_" + index)
                .lastName("updatelastName_" + index)
                .dob(generateRandomDob())
                .taxNumber(String.format("VN" + "%09d", 900000000 + index))
                .build();
        mockEvent.setId(generateRandomUUID());
        mockEvent.setEventType(EventType.UPDATE_PERSON);
        mockEvent.setOrderNumber(index * 2 + 3);
        mockEvent.setTimestamp(LocalDateTime.now());
        return mockEvent;
    }
    public static CreatePersonRequest createTestSuccessPersonRequest(int index) {
        return CreatePersonRequest.builder()
                .firstName("firstName_" + index)
                .lastName("lastName_" + index)
                .dob(LocalDate.parse("199" + (index % 10) + "-" + String.format("%02d", (index % 12) + 1) + "-15"))
                .taxNumber("VN" + String.format("%09d", 900000000 + index))
                .build();
    }

    public static CreatePersonRequest createTestFailPersonRequest(int index) {
        return CreatePersonRequest.builder()
                .firstName("firstName_" + index)
                .lastName("lastName_" + index)
                .dob(LocalDate.parse("199" + (index % 10) + "-" + String.format("%02d", (index % 12) + 1) + "-15"))
                .taxNumber(index % 2 == 0 ? "INVALID_TAX" : "VN" + String.format("%09d", 900000000 + index))
                .build();
    }
}