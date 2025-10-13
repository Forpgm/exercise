package com.example.person_service.consumer;

import com.example.person_service.dto.request.CreatePersonRequest;
import com.example.person_service.dto.request.UpdatePersonRequest;
import com.example.person_service.entity.Person;
import com.example.person_service.exception.AppException;
import com.example.person_service.exception.ErrorCode;
import com.example.person_service.repository.PersonRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;

@Component
//@KafkaListener(topics = "person-updates", groupId = "person-service")
//public class PersonConsumer {
//
//    @Autowired
//    private PersonRepository personRepository;
//
//    @KafkaHandler
//    public void handleCreate(CreatePersonRequest dto) {
//        Person person = new Person();
//        person.setFirstName(dto.getFirstName());
//        person.setLastName(dto.getLastName());
//        person.setDob(dto.getDob());
//        person.setTaxNumber(dto.getTaxNumber());
//        personRepository.save(person);
//        System.out.println("Created person: " + person.getFirstName());
//    }
//
//    @KafkaHandler
//    public void handleUpdate(UpdatePersonRequest dto) {
//        System.out.println("Received UpdatePersonRequest: " + dto);
//
//        try {
//            Person person =
//                    personRepository.findById(UUID.fromString(dto.getId())).orElseThrow(() -> new AppException(new ErrorCode(404,
//                            "Person not found")));
//
//            if (dto.getFirstName() != null) person.setFirstName(dto.getFirstName());
//            if (dto.getLastName() != null) person.setLastName(dto.getLastName());
//            if (dto.getDob() != null) person.setDob(dto.getDob());
//
//            personRepository.save(person);
//            System.out.println("Updated person: " + person.getFirstName());
//
//        } catch (IllegalArgumentException e) {
//            System.out.println("Invalid UUID format: " + dto.getId());
//        } catch (RuntimeException e) {
//            System.out.println(e.getMessage());
//        }
//    }
//
//
//    // Bắt các message không khớp
//    @KafkaHandler(isDefault = true)
//    public void handleOther(Object obj) {
//        System.out.println("Received unknown message: " + obj);
//    }
//}

@KafkaListener(topics = "person-updates", groupId = "person-service")
public class PersonConsumer {

    private final PersonRepository personRepository;
    private final ObjectMapper objectMapper;

    public PersonConsumer(PersonRepository personRepository, ObjectMapper objectMapper) {
        this.personRepository = personRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaHandler(isDefault = true)
    public void handleMessage(Object obj) {
        // obj thường là LinkedHashMap
        Map<String, Object> map = (Map<String, Object>) obj;

        if (map.containsKey("taxNumber")) {
            // Đây là CreatePersonRequest
            CreatePersonRequest dto = objectMapper.convertValue(map, CreatePersonRequest.class);
            createPerson(dto);
        } else if (map.containsKey("id")) {
            // Đây là UpdatePersonRequest
            UpdatePersonRequest dto = objectMapper.convertValue(map, UpdatePersonRequest.class);
            updatePerson(dto);
        } else {
            System.out.println("Unknown message: " + obj);
        }
    }

    private void createPerson(CreatePersonRequest dto) {
        Person person = new Person();
        person.setFirstName(dto.getFirstName());
        person.setLastName(dto.getLastName());
        person.setDob(dto.getDob());
        person.setTaxNumber(dto.getTaxNumber());
        personRepository.save(person);
        System.out.println("Created person: " + person.getFirstName());
    }

    private void updatePerson(UpdatePersonRequest dto) {
        try {
            Person person = personRepository.findById(UUID.fromString(dto.getId()))
                    .orElseThrow(() -> new RuntimeException("Person not found"));

            if (dto.getFirstName() != null) person.setFirstName(dto.getFirstName());
            if (dto.getLastName() != null) person.setLastName(dto.getLastName());
            if (dto.getDob() != null) person.setDob(dto.getDob());

            personRepository.save(person);
            System.out.println("Updated person: " + person.getFirstName());
        } catch (IllegalArgumentException e) {
            System.out.println("Invalid UUID: " + dto.getId());
        }
    }
}
