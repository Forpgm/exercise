package com.example.person_service.consumer;

import com.example.person_service.dto.request.CreatePersonRequest;
import com.example.person_service.dto.request.UpdatePersonRequest;
import com.example.person_service.entity.Person;
import com.example.person_service.repository.PersonRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;
@Slf4j
@Component
@RequiredArgsConstructor
public class PersonConsumer {
    private final PersonRepository personRepository;

    @KafkaListener(topics = "CreatePerson", groupId = "CreatePerson")
    public void handleCreatePerson(CreatePersonRequest dto) {
            Person person = new Person();
            person.setFirstName(dto.getFirstName());
            person.setLastName(dto.getLastName());
            person.setDob(dto.getDob());
            person.setTaxNumber(dto.getTaxNumber());
            personRepository.save(person);
    }

    @KafkaListener(topics = "UpdatePerson", groupId = "UpdatePerson")
    public void handleUpdatePerson(UpdatePersonRequest dto) {
            Person person = personRepository.findById(UUID.fromString(dto.getId()))
                    .orElseThrow(() -> new RuntimeException("Person not found"));
            if (dto.getFirstName() != null) person.setFirstName(dto.getFirstName());
            if (dto.getLastName() != null) person.setLastName(dto.getLastName());
            if (dto.getDob() != null) person.setDob(dto.getDob());
            personRepository.save(person);
    }

}
