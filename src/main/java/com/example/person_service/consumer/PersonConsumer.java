package com.example.person_service.consumer;

import com.example.person_service.dto.request.CreatePersonRequest;
import com.example.person_service.dto.request.UpdatePersonRequest;
import com.example.person_service.entity.Person;
import com.example.person_service.repository.PersonRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class PersonConsumer {

    private final PersonRepository personRepository;

    public PersonConsumer(PersonRepository personRepository) {
        this.personRepository = personRepository;
    }

    // Listener cho topic CreatePerson
    @KafkaListener(topics = "CreatePerson", groupId = "person-service")
    public void handleCreatePerson(CreatePersonRequest dto) {
        Person person = new Person();
        person.setFirstName(dto.getFirstName());
        person.setLastName(dto.getLastName());
        person.setDob(dto.getDob());
        person.setTaxNumber(dto.getTaxNumber());
        personRepository.save(person);
        System.out.println("Created person: " + person.getFirstName());
    }

    // Listener cho topic UpdatePerson
    @KafkaListener(topics = "UpdatePerson", groupId = "person-service")
    public void handleUpdatePerson(UpdatePersonRequest dto) {
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
