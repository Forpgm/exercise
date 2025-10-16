package com.example.person_service.consumer;

import com.example.person_service.dto.request.CreatePersonRequest;
import com.example.person_service.dto.request.UpdatePersonRequest;
import com.example.person_service.entity.Person;
import com.example.person_service.repository.PersonRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.util.UUID;
@Slf4j
@Component
public class PersonConsumer {

    private final PersonRepository personRepository;

    public PersonConsumer(PersonRepository personRepository) {
        this.personRepository = personRepository;
    }
    @DltHandler
    public void handleDlt(Object message) {
        log.error("💀 Message sent to DLT: {}", message);
    }

    // Listener cho topic CreatePerson
    @KafkaListener(topics = "CreatePerson", groupId = "group_id")
    @KafkaHandler
    @RetryableTopic(
            attempts = "4", // 1 gốc + 3 retry
            backoff = @Backoff(delay = 3000, multiplier = 2.0), // 3s, 6s, 12s
            autoCreateTopics = "true",
            exclude = {IllegalArgumentException.class, NullPointerException.class},
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE
    )
    public void handleCreatePerson(CreatePersonRequest dto) {
        try {
            Person person = new Person();
            person.setFirstName(dto.getFirstName());
            person.setLastName(dto.getLastName());
            person.setDob(dto.getDob());
            person.setTaxNumber(dto.getTaxNumber());
            personRepository.save(person);
            System.out.println("Created person: " + person.getFirstName());
        } catch (Exception e) {
            throw e;
        }
    }

    // Listener cho topic UpdatePerson

    @KafkaListener(topics = "UpdatePerson", groupId = "group_id")
    @KafkaHandler
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
