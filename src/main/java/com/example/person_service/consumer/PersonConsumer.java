package com.example.person_service.consumer;

import com.example.person_service.dto.request.CreatePersonRequest;
import com.example.person_service.dto.request.UpdatePersonRequest;
import com.example.person_service.entity.Person;
import com.example.person_service.repository.PersonRepository;
import com.example.person_service.service.PersonService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class PersonConsumer {
    private final PersonRepository personRepository;
    private final PersonService personService;

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

    @KafkaListener(topics = "CreatePersonBatch", groupId = "CreatePersonBatch")
    public void handleBatchWithSimulate(
            List<CreatePersonRequest> dtos,
            Acknowledgment ack // để manual commit offset
    ) {
        boolean isSuccess = true;
        for (int i = 0; i < dtos.size(); i++) {
            CreatePersonRequest dto = dtos.get(i);
            try {
                // Simulate lỗi
                if (i == 0) {
                    throw new RuntimeException("Simulated 500 error for first item");
                } else if (i == 2) {
                    throw new IllegalArgumentException("Simulated 400 error for third item");
                } else if ("DB_ERROR".equals(dto.getFirstName())) {
                    throw new RuntimeException("Simulated DB error for manual ack");
                }

                // Xử lý bình thường
                var person = personService.createPerson(dto);
                System.out.println("Created person: " + person.getTaxNumber());

            } catch (Exception ex) {
                isSuccess = false;
                System.err.println("Error processing item " + i + " (id=" + dto.getTaxNumber() + "): " + ex.getMessage());
            }
        }

        if (isSuccess) {
            ack.acknowledge();
            System.out.println("Batch acknowledged");

        } else {
            System.out.println("No Batch acknowledged");
        }
    }

}
