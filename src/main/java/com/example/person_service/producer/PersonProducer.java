package com.example.person_service.producer;

import com.example.person_service.dto.request.CreatePersonRequest;
import com.example.person_service.dto.request.UpdatePersonRequest;
import com.example.person_service.entity.Person;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PersonProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void sendPersonCreate(CreatePersonRequest request) {
        kafkaTemplate.send("person-updates", request);
    }

    public void sendPersonUpdate(UpdatePersonRequest request) {
        kafkaTemplate.send("person-updates", request);
    }
}
