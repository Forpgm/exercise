package com.example.person_service.producer;

import com.example.person_service.dto.request.CreatePersonRequest;
import com.example.person_service.dto.request.StringBatch;
import com.example.person_service.dto.request.UpdatePersonRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class PersonProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void sendPersonCreate(CreatePersonRequest request) {
        kafkaTemplate.send("CreatePerson", request);
    }

    public void sendPersonUpdate(UpdatePersonRequest request) {
        kafkaTemplate.send("UpdatePerson", request);
    }

    public void sendMessages(CreatePersonRequest request) {
        kafkaTemplate.send("CreatePersonBatch", request);
    }

    public void sendBatch(List<StringBatch> batch) {
        kafkaTemplate.send("string-batch-topic", batch);
    }
}
