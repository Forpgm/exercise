package com.example.person_service.service;

import com.example.person_service.dto.request.BaseEvent;
import com.example.person_service.dto.request.CalculateTaxRequest;
import com.example.person_service.dto.request.CreatePersonRequest;
import com.example.person_service.dto.request.UpdatePersonRequest;
import com.example.person_service.dto.request.DeletePersonRequest;
import java.util.List;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class KafkaProducerService {
    KafkaTemplate<String, Object> kafkaTemplate;

    public void sendCreatePerson(String topic, CreatePersonRequest request) {
        kafkaTemplate.send(topic, request);
    }

    public void sendUpdatePerson(String topic, UpdatePersonRequest request) {
        kafkaTemplate.send(topic, request);
    }

    public void sendDeletePerson(String topic, DeletePersonRequest request) {
        kafkaTemplate.send(topic, request);
    }

    public void sendCalculateTax(String topic, CalculateTaxRequest request) {
        kafkaTemplate.send(topic, request);
    }

    public void sendMultipleMessages(List<CreatePersonRequest> requests) {
        log.info("Sending {} messages concurrently", requests.size());
        for (CreatePersonRequest request : requests) {
            kafkaTemplate.send("send_messages_topic", request);
        }
    }

    public void sendSuccessPersonEventBatch(CreatePersonRequest request, String key) {
        kafkaTemplate.send("success-person-events", key, request);
    }

    public void sendFailPersonEventBatch(CreatePersonRequest request, String key) {
        kafkaTemplate.send("fail-person-events", key, request);
    }

    public void sendFailPersonEventBatchWithNonBlockingRetry(CreatePersonRequest request, String key) {
        kafkaTemplate.send("fail-non-blocking", key, request);
    }

    public void sendDependentEvents(BaseEvent baseEvent, String key) {
        kafkaTemplate.send("success-dependent-events", key, baseEvent);
    }

    public void sendFailDependentEvents(BaseEvent baseEvent, String key) {
        kafkaTemplate.send("fail-dependent-events", key, baseEvent);
    }

    public void sendSuccessIndependentEvents(BaseEvent baseEvent, String key) {
        kafkaTemplate.send("success-independent-events", key, baseEvent);
    }

    public void sendFailIndependentEvents(BaseEvent baseEvent, String key) {
        kafkaTemplate.send("fail-independent-events", key, baseEvent);
    }

    public void sendManualConsumedBatch(CreatePersonRequest request,String key) {
        kafkaTemplate.send("manual-consumption-topic",key, request);
    }
}