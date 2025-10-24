package com.example.person_service.intergration.kafka.consumer;

import com.example.person_service.dto.request.CalculateTaxRequest;
import com.example.person_service.dto.request.CreatePersonRequest;
import com.example.person_service.dto.request.DeletePersonRequest;
import com.example.person_service.dto.request.UpdatePersonRequest;
import com.example.person_service.exception.AppException;
import com.example.person_service.service.PersonService;
import jakarta.transaction.Transactional;
import jakarta.validation.ConstraintViolationException;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class EventConsumer {
    private final PersonService personService;

    @RetryableTopic(backoff = @Backoff(delay = 2000, multiplier = 1.5), attempts = "2", exclude = {AppException.class, DataIntegrityViolationException.class, ConstraintViolationException.class, IllegalArgumentException.class})
    @KafkaListener(topics = "create_person_topic", groupId = "create_person_group")
    @KafkaHandler
    public void createPerson(@Payload CreatePersonRequest request) {
        personService.createPerson(request);
        personService.createPerson(request);
    }

    @KafkaListener(topics = "update_person_topic", groupId = "update_person_group")
    @KafkaHandler
    @RetryableTopic(backoff = @Backoff(delay = 2000, multiplier = 1), attempts = "2", exclude = {AppException.class, DataIntegrityViolationException.class, ConstraintViolationException.class, IllegalArgumentException.class})
    public void updatePerson(UpdatePersonRequest request) {
        personService.updatePerson(request.getTaxNumber(), request);
    }

    @KafkaListener(topics = "delete_person_topic", groupId = "delete_person_group")
    @KafkaHandler
    @Transactional
    public void deletePerson(DeletePersonRequest request) {
        personService.deletePerson(request.getTaxNumber());
    }

    @RetryableTopic(backoff = @Backoff(delay = 3000L, multiplier = 1), attempts = "2", exclude = {AppException.class, DataIntegrityViolationException.class, ConstraintViolationException.class, IllegalArgumentException.class})
    @KafkaListener(topics = "tax_calculation_topic", groupId = "tax_calculation_group")
    @KafkaHandler
    @Transactional
    public void handleTaxCalculation(CalculateTaxRequest request) {
        personService.handleTaxCalculation(request);
    }

    @RetryableTopic(backoff = @Backoff(
            delayExpression = "3000",
            multiplierExpression = "1.5"
    ), attempts = "2", exclude = {IllegalArgumentException.class})
    @KafkaListener(topics = "send_messages_topic", groupId = "send_messages_consumer")
    public void createBatchPerson(@Payload List<CreatePersonRequest> requests) {
        for (CreatePersonRequest request : requests) {
            personService.createPerson(request);
            log.info(" Person created with taxNumber: {}", request.getTaxNumber());

        }
    }

    @DltHandler
    public void handleDlt(@Payload Object message) {
        try {
            log.error("=== DLT HANDLER ===");
            if (message instanceof ConsumerRecord<?, ?> record) {
                Object payload = record.value();
                log.error("Payload type: {}", payload.getClass().getSimpleName());
                log.error("Payload: {}", payload);
            }
        } catch (Exception e) {
            log.error("DLT Handler failed: ", e);
        }
    }
}