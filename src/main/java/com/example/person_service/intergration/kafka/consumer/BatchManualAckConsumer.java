package com.example.person_service.intergration.kafka.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.example.person_service.constant.EventType;
import com.example.person_service.dto.request.*;
import com.example.person_service.dto.response.PersonResponse;
import com.example.person_service.entity.FailPersonEvent;
import com.example.person_service.exception.AppException;
import com.example.person_service.service.FailPersonEventService;
import com.example.person_service.service.PersonService;
import com.example.person_service.utils.PersonEventMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.logging.log4j.util.InternalException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
@EnableRetry
public class BatchManualAckConsumer {
    private final PersonService personService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final PersonEventMapper personEventMapper;
    private final FailPersonEventService failPersonEventService;
    private final ConsumerFactory<String, Object> consumerFactory;
    @Qualifier("manualConsumerFactory")
    private final ConsumerFactory<String, Object> manualConsumerFactory;

    @KafkaListener(topics = "fail-person-events", containerFactory = "batchKafkaListenerContainerFactory")
    public void handleFailBatchPersons(List<ConsumerRecord<String, CreatePersonRequest>> records, Acknowledgment ack) {
        log.info("Processing batch of {} persons", records.size());
        boolean success = processRecords(records);
        if (success) {
            ack.acknowledge();
            log.info("Batch of {} persons processed", records.size());
        }
    }

    boolean processRecords(List<ConsumerRecord<String, CreatePersonRequest>> records) {
        for (ConsumerRecord<String, CreatePersonRequest> record : records) {
            CreatePersonRequest person = record.value();
            try {
                log.info("Processing person {} at offset {}", person.getFirstName(), person.getTaxNumber());
                simulateErrors(record);
                personService.createPerson(person);
                log.info("Acknowledged person [{}]: {} {}", record.key(), person.getFirstName(), person.getTaxNumber());
            } catch (AppException e) {
                log.error("Error processing person {} at offset {}", person.getFirstName(), person.getTaxNumber());
                throw e;
            } catch (Exception e) {
                log.error("System error for person [{}]: {}", record.key(), e.getMessage(), e);
                throw e;
            }
        }
        return true;
    }

    @KafkaListener(topics = "success-person-events", containerFactory = "batchKafkaListenerContainerFactory")
    public void handSuccessBatchPersons(List<ConsumerRecord<String, CreatePersonRequest>> records, Acknowledgment ack) {
        log.info("Processing batch of {} persons", records.size());
        boolean success = true;

        for (ConsumerRecord<String, CreatePersonRequest> record : records) {
            CreatePersonRequest person = record.value();
            try {
                log.info("Processing person {} at offset {}", person.getFirstName(), person.getTaxNumber());
                personService.createPerson(person);
                log.info("Successfully processed and acknowledged person [{}]: {} {}", record.key(), person.getFirstName(), person.getTaxNumber());
            } catch (AppException e) {
                success = false;
                log.error("Error processing person {} with tax number: {}", person.getFirstName(), person.getTaxNumber());
            } catch (Exception e) {
                success = false;
                log.error("System error for person [{}]: {}", record.key(), e.getMessage(), e);
            }
        }
        if (success) {
            ack.acknowledge();
        }
        log.info("Batch of {} persons processed", records.size());
    }

    public void simulateErrors(ConsumerRecord<String, CreatePersonRequest> record) {
        CreatePersonRequest person = record.value();
        if (person.getFirstName().contains("firstName_1")) {
            throw new InternalException("Database connection timeout for person");
        }
    }

    @KafkaListener(topics = "fail-non-blocking", containerFactory = "batchKafkaListenerContainerFactory")
    public void handleFailBatchPersonsWithNonBlockingRetry(List<ConsumerRecord<String, CreatePersonRequest>> records, Acknowledgment ack) {
        log.info("Processing batch of {} persons", records.size());
        List<CreatePersonRequest> failedMessages = new ArrayList<>();

        for (ConsumerRecord<String, CreatePersonRequest> record : records) {
            CreatePersonRequest person = record.value();
            try {
                simulateErrors(record);
                personService.createPerson(person);
                log.info("Successfully processed: {} {}", person.getFirstName(), person.getTaxNumber());
            } catch (AppException e) {
                failedMessages.add(record.value());
                log.error("Error processing person {} at offset {}", person.getFirstName(), person.getTaxNumber());
            } catch (Exception e) {
                failedMessages.add(record.value());
                log.error("System error for person [{}]: {}", record.key(), e.getMessage(), e);
            }
        }
        for (CreatePersonRequest failedMsg : failedMessages) {
            kafkaTemplate.send("retry-non-blocking", failedMsg);
        }

        ack.acknowledge();
        log.info("Batch of {} persons processed", records.size());
    }

    @KafkaListener(topics = "retry-non-blocking")
    public void handleRetryNonBlocking(ConsumerRecord<String, CreatePersonRequest> record) {
        try {
            personService.createPerson(record.value());
        } catch (AppException e) {
            log.info("Retry 1 failed: {} {}, send to DLT", record.value().getFirstName(), record.value().getTaxNumber());
            kafkaTemplate.send("dlt-non-blocking", record.value());
        }
    }

    @KafkaListener(topics = "dlt-non-blocking")
    public void handleRetryNonBlockingDlt(ConsumerRecord<String, CreatePersonRequest> record) {
        log.error("=== DLT HANDLER ===");
        log.error("record: {}", record.value());
    }

    @KafkaListener(topics = "success-dependent-events", containerFactory = "batchKafkaListenerContainerFactory")
    public void handSuccessBatchDependentEvents(List<ConsumerRecord<String, BaseEvent>> records, Acknowledgment ack) {
        log.info("Processing batch of {} events: {}", records.size());
        boolean success = true;
        for (ConsumerRecord<String, BaseEvent> item : records) {
            var event = item.value();
            try {
                if (event.getEventType().equals(EventType.CREATE_PERSON)) {
                    CreatePersonRequest request = personEventMapper.toRequest((CreatePersonEvent) event);
                    personService.createPerson(request);
                    log.info("Success create person event");
                } else if (event.getEventType().equals(EventType.READ_PERSON)) {
                    PersonResponse personResponse = personService.findPersonByTaxNumber(((ReadPersonEvent) event).getTaxNumber());
                    log.info("Success read person event: {}", personResponse);
                } else if (event.getEventType().equals(EventType.UPDATE_PERSON)) {
                    UpdatePersonRequest request = personEventMapper.toUpdateRequest((UpdatePersonEvent) event);
                    PersonResponse personResponse = personService.updatePerson(((UpdatePersonEvent) event).getTaxNumber(), request);
                    log.info("Success update person event: {}", personResponse);
                }
            } catch (AppException e) {
                success = false;
                log.error("AppException processing event {}: {}", item.value(), e.getErrorCode().getErrorMessage());
            } catch (Exception e) {
                log.error("Exception processing event {}: {}", item.value(), e.getMessage());
            }
        }
        if (success) {
            ack.acknowledge();
            log.info("Batch of {} events processed", records.size());
        } else {
            log.info("Batch of {} events failed", records.size());
        }
    }

    @KafkaListener(topics = "fail-dependent-events", containerFactory = "batchKafkaListenerContainerFactory")
    public void handFailBatchDependentEvents(List<ConsumerRecord<String, BaseEvent>> records, Acknowledgment ack) {
        for (ConsumerRecord<String, BaseEvent> item : records) {
            var event = item.value();
            try {
                if (event.getEventType().equals(EventType.CREATE_PERSON)) {
                    CreatePersonRequest request = personEventMapper.toRequest((CreatePersonEvent) event);
                    personService.simulateTimeoutError(request.getTaxNumber(), request);
                    log.info("Success person event: create {}", request.getTaxNumber());
                } else if (event.getEventType().equals(EventType.READ_PERSON)) {
                    ReadPersonEvent readPersonEvent = (ReadPersonEvent) event;
                    Optional<FailPersonEvent> previousEvent = failPersonEventService.findByCorrelationIdAndOrderNumber(event.getCorrelationId(), event.getOrderNumber() - 1);
                    if (previousEvent.isPresent() && previousEvent.get().getEventType().equals(String.valueOf(EventType.CREATE_PERSON))) {
                        FailPersonEvent failedEvent = previousEvent.get();
                        CreatePersonEvent createEvent = personEventMapper.extractFailPersonEvent(failedEvent);
                        CreatePersonRequest request = personEventMapper.toRequest(createEvent);
                        personService.createPerson(request);
                    }
                    PersonResponse personResponse = personService.findPersonByTaxNumber(readPersonEvent.getTaxNumber());
                    log.info("Success person event: read {}", personResponse);
                } else if (event.getEventType().equals(EventType.UPDATE_PERSON)) {
                    UpdatePersonRequest request = personEventMapper.toUpdateRequest((UpdatePersonEvent) event);
                    PersonResponse personResponse = personService.updatePerson(((UpdatePersonEvent) event).getTaxNumber(), request);
                    log.info("Success person event: update {}", personResponse);
                }
            } catch (AppException e) {
                log.error("AppException processing event {}: {}", event, e.getErrorCode().getErrorMessage());
                throw e;
            } catch (RuntimeException e) {
                log.error("RuntimeException processing event {}: {}", e.getMessage(), event);
                throw e;
            } catch (Exception e) {
                log.error("Exception processing event {}: {}", item.value(), e.getMessage());
                throw e;
            }
        }
        ack.acknowledge();
        log.info("Batch of {} events success", records.size());
    }

    @KafkaListener(topics = "success-independent-events", containerFactory = "batchKafkaListenerContainerFactory")
    public void handSuccessBatchIndependentEvents(List<ConsumerRecord<String, BaseEvent>> records, Acknowledgment ack) {
        boolean success = true;
        for (ConsumerRecord<String, BaseEvent> item : records) {
            var event = item.value();
            try {
                CreatePersonRequest request = personEventMapper.toRequest((CreatePersonEvent) event);
                personService.createPerson(request);
                log.info("Success create person event");
            } catch (AppException e) {
                success = false;
                log.error("AppException processing event {}: {}", item.value(), e.getErrorCode().getErrorMessage());
            } catch (Exception e) {
                log.error("Exception processing event {}: {}", item.value(), e.getMessage());
            }
        }
        if (success) {
            ack.acknowledge();
        }
    }

    @KafkaListener(topics = "fail-independent-events", containerFactory = "batchKafkaListenerContainerFactory")
    public void handFailBatchIndependentEvents(List<ConsumerRecord<String, BaseEvent>> records, Acknowledgment ack) {
        for (ConsumerRecord<String, BaseEvent> record : records) {
            var event = record.value();
            try {
                CreatePersonRequest request = personEventMapper.toRequest((CreatePersonEvent) event);
                if (((CreatePersonEvent) event).getFirstName().equals("firstName_0")) {
                    personService.simulateTimeoutError(request.getTaxNumber(), request);
                } else {
                    personService.createPerson(request);
                }
                log.info("Success create person event");
            } catch (AppException e) {
                log.error("AppException processing event {}: {}", event, e.getErrorCode().getErrorMessage());
                throw e;
            } catch (Exception e) {
                log.error("Exception processing event {}: {}", event, e.getMessage());
                throw e;
            }
        }
    }

    public void manualConsumeMessages(Integer maxEvents) {
        int limit = maxEvents != null ? maxEvents : 10;
        try (Consumer<String, Object> consumer = consumerFactory.createConsumer()) {
            consumer.subscribe(Collections.singletonList("manual-consumption-topic"));


            log.info("Assigned partitions: {}", consumer.assignment());

            ConsumerRecords<String, Object> records = consumer.poll(Duration.ofSeconds(5));
            log.info("Polled {} records from topic", records.count());

            // Rest of your code...
        } catch (Exception e) {
            log.error("Error consuming messages from topic", e);
        }
    }

}