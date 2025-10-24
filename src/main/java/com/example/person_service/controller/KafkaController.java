package com.example.person_service.controller;

import com.example.person_service.dto.request.*;
import com.example.person_service.dto.response.ApiResponse;
import com.example.person_service.intergration.kafka.consumer.BatchManualAckConsumer;
import com.example.person_service.service.KafkaProducerService;
import com.example.person_service.utils.MockDataGenerator;
import jakarta.validation.Valid;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka/persons")
@Slf4j
@RequiredArgsConstructor
public class KafkaController {
    private final KafkaProducerService kafkaProducerService;
    private final BatchManualAckConsumer batchManualAckConsumer;

    @PostMapping
    public ApiResponse<Void> createPerson(
            @Valid @RequestBody CreatePersonRequest request) {
        kafkaProducerService.sendCreatePerson("create_person_topic", request);
        return ApiResponse.<Void>builder()
                .code(HttpStatus.ACCEPTED.value())
                .message("Person creation request accepted")
                .build();
    }

    @PutMapping
    public ApiResponse<Void> updatePerson(
            @Valid @RequestBody UpdatePersonRequest request) {
        kafkaProducerService.sendUpdatePerson("update_person_topic", request);
        return ApiResponse.<Void>builder()
                .code(HttpStatus.ACCEPTED.value())
                .message("Person modification request accepted")
                .build();
    }

    @DeleteMapping
    public ApiResponse<Void> deletePerson(
            @Valid @RequestBody DeletePersonRequest request) {
        kafkaProducerService.sendDeletePerson("delete_person_topic", request);
        return ApiResponse.<Void>builder()
                .code(HttpStatus.ACCEPTED.value())
                .message("Person removal request accepted")
                .build();
    }

    @PostMapping("/tax-debts")
    public ApiResponse<Void> addPersonTaxDebt(
            @Valid @RequestBody CalculateTaxRequest request) {
        kafkaProducerService.sendCalculateTax("tax_calculation_topic", request);
        return ApiResponse.<Void>builder()
                .code(HttpStatus.ACCEPTED.value())
                .message("Person tax debt calculation request accepted")
                .build();
    }

    @PostMapping("/send")
    public ResponseEntity<String> sendMultipleItems(@RequestBody List<CreatePersonRequest> requests) {
        log.info("Sending {} messages concurrently", requests.size());
        kafkaProducerService.sendMultipleMessages(requests);
        return ResponseEntity.ok(requests.size() + " messages sent");
    }

    @PostMapping("/send-fail-batch")
    public ResponseEntity<Map<String, Object>> sendFailBatch(@RequestParam(defaultValue = "8") int size) {
        log.info("Sending {} messages", size);
        List<CreatePersonRequest> personList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            String key = "batch-" + System.currentTimeMillis() + i;
            personList.add(MockDataGenerator.createTestFailPersonRequest(i));
            kafkaProducerService.sendFailPersonEventBatch(MockDataGenerator.createTestFailPersonRequest(i), key);
        }
        return ResponseEntity.ok(Map.of(
                "personsInBatch", size
        ));
    }

    @PostMapping("/send-success-batch")
    public ResponseEntity<Map<String, Object>> sendSuccessBatch(@RequestParam(defaultValue = "8") int size) {
        log.info("Sending {} separate messages", size);
        List<CreatePersonRequest> personList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            String key = "batch-" + System.currentTimeMillis() + i;
            personList.add(MockDataGenerator.createTestSuccessPersonRequest(i));
            kafkaProducerService.sendSuccessPersonEventBatch(MockDataGenerator.createTestSuccessPersonRequest(i), key);
        }

        return ResponseEntity.ok(Map.of(
                "personsInBatch", size,
                "message", "Sent 1 message containing " + size + " persons"
        ));
    }


    @PostMapping("/send-fail-batch-non-blocking")
    public ResponseEntity<Map<String, Object>> sendFailBatchNonBlocking(@RequestParam(defaultValue = "8") int size) {
        log.info("Sending {} separate messages", size);
        List<CreatePersonRequest> personList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            String key = "batch-" + System.currentTimeMillis() + i;
            personList.add(MockDataGenerator.createTestFailPersonRequest(i));
            kafkaProducerService.sendFailPersonEventBatchWithNonBlockingRetry(MockDataGenerator.createTestFailPersonRequest(i), key);
        }
        return ResponseEntity.ok(Map.of(
                "personsInBatch", size,
                "message", "Sent 1 message containing " + size + " persons"
        ));
    }

    @PostMapping("/batch/events/dependent/success")
    public ResponseEntity<Map<String, Object>> sendSuccessDependentEvents(@RequestParam(defaultValue = "1") int size) {
        List<BaseEvent> eventList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            CreatePersonEvent createPersonEvent = MockDataGenerator.generateRandomCreatePersonEvent(i);
            UUID batchCorrelationId = MockDataGenerator.generateRandomUUID();
            createPersonEvent.setCorrelationId(batchCorrelationId);
            eventList.add(createPersonEvent);
            ReadPersonEvent readPersonEvent = MockDataGenerator.generateRandomReadPersonRequest(i);
            readPersonEvent.setCorrelationId(batchCorrelationId);
            eventList.add(readPersonEvent);
            UpdatePersonEvent updatePersonEvent = MockDataGenerator.generateRandomUpdatePersonRequest(i);
            updatePersonEvent.setCorrelationId(batchCorrelationId);
            eventList.add(updatePersonEvent);
        }
        for (BaseEvent baseEvent : eventList) {
            String key = "batch-" + System.currentTimeMillis();
            kafkaProducerService.sendDependentEvents(baseEvent, key);
        }
        return ResponseEntity.ok(Map.of(
                "eventInBatch", size
        ));
    }

    @PostMapping("/batch/events/dependent/failed")
    public ResponseEntity<Map<String, Object>> sendFailDependentEvents(@RequestParam(defaultValue = "1") int size) {
        List<BaseEvent> eventList = new ArrayList<>();
        UUID batchCorrelationId = MockDataGenerator.generateRandomUUID();
        for (int i = 0; i < size; i++) {
            CreatePersonEvent createPersonEvent = MockDataGenerator.generateRandomCreatePersonEvent(i);
            eventList.add(createPersonEvent);

            ReadPersonEvent readPersonEvent = MockDataGenerator.generateRandomReadPersonRequest(i);
            eventList.add(readPersonEvent);

            UpdatePersonEvent updatePersonEvent = MockDataGenerator.generateRandomUpdatePersonRequest(i);
            eventList.add(updatePersonEvent);
        }
        for (BaseEvent baseEvent : eventList) {
            String key = "batch-" + System.currentTimeMillis();
            baseEvent.setCorrelationId(batchCorrelationId);
            kafkaProducerService.sendFailDependentEvents(baseEvent, key);
        }
        return ResponseEntity.ok(Map.of(
                "eventInBatch", size
        ));
    }

    @PostMapping("/batch/events/independent/success")
    public ResponseEntity<Map<String, Object>> sendSuccessIndependentEvents(@RequestParam(defaultValue = "3") int size) {
        List<BaseEvent> eventList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            CreatePersonEvent createPersonEvent = MockDataGenerator.generateRandomCreatePersonEvent(i);
            eventList.add(createPersonEvent);
        }
        for (BaseEvent baseEvent : eventList) {
            String key = "batch-" + System.currentTimeMillis();
            baseEvent.setCorrelationId(MockDataGenerator.generateRandomUUID());
            kafkaProducerService.sendSuccessIndependentEvents(baseEvent, key);
        }
        return ResponseEntity.ok(Map.of(
                "eventInBatch", size
        ));
    }

    @PostMapping("/batch/events/independent/fail")
    public ResponseEntity<Map<String, Object>> sendFailIndependentEvents(@RequestParam(defaultValue = "3") int size) {
        List<BaseEvent> eventList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            CreatePersonEvent createPersonEvent = MockDataGenerator.generateRandomCreatePersonEvent(i);
            eventList.add(createPersonEvent);
        }
        for (BaseEvent baseEvent : eventList) {
            String key = "batch-" + System.currentTimeMillis();
            baseEvent.setCorrelationId(MockDataGenerator.generateRandomUUID());
            kafkaProducerService.sendFailIndependentEvents(baseEvent, key);
        }
        return ResponseEntity.ok(Map.of(
                "eventInBatch", size
        ));
    }

    @PostMapping("/batch/events/manual")
    public ResponseEntity<Map<String, Object>> sendManualConsumedBatch(@RequestParam(defaultValue = "6") int size) {
        List<CreatePersonRequest> eventList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            CreatePersonRequest createPersonRequest = MockDataGenerator.createTestSuccessPersonRequest(i);
            eventList.add(createPersonRequest);
        }
        for (CreatePersonRequest item : eventList) {
            String key = "batch-" + System.currentTimeMillis();
            kafkaProducerService.sendManualConsumedBatch(item, key);
        }
        return ResponseEntity.ok(Map.of(
                "eventInBatch", size
        ));
    }

    @PostMapping("/batch/events/consume")
    public ResponseEntity<Map<String, Object>> consumeBatchManual(@RequestParam(defaultValue = "6") int size) {
        batchManualAckConsumer.manualConsumeMessages(size);
        return ResponseEntity.ok(Map.of(
                "eventInBatch", size
        ));
    }

}