package com.example.person_service.controller;


import com.example.person_service.dto.request.CreatePersonRequest;
import com.example.person_service.dto.request.StringBatch;
import com.example.person_service.dto.request.UpdatePersonRequest;
import com.example.person_service.dto.response.ApiResponse;
import com.example.person_service.dto.response.CreatePersonResponse;
import com.example.person_service.producer.PersonProducer;
import com.example.person_service.service.PersonService;
import jakarta.validation.Valid;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@RestController
@RequestMapping("/persons")
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class PersonController {

    private final PersonService personService;
    private final PersonProducer producer;

    @PostMapping
    public ApiResponse<CreatePersonResponse> createPerson(@Valid @RequestBody CreatePersonRequest request) {
        CreatePersonResponse response = personService.createPerson(request);
        return ApiResponse.<CreatePersonResponse>builder().code(HttpStatus.CREATED.value()).message("Create person " +
                "successffully").result(response).build();
    }

    @PostMapping("/kafka")
    public ResponseEntity<ApiResponse<String>> createPersonKafka(@Valid @RequestBody CreatePersonRequest request) {
        producer.sendPersonCreate(request);
        ApiResponse<String> response = ApiResponse.<String>builder()
                .code(HttpStatus.ACCEPTED.value())
                .message("Person event sent to Kafka successfully")
                .result("Event queued for person: " + request.getFirstName())
                .build();
        return ResponseEntity.accepted().body(response);
    }

    @PutMapping("/kafka")
    public ResponseEntity<ApiResponse<String>> updatePersonKafka(@Valid
                                                                 @RequestBody UpdatePersonRequest request) {

        producer.sendPersonUpdate(request);
        ApiResponse<String> response = ApiResponse.<String>builder()
                .code(HttpStatus.ACCEPTED.value())
                .message("Person event sent to Kafka successfully")
                .result("Event queued for person: " + request.getFirstName())
                .build();
        return ResponseEntity.accepted().body(response);
    }


    @GetMapping("/tax-numbers/{taxNumber}")
    public ApiResponse<CreatePersonResponse> findPersonByTaxNumber(@Valid @PathVariable String taxNumber) {
        CreatePersonResponse response = personService.findByTaxNumber(taxNumber);
        return ApiResponse.<CreatePersonResponse>builder().code(HttpStatus.CREATED.value()).message("Find person by " +
                "tax number successfully").result(response).build();
    }


    @PostMapping("kafka/send-string-batch")
    public void sendStringBatch(@RequestBody List<StringBatch> items) {
        items.forEach(System.out::println);
        producer.sendBatch(items);
    }

    @PostMapping("kafka/send-batch")
    public void sendPersonBatch(@RequestParam(defaultValue = "10") int size) {
        List<CreatePersonRequest> items = generateBatch(size);
        for (int i = 0; i < size; i++) {
            producer.sendMessages(items.get(i));
        }
    }

    public List<CreatePersonRequest> generateBatch(int size) {
        List<CreatePersonRequest> batch = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < size; i++) {
            String firstName = "First" + (i + 1);
            String lastName = "Last" + (i + 1);

            // random ngày sinh trong khoảng 1980 - 2010
            int year = 1980 + random.nextInt(31);
            int month = 1 + random.nextInt(12);
            int day = 1 + random.nextInt(28);

            LocalDate dob = LocalDate.of(year, month, day);

            // taxNumber dạng VN + 9 chữ số
            String taxNumber = "VN" + String.format("%09d", random.nextInt(1_000_000_000));

            batch.add(new CreatePersonRequest(firstName, lastName, dob, taxNumber));
        }

        return batch;
    }
}
