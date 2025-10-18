package com.example.person_service.controller;


import com.example.person_service.dto.request.CreatePersonRequest;
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
}
