package com.example.person_service.controller;

import com.example.person_service.dto.request.CreatePersonRequest;
import com.example.person_service.dto.request.UpdatePersonRequest;
import com.example.person_service.dto.response.ApiResponse;
import com.example.person_service.dto.response.PersonResponse;
import com.example.person_service.service.PersonService;
import jakarta.validation.Valid;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/persons")
@RequiredArgsConstructor
@Validated
public class PersonController {
    private final PersonService personService;
    private PersonResponse[] response;

    @PostMapping
    public ApiResponse<PersonResponse> createPerson(
            @Valid @RequestBody CreatePersonRequest request) {
        PersonResponse response = personService.createPerson(request);
        return ApiResponse.<PersonResponse>builder()
                .code(201)
                .message("Created Person successfully")
                .result(response)
                .build();
    }

    @GetMapping("/taxes/{taxNumber}")
    public ApiResponse<PersonResponse> findByTaxNumber(
            @PathVariable String taxNumber) {
        PersonResponse response = personService.findPersonByTaxNumber(taxNumber);
        return ApiResponse.<PersonResponse>builder()
                .code(HttpStatus.OK.value())
                .message("Find person by tax number successfully")
                .result(response)
                .build();
    }


    @GetMapping()
    public ApiResponse<List<PersonResponse>> listAllPersons(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size
    ) {
        Pageable pageable = PageRequest.of(page, size);
        List<PersonResponse> response = personService.findAll(pageable);
        return ApiResponse.<List<PersonResponse>>builder()
                .code(HttpStatus.OK.value())
                .message("List persons successfully")
                .result(response)
                .build();
    }


    @GetMapping("/filter")
    public ApiResponse<List<PersonResponse>> filterPersons(@RequestParam(defaultValue = "Mi") String prefix,
                                                           @RequestParam(defaultValue = "30") Integer minAge,
                                                           @RequestParam(defaultValue = "0") int page,
                                                           @RequestParam(defaultValue = "2") int size) {
        Pageable pageable = PageRequest.of(page, size);
        List<PersonResponse> response = personService.filterPersons(prefix, minAge, pageable);
        return ApiResponse.<List<PersonResponse>>builder()
                .code(HttpStatus.OK.value())
                .message("filter persons successfully")
                .result(response)
                .build();
    }

    @PutMapping("")
    public ApiResponse<PersonResponse> updatePerson(@RequestParam String taxId,
                                                    @Valid @RequestBody UpdatePersonRequest request) {
        PersonResponse response = personService.updatePerson(taxId, request);
        return ApiResponse.<PersonResponse>builder()
                .code(HttpStatus.OK.value())
                .message("update person successfully")
                .result(response)
                .build();
    }

    @DeleteMapping()
    public ApiResponse<Void> deletePerson(
            @RequestParam String taxId) {
        personService.deletePerson(taxId);
        return ApiResponse.<Void>builder()
                .code(HttpStatus.OK.value())
                .message("delete person successfully")
                .build();
    }


}