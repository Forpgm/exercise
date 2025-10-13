package com.example.person_service.service;

import com.example.person_service.dto.request.CreatePersonRequest;
import com.example.person_service.dto.response.CreatePersonResponse;
import com.example.person_service.entity.Person;
import com.example.person_service.exception.AppException;
import com.example.person_service.exception.ErrorCode;
import com.example.person_service.repository.PersonRepository;
import com.example.person_service.utils.CalculateAge;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.Optional;

@RequiredArgsConstructor
@Service
public class PersonService {
    private final PersonRepository personRepository;
    private final CalculateAge calculateAge;

    public CreatePersonResponse createPerson(CreatePersonRequest request) {

        Optional<Person> existingTaxNumber = personRepository.findByTaxNumber(request.getTaxNumber());
        if (existingTaxNumber.isPresent()) {
            throw new AppException(new ErrorCode(HttpStatus.UNPROCESSABLE_ENTITY.value(), "Tax number already " +
                    "existed"));
        }
        Person person =
                Person.builder().firstName(request.getFirstName()).lastName(request.getLastName()).taxNumber(request.getTaxNumber()).dob(request.getDob()).build();
        personRepository.save(person);
        // trả age thay vì dob
        var age = calculateAge.calculateAge(request.getDob());
        return CreatePersonResponse.builder().id(person.getId()).firstName(request.getFirstName()).lastName(request.getLastName()).age(age).taxNumber(request.getTaxNumber()).build();
    }

    public CreatePersonResponse findByTaxNumber(String taxNumber) {
        Person person = personRepository.findByTaxNumber(taxNumber)
                .orElseThrow(() -> new AppException(new ErrorCode(HttpStatus.NOT_FOUND.value(), "Person not found")));

        var age = calculateAge.calculateAge(person.getDob());
        return CreatePersonResponse.builder()
                .id(person.getId())
                .firstName(person.getFirstName())
                .lastName(person.getLastName())
                .age(age)
                .taxNumber(person.getTaxNumber())
                .build();
    }

}
