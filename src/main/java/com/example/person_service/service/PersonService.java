package com.example.person_service.service;

import com.example.person_service.dto.request.CalculateTaxRequest;
import com.example.person_service.dto.request.CreatePersonRequest;
import com.example.person_service.dto.request.UpdatePersonRequest;
import com.example.person_service.dto.response.PersonResponse;
import com.example.person_service.entity.Person;
import com.example.person_service.exception.AppException;
import com.example.person_service.exception.ErrorCode;
import com.example.person_service.repository.PersonRepository;
import com.example.person_service.utils.CalculateAge;
import jakarta.transaction.Transactional;

import java.math.BigDecimal;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Page;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional
public class PersonService {
    private static final String VIETNAM_TAX_PATTERN = "^VN[0-9]{9}$";
    private final PersonRepository personRepository;
    private final Map<String, Integer> taxNumberAttempts = new ConcurrentHashMap<>();

    public PersonResponse createPerson(CreatePersonRequest request) {
        if (!request.getTaxNumber().matches(VIETNAM_TAX_PATTERN)) {
            log.error("Invalid tax number: {}", request.getTaxNumber());
            throw new AppException(new ErrorCode(HttpStatus.BAD_REQUEST.value(), "Tax number must follow format: VN +" +
                    " 9 digits"));
        }
        if (personRepository.findByTaxNumber(request.getTaxNumber()).isPresent()) {
            log.warn("Person with TAX number {} already exists", request.getTaxNumber());
            throw new AppException(new ErrorCode(HttpStatus.BAD_REQUEST.value(), "Tax number already exists"));
        }

        Person person =
                Person.builder().firstName(request.getFirstName()).lastName(request.getLastName()).dob(request.getDob()).taxNumber(request.getTaxNumber()).build();
        Person savedPerson = personRepository.save(person);

        return PersonResponse.builder().id(savedPerson.getId()).firstName(savedPerson.getFirstName()).lastName(savedPerson.getLastName()).age(CalculateAge.calAge(request.getDob())).taxNumber(savedPerson.getTaxNumber()).taxDebt(savedPerson.getTaxDebt()).build();

    }

    public PersonResponse findPersonByTaxNumber(String taxNumber) {
        var person = personRepository.findByTaxNumber(taxNumber).orElseThrow(() -> new AppException(new ErrorCode(404
                , "Person not found")));
        return PersonResponse.builder().id(person.getId()).firstName(person.getFirstName()).lastName(person.getLastName()).age(CalculateAge.calAge(person.getDob())).taxNumber(person.getTaxNumber()).taxDebt(person.getTaxDebt() == null ? BigDecimal.ZERO : person.getTaxDebt()).build();
    }

    public List<PersonResponse> findAll(Pageable pageable) {
        Page<Person> personPage = personRepository.findAll(pageable);
        return personPage.map(person ->
                PersonResponse.builder()
                        .id(person.getId())
                        .firstName(person.getFirstName())
                        .lastName(person.getLastName())
                        .age(CalculateAge.calAge(person.getDob()))
                        .taxNumber(person.getTaxNumber())
                        .build()
        ).getContent();
    }

    public List<PersonResponse> filterPersons(String prefix, int minAge, Pageable pageable) {
//        Specification<Person> specification = Specification.where(PersonSpecification.hasNameStartsWithIgnoreCase
//        (prefix)).and(PersonSpecification.isOlderThan(minAge));
//        var persons = personRepository.findAll(specification);
//        PersonResponse[] responseArray = new PersonResponse[persons.size()];
//        for (int i = 0; i < persons.size(); i++) {
//            Person person = persons.get(i);
//            var age = CalculateAge.calAge(person.getDob());
//            responseArray[i] = PersonResponse.builder()
//                                             .id(person.getId())
//                                             .taxNumber(person.getTaxNumber())
//                                             .firstName(person.getFirstName())
//                                             .lastName(person.getLastName())
//                                             .age(age)
//                                             .build();
//        }
        Page<Person> personPage = personRepository.findByNamePrefixAndMinAge("Mi", minAge, pageable);
        return personPage.map(person ->
                PersonResponse.builder()
                        .id(person.getId())
                        .firstName(person.getFirstName())
                        .lastName(person.getLastName())
                        .age(CalculateAge.calAge(person.getDob()))
                        .taxNumber(person.getTaxNumber())
                        .taxDebt(person.getTaxDebt())
                        .build()
        ).getContent();
    }

    private boolean isNotBlank(String value) {
        return value != null && !value.trim().isEmpty();
    }

    public PersonResponse updatePerson(String taxId, UpdatePersonRequest request) {
        var existingPerson =
                personRepository.findByTaxNumber(taxId).orElseThrow(() -> new AppException(new ErrorCode(HttpStatus.NOT_FOUND.value(), "Person not found")));
        Person updatedPerson = Person.builder()
                .id(existingPerson.getId())
                .taxNumber(existingPerson.getTaxNumber())
                .firstName(isNotBlank(request.getFirstName()) ? request.getFirstName().trim() :
                        existingPerson.getFirstName())
                .lastName(isNotBlank(request.getLastName()) ? request.getLastName().trim() :
                        existingPerson.getLastName())
                .dob(request.getDob() != null ? request.getDob() : existingPerson.getDob())
                .build();

        Person savedPerson = personRepository.save(updatedPerson);
        int age = CalculateAge.calAge(existingPerson.getDob());
        if (request.getDob() != null) {
            age = CalculateAge.calAge(request.getDob());
        }
        return PersonResponse.builder().id(savedPerson.getId()).firstName(savedPerson.getFirstName()).lastName(savedPerson.getLastName()).age(age).taxNumber(savedPerson.getTaxNumber()).taxDebt(savedPerson.getTaxDebt())
                .build();
    }

    @Transactional
    public void deletePerson(String taxId) {
        personRepository.findByTaxNumber(taxId).orElseThrow(() -> new AppException(new ErrorCode(HttpStatus.NOT_FOUND.value(), "Person not found")));
        personRepository.deleteByTaxNumber(taxId);
    }

    public void handleTaxCalculation(CalculateTaxRequest request) {
        var existingPerson =
                personRepository.findByTaxNumber(request.getTaxNumber()).orElseThrow(() -> new AppException(new ErrorCode(HttpStatus.NOT_FOUND.value(), "Person not found")));
        BigDecimal taxAmount;
        if (request.getAmount() != null) {
            taxAmount = request.getAmount();
        } else {
            taxAmount = BigDecimal.ZERO;
        }
        existingPerson.addTaxDebt(taxAmount);
        personRepository.save(existingPerson);
    }


    public void simulateTimeoutError(String taxNumber, CreatePersonRequest request) {
        int currentAttempt = taxNumberAttempts.getOrDefault(taxNumber, 0) + 1;
        log.info("Current attempt number: " + currentAttempt);
        taxNumberAttempts.put(taxNumber, currentAttempt);

        log.warn("Simulating timeout for tax number: {} on attempt: {}", taxNumber, currentAttempt);

        if (currentAttempt <= 4) {
            SocketTimeoutException timeoutException = new SocketTimeoutException("External service timeout - attempt "
                    + currentAttempt);
            throw new RuntimeException("Service timeout occurred", timeoutException);
        } else {
            createPerson(request);
        }

        taxNumberAttempts.remove(taxNumber);
        log.info("Successfully processed tax number: {} on attempt: {}", taxNumber, currentAttempt);
    }
}