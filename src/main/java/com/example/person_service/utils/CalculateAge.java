package com.example.person_service.utils;

import com.example.person_service.entity.Person;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.Period;

@Component
public class CalculateAge {
    public static int calAge(LocalDate dob) {
        return Period.between(dob, LocalDate.now()).getYears();
    }
}
