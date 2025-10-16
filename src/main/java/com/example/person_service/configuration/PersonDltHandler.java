package com.example.person_service.configuration;
import org.springframework.stereotype.Component;

@Component("personDltHandler")
public class PersonDltHandler {

    // Method có thể nhận String hoặc Object, và Exception
    public void handleDlt(Object message, Exception e) {
        System.err.println("DLT message: " + message);
        e.printStackTrace();
    }
}
