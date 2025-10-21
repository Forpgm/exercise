package com.example.person_service.consumer;

import com.example.person_service.dto.request.CreatePersonBatchRequest;
import com.example.person_service.dto.request.CreatePersonRequest;
import com.example.person_service.dto.request.StringBatch;
import com.example.person_service.service.PersonService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.List;


@RequiredArgsConstructor
@Service
public class BatchConsumer {
    private final PersonService personService;

    //    @KafkaListener(topics = "CreatePersonBatch", containerFactory = "kafkaListenerContainerFactory", groupId =
    //    "CreatePersonBatch")
//    public void listen(CreatePersonBatchRequest batch, Acknowledgment ack) {
//        try {
//            batch.getPersons().forEach(person -> {
//                personService.createPerson(person);
//                System.out.println("Processed: " + person);
//            });
//        } catch (Exception e) {
//            System.err.println(e.getMessage());
//        }
//        ack.acknowledge();
//        System.out.println("Batch acknowledged");
//    }
//    @KafkaListener(topics = "CreatePersonBatch", containerFactory = "kafkaListenerContainerFactory", groupId =
//            "CreatePersonBatch")
//    public void listen(List<CreatePersonRequest> requests, Acknowledgment ack) {
//        requests.forEach(personService::createPerson);
//        ack.acknowledge();
//    }
    @KafkaListener(topics = "CreatePersonBatch", containerFactory = "kafkaListenerContainerFactory", groupId =
            "CreatePersonBatch")
//    public void listen(List<CreatePersonRequest> requests, Acknowledgment ack) {
//        System.out.println("Received batch size: " + requests.size());
//        try {
//            for (CreatePersonRequest req : requests) {
//                personService.createPerson(req);
//                System.out.println("Processed: " + req.getFirstName());
//            }
//            ack.acknowledge();
//            System.out.println("Batch acknowledged");
//        } catch (Exception e) {
//            System.err.println("Batch error: " + e.getMessage());
//        }
//    }

    @KafkaListener(
            topics = "string-batch-topic",
            containerFactory = "kafkaListenerContainerFactory",
            groupId = "string-batch-group"
    )
    public void listen(List<StringBatch> batch, Acknowledgment ack) {
        if (batch != null && !batch.isEmpty()) {
            batch.forEach(item -> System.out.println("Processed: " + item));
        }

        ack.acknowledge();
        System.out.println("Batch acknowledged");
    }


}
