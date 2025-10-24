package com.example.person_service.service;

import com.example.person_service.entity.FailPersonEvent;
import com.example.person_service.repository.FailPersonEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class FailPersonEventService {
    private final FailPersonEventRepository failPersonEventRepository;

    public void save(FailPersonEvent failPersonEvent) {
        failPersonEventRepository.save(failPersonEvent);
    }

    public Optional<FailPersonEvent> findByCorrelationIdAndOrderNumber(UUID correlationId, int orderNumber) {
        return failPersonEventRepository.findByCorrelationIdAndOrderNumber(correlationId, orderNumber);
    }
}