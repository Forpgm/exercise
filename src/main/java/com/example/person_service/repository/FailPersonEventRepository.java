package com.example.person_service.repository;
import java.util.Optional;
import java.util.UUID;

import com.example.person_service.entity.FailPersonEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

@Repository
public interface FailPersonEventRepository extends JpaRepository<FailPersonEvent, UUID>, JpaSpecificationExecutor<FailPersonEvent> {
    Optional<FailPersonEvent> findByCorrelationIdAndOrderNumber(UUID correlationId, int orderNumber);
}