package com.example.person_service.dto.request;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.List;
@Data
@AllArgsConstructor
@Builder
public class CreatePersonBatchRequest {
    private List<CreatePersonRequest> persons;
}
