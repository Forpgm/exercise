package com.example.person_service.dto.request;
import lombok.AllArgsConstructor;
import lombok.Data;
import java.util.List;

@Data
@AllArgsConstructor
public class StringBatch {
    private List<String> items;
}
