package com.llmgateway.controller;

import com.llmgateway.dto.ModelCard;
import com.llmgateway.dto.ModelListResponse;
import com.llmgateway.service.ModelRoutingService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/v1/models")
public class ModelController {

    private final ModelRoutingService routingService;

    public ModelController(ModelRoutingService routingService) {
        this.routingService = routingService;
    }

    @GetMapping
    public ResponseEntity<ModelListResponse> listModels() {
        List<ModelCard> models = routingService.listModels();
        return ResponseEntity.ok(ModelListResponse.builder()
                .data(models)
                .build());
    }

    @GetMapping("/{model}")
    public ResponseEntity<ModelCard> getModel(@PathVariable("model") String model) {
        return routingService.getModel(model)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}

