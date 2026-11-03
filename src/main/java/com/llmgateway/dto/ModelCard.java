package com.llmgateway.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ModelCard {

    @JsonProperty("id")
    private String id;

    @JsonProperty("object")
    @Builder.Default
    private String object = "model";

    @JsonProperty("created")
    @Builder.Default
    private Long created = System.currentTimeMillis() / 1000;

    @JsonProperty("owned_by")
    @Builder.Default
    private String ownedBy = "llm-gateway";

    @JsonProperty("permission")
    @Builder.Default
    private List<Object> permission = Collections.emptyList();

    @JsonProperty("root")
    private String root;

    @JsonProperty("parent")
    private String parent;
}

