package com.llmgateway.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ChatMessage {

    @JsonProperty("role")
    private String role;

    @JsonProperty("content")
    private Object content;

    @JsonProperty("name")
    private String name;

    public ChatMessage(String role, String content) {
        this.role = role;
        this.content = content;
    }

    /**
     * Helper to safely extract string content regardless of whether content
     * is a plain String or a structured list (multi-modal parts).
     */
    @JsonIgnore
    public String getContentAsString() {
        if (content == null) {
            return "";
        }
        return content.toString();
    }
}

