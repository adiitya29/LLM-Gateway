package com.llmgateway.service;

import com.knuddels.jtokkit.Encodings;
import com.knuddels.jtokkit.api.Encoding;
import com.knuddels.jtokkit.api.EncodingRegistry;
import com.knuddels.jtokkit.api.EncodingType;
import com.llmgateway.dto.ChatCompletionRequest;
import com.llmgateway.dto.ChatMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TokenizerService {

    private static final Logger log = LoggerFactory.getLogger(TokenizerService.class);
    private final EncodingRegistry registry;
    private final Encoding defaultEncoding;

    public TokenizerService() {
        this.registry = Encodings.newDefaultEncodingRegistry();
        this.defaultEncoding = registry.getEncoding(EncodingType.CL100K_BASE);
    }

    /**
     * Resolves an encoding for the requested model or falls back to CL100K_BASE.
     */
    public Encoding getEncoding(String model) {
        if (model == null) {
            return defaultEncoding;
        }
        try {
            return registry.getEncodingForModel(model).orElse(defaultEncoding);
        } catch (Exception e) {
            return defaultEncoding;
        }
    }

    /**
     * Counts tokens for an arbitrary text string.
     */
    public int countTokens(String text, String model) {
        if (text == null || text.isEmpty()) {
            return 0;
        }
        Encoding encoding = getEncoding(model);
        return encoding.countTokens(text);
    }

    /**
     * Counts estimated prompt tokens for an entire ChatCompletionRequest according to OpenAI specifications.
     */
    public int countPromptTokens(ChatCompletionRequest request) {
        if (request == null || request.getMessages() == null || request.getMessages().isEmpty()) {
            return 0;
        }

        Encoding encoding = getEncoding(request.getModel());
        int tokens = 3; // Assistant reply prompt priming overhead (<|im_start|>assistant)

        for (ChatMessage msg : request.getMessages()) {
            tokens += 3; // <|im_start|> + role + \n + <|im_end|>
            if (msg.getRole() != null) {
                tokens += encoding.countTokens(msg.getRole());
            }
            if (msg.getContent() != null) {
                tokens += encoding.countTokens(msg.getContentAsString());
            }
            if (msg.getName() != null) {
                tokens += encoding.countTokens(msg.getName());
                tokens += 1; // additional overhead when name is present
            }
        }

        return tokens;
    }
}
