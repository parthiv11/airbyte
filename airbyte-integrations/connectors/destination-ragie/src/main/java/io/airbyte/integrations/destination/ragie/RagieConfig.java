package io.airbyte.integrations.destination.ragie;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.airbyte.cdk.integrations.destination.StandardNameTransformer; // For potential key cleaning
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Using Lombok for brevity, replace with manual getters if not using Lombok
import lombok.Getter;

@Getter
public class RagieConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(RagieConfig.class);
    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private final String apiKey;
    private final String apiUrl;
    private final String partition; // Nullable
    private final String processingMode;
    private final List<String> metadataFields;
    private final Map<String, Object> metadataStatic;
    private final String documentNameField; // Nullable
    private final String externalIdField; // Nullable
    private final int logInterval;

    public RagieConfig(
            String apiKey,
            String apiUrl,
            String partition,
            String processingMode,
            List<String> metadataFields,
            Map<String, Object> metadataStatic,
            String documentNameField,
            String externalIdField,
            int logInterval) {
        this.apiKey = apiKey;
        this.apiUrl = apiUrl;
        this.partition = (partition != null && partition.isBlank()) ? null : partition; // Treat blank as null
        this.processingMode = processingMode;
        this.metadataFields = metadataFields != null ? metadataFields : Collections.emptyList();
        this.metadataStatic = metadataStatic != null ? metadataStatic : Collections.emptyMap();
        this.documentNameField = (documentNameField != null && documentNameField.isBlank()) ? null : documentNameField;
        this.externalIdField = (externalIdField != null && externalIdField.isBlank()) ? null : externalIdField;
        this.logInterval = logInterval;

        validate();
    }

    private void validate() {
        if (partition != null && !partition.matches("^[a-z0-9_\\-]*$")) {
            throw new IllegalArgumentException("Partition name must be lowercase alphanumeric with '-' or '_'. Found: " + partition);
        }
        if (apiKey == null || apiKey.isBlank()) {
             throw new IllegalArgumentException("API Key cannot be empty.");
        }
        if (!List.of("fast", "hi-res").contains(processingMode)) {
             throw new IllegalArgumentException("Processing mode must be 'fast' or 'hi_res'. Found: " + processingMode);
        }
         if (logInterval < 0) {
             throw new IllegalArgumentException("Log Interval cannot be negative.");
        }
    }


    public static RagieConfig get(JsonNode config) {
        try {
            String partition = config.has("partition") ? config.get("partition").asText(null) : null;
            Map<String, Object> staticMetadata = config.has("metadata_static") ?
                    MAPPER.convertValue(config.get("metadata_static"), Map.class) :
                    Collections.emptyMap();

            List<String> metadataFieldsList = config.has("metadata_fields") ?
                 MAPPER.convertValue(config.get("metadata_fields"), List.class) : // Assuming it's already an array from spec
                 Collections.emptyList();


            return new RagieConfig(
                    config.get("api_key").asText(),
                    config.get("api_url").asText("https://api.ragie.ai"), // Use default from spec
                    partition,
                    config.get("processing_mode").asText("fast"), // Use default from spec
                    metadataFieldsList,
                    staticMetadata,
                    config.has("document_name_field") ? config.get("document_name_field").asText(null) : null,
                    config.has("external_id_field") ? config.get("external_id_field").asText(null) : null,
                    config.get("log_interval").asInt(500) // Use default from spec
            );
        } catch (Exception e) {
            LOGGER.error("Error parsing Ragie configuration: {}", e.getMessage(), e);
            throw new IllegalArgumentException("Invalid Ragie configuration provided.", e);
        }
    }

     // Helper to safely get partition, returning null if blank or null
     public Optional<String> getPartitionOptional() {
        return Optional.ofNullable(partition).filter(p -> !p.isBlank());
     }
}