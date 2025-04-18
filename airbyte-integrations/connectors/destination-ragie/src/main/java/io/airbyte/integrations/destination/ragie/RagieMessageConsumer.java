package io.airbyte.integrations.destination.ragie;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.cdk.integrations.base.AirbyteMessageConsumer;
import io.airbyte.cdk.integrations.base.AirbyteTraceMessageUtility; // For logging errors
import io.airbyte.cdk.integrations.base.FailureTrackingAirbyteMessageConsumer; // Optional: Use for better failure tracking
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.AirbyteRecordMessage;
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream;
import io.airbyte.protocol.models.v0.DestinationSyncMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.commons.io.FilenameUtils; // If needed for path manipulation
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;


public class RagieMessageConsumer extends FailureTrackingAirbyteMessageConsumer { // Extend FailureTracking... for convenience

    private static final Logger LOGGER = LoggerFactory.getLogger(RagieMessageConsumer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final RagieClient client;
    private final RagieConfig config;
    private final ConfiguredAirbyteCatalog catalog;
    private final Map<AirbyteStreamNameNamespacePair, ConfiguredAirbyteStream> streamConfigMap;
    private final Consumer<AirbyteMessage> outputRecordCollector; // Needed by FailureTracking...

    // Deduplication state
    private final Map<AirbyteStreamNameNamespacePair, Set<String>> seenHashes;
    private final Set<AirbyteStreamNameNamespacePair> hashesPreloaded;


    public RagieMessageConsumer(
            RagieClient client,
            RagieConfig config,
            ConfiguredAirbyteCatalog catalog,
            Consumer<AirbyteMessage> outputRecordCollector) {
        this.client = client;
        this.config = config;
        this.catalog = catalog;
        this.outputRecordCollector = outputRecordCollector;
        this.streamConfigMap = catalog.getStreams().stream()
                .collect(Collectors.toMap(AirbyteStreamNameNamespacePair::fromConfiguredAirbyteSteam, s -> s));
        this.seenHashes = new HashMap<>();
        this.hashesPreloaded = new HashSet<>();
        LOGGER.info("RagieMessageConsumer initialized.");
    }

    @Override
    protected void startTracked() throws Exception {
        LOGGER.info("Starting tracked consumer operations.");
        // 1. Handle Overwrite Mode
        deleteStreamsToOverwrite();
        // 2. Preload Hashes for Dedup Streams (if any)
        preloadHashesForDedupStreams();
    }

    @Override
    protected void acceptTracked(AirbyteMessage message) throws Exception {
        switch (message.getType()) {
            case RECORD -> processRecord(message.getRecord());
            case STATE -> {
                LOGGER.debug("Received STATE message. Flushing (currently no-op).");
                // Flush is implicitly handled by FailureTrackingAirbyteMessageConsumer framework
                // or could be explicitly called if needed: flush();
                outputRecordCollector.accept(message); // Emit state
            }
            default -> LOGGER.debug("Ignoring message type: {}", message.getType());
        }
    }

    private void processRecord(AirbyteRecordMessage recordMessage) {
        AirbyteStreamNameNamespacePair streamPair = AirbyteStreamNameNamespacePair.fromRecordMessage(recordMessage);
        ConfiguredAirbyteStream streamConfig = streamConfigMap.get(streamPair);

        if (streamConfig == null) {
            LOGGER.warn("Stream config not found for '{}:{}'. Skipping record.", streamPair.getNamespace(), streamPair.getName());
            return;
        }

        JsonNode recordData = recordMessage.getData();
        if (recordData == null || !recordData.isObject()) {
             LOGGER.warn("Record data is null or not an object in stream '{}'. Skipping.", streamPair.getName());
             return;
        }

        LOGGER.debug("Processing record for stream '{}:{}'. Record data keys: {}",
                     streamPair.getNamespace(), streamPair.getName(), recordData.fieldNames());

        Map<String, Object> payload = new HashMap<>();
        JsonNode fileInfoNode = null;
        String filePathToUpload = null;
        boolean isFileBased = false;

        // --- 1. Identify Record Type & Extract Content/File Path ---
        if (recordData.has("file") && recordData.get("file").isObject()) {
            JsonNode fileNode = recordData.get("file");
            if (fileNode.has("file_url") && fileNode.get("file_url").isTextual()) {
                 String fileUrl = fileNode.get("file_url").asText();
                 if (fileUrl != null && !fileUrl.isBlank()) {
                    isFileBased = true;
                    fileInfoNode = fileNode;
                    String localFilePath = fileUrl.startsWith("file://") ? fileUrl.substring(7) : fileUrl;
                     Path path = Paths.get(localFilePath);
                     if (!Files.exists(path) || !Files.isRegularFile(path)) {
                         String errorMsg = String.format("File not found or not a file at path: '%s' (from file.file_url). Skipping record.", localFilePath);
                         LOGGER.error(errorMsg);
                         // Throw runtime to be caught by FailureTracking...
                         throw new RuntimeException(errorMsg);
                     }
                     filePathToUpload = localFilePath;
                     payload.put("file_path", filePathToUpload); // Signal to client
                     LOGGER.debug("Handling as file-based record. Path: {}", filePathToUpload);
                 }
            }
        }

        // --- 2. Prepare JSON Content if Not File-Based ---
        if (!isFileBased) {
            if (recordData.isEmpty()) {
                 LOGGER.warn("JSON record data for stream '{}' is empty. Skipping.", streamPair.getName());
                 return;
            }
            // Convert JsonNode to Map for JSON upload payload
            Map<String, Object> jsonDataMap = MAPPER.convertValue(recordData, Map.class);
            payload.put("data", jsonDataMap); // Key for JSON upload
            LOGGER.debug("Handling as JSON-based record.");
        }

        // --- 3. Prepare Metadata ---
        Map<String, Object> finalMetadata = prepareMetadata(recordData, streamPair);

        // --- 4. Determine Name & External ID ---
        String docName = getDocumentName(recordData, fileInfoNode, streamPair);
        payload.put("name", docName);

        getOptionalField(recordData, config.getExternalIdField()).ifPresent(extId ->
             payload.put("external_id", extId)
        );

        // --- 5. Calculate Hash & Add to Metadata ---
        // Convert fileInfoNode and content_to_send (payload.get("data")) to maps for hashing function if needed
        Map<String, Object> fileInfoMap = isFileBased ? MAPPER.convertValue(fileInfoNode, Map.class) : null;
        Map<String, Object> contentMap = !isFileBased ? (Map<String, Object>) payload.get("data") : null;

        // Calculate hash using metadata *before* adding the hash field itself
        Map<String, Object> tempMetadataForHash = new HashMap<>(finalMetadata);
        String contentHash = calculateContentHash(tempMetadataForHash, contentMap, fileInfoMap);
        finalMetadata.put(RagieClient.METADATA_CONTENT_HASH_FIELD, contentHash);
        payload.put("metadata", finalMetadata); // Add final metadata (including hash)

        // --- 6. Deduplication Check ---
        if (streamConfig.getDestinationSyncMode() == DestinationSyncMode.APPEND_DEDUP) {
             Set<String> hashesForStream = seenHashes.computeIfAbsent(streamPair, k -> new HashSet<>());
             if (hashesForStream.contains(contentHash)) {
                 LOGGER.info("Skipping duplicate record in stream '{}:{}' (Hash: {}, Name: '{}').",
                             streamPair.getNamespace(), streamPair.getName(), contentHash, docName);
                 return; // Skip
             } else {
                 hashesForStream.add(contentHash); // Add hash for this run
             }
        }

        // --- 7. Add Other Parameters ---
        payload.put("mode", config.getProcessingMode());

        // --- 8. Send to Client ---
        try {
             String logType = isFileBased ? "File" : "JSON";
             LOGGER.debug("Queueing {} payload for '{}' (Stream: {}, Hash: {})",
                          logType, docName, streamPair, contentHash);
             client.indexDocuments(List.of(payload)); // Send immediately
        } catch (Exception e) {
             // Let FailureTrackingAirbyteMessageConsumer handle reporting this exception
             String errorMsg = String.format("Failed to index item '%s' (Stream: %s): %s", docName, streamPair, e.getMessage());
             LOGGER.error(errorMsg, e);
             throw new RuntimeException(errorMsg, e); // Wrap and rethrow
        }
    }

    // --- Helper Methods ---

    private void deleteStreamsToOverwrite() {
        List<String> streamsToDelete = catalog.getStreams().stream()
                .filter(s -> s.getDestinationSyncMode() == DestinationSyncMode.OVERWRITE)
                .map(s -> streamTupleToId(s.getStream().getNamespace(), s.getStream().getName()))
                .toList();

        if (streamsToDelete.isEmpty()) {
             return;
        }
        LOGGER.info("OVERWRITE mode for streams: {}. Deleting existing data...", streamsToDelete);

        Set<String> allIdsToDelete = new HashSet<>();
        for(ConfiguredAirbyteStream stream : catalog.getStreams()) {
             if (stream.getDestinationSyncMode() == DestinationSyncMode.OVERWRITE) {
                  String streamId = streamTupleToId(stream.getStream().getNamespace(), stream.getStream().getName());
                  LOGGER.info("Finding existing documents for stream '{}'...", streamId);
                  Map<String, Object> filter = Map.of(RagieClient.METADATA_AIRBYTE_STREAM_FIELD, streamId);
                  try {
                      List<String> ids = client.findIdsByMetadata(filter);
                      if (!ids.isEmpty()) {
                          LOGGER.info("Found {} document IDs for stream '{}'.", ids.size(), streamId);
                          allIdsToDelete.addAll(ids);
                      } else {
                          LOGGER.info("No existing documents found for stream '{}'.", streamId);
                      }
                  } catch (Exception e) {
                       String errorMsg = "Failed to query existing documents for overwrite stream '" + streamId + "'.";
                       LOGGER.error(errorMsg, e);
                       throw new RuntimeException(errorMsg, e);
                  }
             }
        }

        if (!allIdsToDelete.isEmpty()) {
             LOGGER.info("Attempting deletion of {} documents for streams: {}", allIdsToDelete.size(), streamsToDelete);
             try {
                 client.deleteDocumentsById(new ArrayList<>(allIdsToDelete));
                 LOGGER.info("Successfully processed deletion requests for overwrite streams.");
             } catch (Exception e) {
                 String errorMsg = "Failed during document deletion for overwrite streams.";
                 LOGGER.error(errorMsg, e);
                 throw new RuntimeException(errorMsg, e);
             }
        } else {
            LOGGER.info("No documents found to delete across overwrite streams.");
        }
    }

    private void preloadHashesForDedupStreams() {
        catalog.getStreams().stream()
                .filter(s -> s.getDestinationSyncMode() == DestinationSyncMode.APPEND_DEDUP)
                .map(AirbyteStreamNameNamespacePair::fromConfiguredAirbyteSteam)
                .forEach(this::preloadHashesIfNeeded); // Call helper for each dedup stream
    }

     private void preloadHashesIfNeeded(AirbyteStreamNameNamespacePair streamPair) {
        if (hashesPreloaded.contains(streamPair)) {
            return;
        }
        LOGGER.info("Preloading hashes for stream '{}:{}'...", streamPair.getNamespace(), streamPair.getName());
        try {
             Map<String, Object> filter = Map.of(RagieClient.METADATA_AIRBYTE_STREAM_FIELD, streamTupleToId(streamPair.getNamespace(), streamPair.getName()));
             // Fetch only metadata containing the hash field
             List<JsonNode> existingDocs = client.findDocsByMetadata(filter, List.of("metadata"));
             Set<String> hashes = new HashSet<>();
             int docsWithoutHash = 0;

             for (JsonNode doc : existingDocs) {
                 if (doc.has("metadata") && doc.get("metadata").has(RagieClient.METADATA_CONTENT_HASH_FIELD)) {
                     hashes.add(doc.get("metadata").get(RagieClient.METADATA_CONTENT_HASH_FIELD).asText());
                 } else {
                     docsWithoutHash++;
                 }
             }
             seenHashes.put(streamPair, hashes);
             String logMsg = String.format("Finished preloading for '%s:%s'. Found %d existing hashes.",
                     streamPair.getNamespace(), streamPair.getName(), hashes.size());
            if (docsWithoutHash > 0) logMsg += String.format(" (%d docs missing hash field).", docsWithoutHash);
            LOGGER.info(logMsg);

        } catch (Exception e) {
            LOGGER.error("Failed to preload hashes for stream '{}:{}': {}", streamPair.getNamespace(), streamPair.getName(), e.getMessage(), e);
             seenHashes.put(streamPair, new HashSet<>()); // Ensure set exists even on failure
             LOGGER.warn("Deduplication for '{}:{}' may be incomplete due to hash preload failure.", streamPair.getNamespace(), streamPair.getName());
             // Do not rethrow, allow sync to continue but without guaranteed dedup for this stream
        } finally {
            hashesPreloaded.add(streamPair); // Mark as attempted
        }
    }

    private Map<String, Object> prepareMetadata(JsonNode recordData, AirbyteStreamNameNamespacePair streamPair) {
         Map<String, Object> combined = new HashMap<>(config.getMetadataStatic());

         // Add configured fields
         for (String fieldPath : config.getMetadataFields()) {
             getValueFromPath(recordData, fieldPath).ifPresent(value -> {
                 String key = fieldPath.replace('.', '_'); // Basic sanitization
                 // Validate value type for Ragie (string, number, boolean, list<string>)
                 Object processedValue = processMetadataValue(value, key, fieldPath);
                 if (processedValue != null) {
                     combined.put(key, processedValue);
                 }
             });
         }

         // Add internal field
         combined.put(RagieClient.METADATA_AIRBYTE_STREAM_FIELD, streamTupleToId(streamPair.getNamespace(), streamPair.getName()));

         // Clean keys (reserved words, problematic chars)
         Map<String, Object> finalMetadata = new HashMap<>();
        combined.forEach((key, value) -> {
             String cleanedKey = cleanMetadataKey(key);
             if (cleanedKey != null) { // cleanMetadataKey returns null if key becomes empty
                 finalMetadata.put(cleanedKey, value);
             }
         });

         // Check size limit (optional)
         if (finalMetadata.size() > 1000) { // Rough check
             LOGGER.warn("Metadata count ({}) > 1000 for record in stream {}. Ragie might truncate/reject.", finalMetadata.size(), streamPair);
         }
        return finalMetadata;
    }

    // Helper to process/validate metadata values
    private Object processMetadataValue(Object value, String key, String fieldPath) {
         if (value instanceof String || value instanceof Boolean) {
             return value;
         } else if (value instanceof Number) {
              // Check for non-finite floats
              if (value instanceof Double && !Double.isFinite((Double) value)) {
                   LOGGER.warn("Skipping non-finite float metadata field '{}' (path: {}). Value: {}", key, fieldPath, value);
                   return null;
              }
              if (value instanceof Float && !Float.isFinite((Float) value)) {
                   LOGGER.warn("Skipping non-finite float metadata field '{}' (path: {}). Value: {}", key, fieldPath, value);
                   return null;
              }
              return value; // Keep as Number (int, long, float, double)
         } else if (value instanceof List<?> listValue) {
              // Check if it's a list of strings
              if (listValue.stream().allMatch(item -> item instanceof String)) {
                  return value;
              } else {
                   LOGGER.warn("Metadata field '{}' (path: {}) is a list but contains non-string items. Converting to string.", key, fieldPath);
                   return value.toString(); // Convert whole list to string
              }
         } else {
              // Convert other types to string as fallback
              LOGGER.debug("Converting metadata field '{}' (path: {}, type: {}) to string.", key, fieldPath, value.getClass().getSimpleName());
              return value.toString();
         }
    }


    // Helper to clean metadata keys
    private String cleanMetadataKey(String key) {
        if (key == null) return null;
        String cleanKey = key.strip();
        if (cleanKey.isEmpty()) {
            LOGGER.warn("Skipping metadata field with empty key (original: '{}').", key);
            return null;
        }
        String newKey = cleanKey;

        if (RagieClient.RESERVED_METADATA_KEYS.contains(newKey) || newKey.startsWith("_")) {
            String tempKey = newKey.lstrip("_");
            newKey = tempKey.isEmpty() ? "_" : tempKey + "_";
             if (!newKey.equals(key)) LOGGER.debug("Adjusted reserved/internal metadata key '{}' to '{}'", key, newKey);
        }

        // Replace common problematic chars with underscore
        String originalKey = newKey;
        newKey = newKey.replaceAll("[.$\\[\\] ]", "_"); // Replace '.', '$', space, brackets
        if (!newKey.equals(originalKey)) {
            LOGGER.warn("Adjusted metadata key '{}' to '{}' due to problematic characters.", originalKey, newKey);
        }

        // Final check if cleaned key is empty or reserved again
        if (newKey.isEmpty()) {
            LOGGER.warn("Skipping metadata field - key became empty after cleaning (original: '{}').", key);
            return null;
        }
         if (RagieClient.RESERVED_METADATA_KEYS.contains(newKey)) {
             newKey = newKey + "_";
             LOGGER.debug("Post-cleaning key '{}' resulted in reserved key, appended underscore -> '{}'", key, newKey);
        }
        return newKey;
    }


    // Helper to get document name
    private String getDocumentName(JsonNode recordData, JsonNode fileInfoNode, AirbyteStreamNameNamespacePair streamPair) {
        return getOptionalField(recordData, config.getDocumentNameField())
                .orElseGet(() -> {
                    if (fileInfoNode != null && fileInfoNode.has("file_relative_path")) {
                        return fileInfoNode.get("file_relative_path").asText(null); // Use relative path for files
                    }
                    return null; // Fallback triggers UUID generation
                })
                .orElseGet(() -> String.format("airbyte_%s_%s", // Generate UUID if no name found
                                             streamTupleToId(streamPair.getNamespace(), streamPair.getName()),
                                             UUID.randomUUID()));
    }

    // Helper to get optional field value as String
    private Optional<String> getOptionalField(JsonNode recordData, String fieldPath) {
        if (fieldPath == null || fieldPath.isBlank()) {
            return Optional.empty();
        }
        return getValueFromPath(recordData, fieldPath).map(Object::toString);
    }

    // Helper to traverse JsonNode using dot notation path
    private Optional<Object> getValueFromPath(JsonNode node, String path) {
        if (node == null || path == null) return Optional.empty();
        String[] keys = path.split("\\.");
        JsonNode current = node;
        for (String key : keys) {
            if (current == null || current.isMissingNode()) return Optional.empty();
            if (current.isObject()) {
                 current = current.get(key);
            } else if (current.isArray() && key.matches("\\d+")) { // Basic array indexing
                 int index = Integer.parseInt(key);
                 if (index >= 0 && index < current.size()) {
                     current = current.get(index);
                 } else {
                     return Optional.empty(); // Index out of bounds
                 }
            } else {
                 return Optional.empty(); // Cannot traverse further
            }
        }
        // Convert JsonNode to Java object before returning
        if (current == null || current.isMissingNode() || current.isNull()) return Optional.empty();
        if (current.isTextual()) return Optional.of(current.asText());
        if (current.isBoolean()) return Optional.of(current.asBoolean());
        if (current.isInt()) return Optional.of(current.asInt());
        if (current.isLong()) return Optional.of(current.asLong());
        if (current.isDouble()) return Optional.of(current.asDouble());
        if (current.isArray()) return Optional.of(MAPPER.convertValue(current, List.class));
        if (current.isObject()) return Optional.of(MAPPER.convertValue(current, Map.class));

        return Optional.of(current.toString()); // Fallback to string
    }


    // Helper to calculate hash (using Java's MessageDigest)
     private String calculateContentHash(Map<String, Object> metadata, Map<String, Object> content, Map<String, Object> fileInfo) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            String contentPart = "";
            String metadataPart = "";

            if (fileInfo != null) {
                Map<String, Object> stableFileInfo = new HashMap<>();
                stableFileInfo.put("path", fileInfo.get("file_relative_path"));
                stableFileInfo.put("modified", fileInfo.get("modified"));
                stableFileInfo.put("size", fileInfo.get("bytes"));
                stableFileInfo.values().removeIf(java.util.Objects::isNull); // Remove nulls
                contentPart = MAPPER.writeValueAsString(stableFileInfo); // Assumes stableFileInfo keys are sorted implicitly or sorting is added
            } else if (content != null) {
                 contentPart = MAPPER.writeValueAsString(content); // Assumes map keys are sorted implicitly or sorting is added
            }

            // Hash metadata excluding internal fields
            Map<String, Object> hashableMetadata = new HashMap<>(metadata);
            hashableMetadata.remove(RagieClient.METADATA_AIRBYTE_STREAM_FIELD);
            // METADATA_CONTENT_HASH_FIELD is not yet in metadata when this is called

            metadataPart = MAPPER.writeValueAsString(hashableMetadata); // Needs reliable sorting if keys aren't naturally ordered

            String combined = contentPart + "::" + metadataPart;
            byte[] hashBytes = digest.digest(combined.getBytes(StandardCharsets.UTF_8));

            // Convert byte array to hex string
            StringBuilder hexString = new StringBuilder(2 * hashBytes.length);
            for (byte b : hashBytes) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            return hexString.toString();

        } catch (NoSuchAlgorithmException | JsonProcessingException e) {
            LOGGER.error("Failed to calculate content hash", e);
            // Fallback to random UUID to avoid skipping everything on hash failure
            return UUID.randomUUID().toString();
        }
    }

    private String streamTupleToId(String namespace, String name) {
         return (namespace != null ? namespace + "_" : "") + name;
    }


    @Override
    protected void closeTracked() throws Exception {
        LOGGER.info("Closing Ragie message consumer.");
        // No explicit flushing needed here as uploads are synchronous
    }
}