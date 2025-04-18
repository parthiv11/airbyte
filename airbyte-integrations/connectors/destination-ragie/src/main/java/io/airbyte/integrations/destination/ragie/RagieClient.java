package io.airbyte.integrations.destination.ragie;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airbyte.cdk.integrations.base.AirbyteTraceMessageUtility; // For error reporting
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Exception Class (can be in its own file)
class RagieApiException extends RuntimeException {
    public RagieApiException(String message) {
        super(message);
    }
    public RagieApiException(String message, Throwable cause) {
        super(message, cause);
    }
}

public class RagieClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(RagieClient.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String DOCUMENTS_RAW_ENDPOINT = "/documents/raw";
    private static final String DOCUMENTS_FILE_UPLOAD_ENDPOINT = "/documents";
    private static final String DOCUMENTS_GENERAL_ENDPOINT = "/documents";
    private static final String METADATA_AIRBYTE_STREAM_FIELD = "airbyte_stream"; // Keep consistent

    private final RagieConfig config;
    private final HttpClient httpClient;
    private final String baseUri;


    public RagieClient(RagieConfig config) {
        this.config = config;
        this.baseUri = config.getApiUrl().endsWith("/") ?
                config.getApiUrl().substring(0, config.getApiUrl().length() - 1) :
                config.getApiUrl();

        // Consider configuring timeouts, executor, etc.
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1) // Or HTTP_2 if supported
                .connectTimeout(Duration.ofSeconds(20))
                .build();
    }

    // --- Connection Check ---
    public Optional<String> checkConnection() {
        LOGGER.info("Performing connection check using GET {}?page_size=1 with partition scope: {}",
             DOCUMENTS_GENERAL_ENDPOINT, config.getPartitionOptional().orElse("Default/Account-wide"));
        URI uri;
        try {
            uri = new URI(baseUri + DOCUMENTS_GENERAL_ENDPOINT + "?page_size=1");
        } catch (URISyntaxException e) {
             return Optional.of("Invalid API URL format: " + baseUri);
        }

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(uri)
                .GET()
                .header("Authorization", "Bearer " + config.getApiKey())
                .header("Accept", "application/json")
                .header("X-source", "airbyte-destination-ragie");

        config.getPartitionOptional().ifPresent(p -> requestBuilder.header("partition", p));

        try {
            HttpResponse<String> response = httpClient.send(requestBuilder.build(), BodyHandlers.ofString());
            int statusCode = response.statusCode();
            LOGGER.info("Connection check response status: {}", statusCode);

            if (statusCode == 200) {
                LOGGER.info("Connection check successful.");
                return Optional.empty(); // Success
            } else if (statusCode == 401) {
                return Optional.of("Authentication failed: Invalid API Key.");
            } else if (statusCode == 403) {
                return Optional.of("Authorization failed: API Key lacks permissions for the specified partition or action.");
            } else if (statusCode >= 400 && statusCode < 500) {
                 String errorBody = response.body() != null ? response.body().substring(0, Math.min(500, response.body().length())) : "No body";
                 return Optional.of(String.format("Connection check failed with status %d. Check API URL/Partition. Response: %s", statusCode, errorBody));
            } else {
                 return Optional.of(String.format("Connection check failed with unexpected status %d.", statusCode));
            }
        } catch (IOException | InterruptedException e) {
            LOGGER.error("Connection check failed due to network error: {}", e.getMessage(), e);
            Thread.currentThread().interrupt(); // Restore interrupt status
            return Optional.of("Failed to connect to Ragie API at " + baseUri + ". Error: " + e.getMessage());
        } catch (Exception e) {
            LOGGER.error("Unexpected error during connection check: {}", e.getMessage(), e);
            return Optional.of("An unexpected error occurred during connection check: " + e.getMessage());
        }
    }


    // --- Indexing (Main Logic) ---
    public void indexDocuments(List<Map<String, Object>> documents) throws RagieApiException {
         if (documents == null || documents.isEmpty()) {
             return;
         }
         LOGGER.info("Indexing {} items one by one...", documents.size());
         int successfulCount = 0;

        for (Map<String, Object> itemPayload : documents) {
            String docIdLog = Optional.ofNullable(itemPayload.get("external_id"))
                    .map(Object::toString)
                    .orElseGet(() -> Optional.ofNullable(itemPayload.get("name")).map(Object::toString).orElse("N/A"));

            // Check for special "file_path" key added by writer
            String filePath = (String) itemPayload.remove("file_path"); // Remove it from payload

            try {
                if (filePath != null) {
                    // --- Handle File Upload ---
                    handleFileUpload(itemPayload, filePath, docIdLog);
                } else {
                    // --- Handle JSON Upload ---
                    handleJsonUpload(itemPayload, docIdLog);
                }
                successfulCount++;
            } catch (RagieApiException e) {
                 // Re-throw specific API errors immediately
                 throw e;
            } catch (Exception e) {
                // Wrap unexpected errors
                String itemType = (filePath != null) ? "file" : "document";
                LOGGER.error("Unexpected error indexing {} '{}': {}", itemType, docIdLog, e.getMessage(), e);
                AirbyteTraceMessageUtility.emitSystemErrorTrace(e, "Unexpected error indexing item: " + docIdLog);
                throw new RagieApiException("Failed to index " + itemType + " '" + docIdLog + "' due to unexpected error.", e);
            }
        }
        LOGGER.info("Successfully processed {} indexing requests.", successfulCount);
    }


    // --- File Upload Helper ---
    private void handleFileUpload(Map<String, Object> payload, String filePathStr, String docIdLog) throws RagieApiException, IOException, InterruptedException {
        Path filePath = Paths.get(filePathStr);
        if (!Files.exists(filePath) || !Files.isRegularFile(filePath)) {
            LOGGER.error("File path does not exist or is not a file, skipping: {} (ID: {})", filePathStr, docIdLog);
             AirbyteTraceMessageUtility.emitSystemErrorTrace(
                new IOException("File path does not exist: "+ filePathStr),
                "Cannot upload file, path not found or not a file: " + docIdLog);
            // Consider throwing or just returning to skip? Throwing is safer.
            throw new RagieApiException("File path does not exist or not a file: " + filePathStr);
        }

        String endpoint = DOCUMENTS_FILE_UPLOAD_ENDPOINT;
        String boundary = "Boundary-" + UUID.randomUUID().toString(); // Unique boundary
        Map<Object, Object> data = buildMultipartFormData(payload, filePath, boundary);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUri + endpoint))
                .header("Authorization", "Bearer " + config.getApiKey())
                .header("Accept", "application/json")
                .header("X-source", "airbyte-destination-ragie")
                .header("Content-Type", "multipart/form-data;boundary=" + boundary)
                .POST(MultipartBodyPublisher.ofMimeMultipartData(data, boundary))
                .timeout(Duration.ofMinutes(5)) // Longer timeout for uploads
                .build();

        LOGGER.debug("Uploading file '{}' (ID: {}) to {}...", filePath.getFileName(), docIdLog, endpoint);
        HttpResponse<String> response = httpClient.send(request, BodyHandlers.ofString());
        handleApiResponse(response, "File Upload", docIdLog);
        LOGGER.debug("Successfully requested upload for file: Name='{}', ExternalID='{}'",
                    payload.getOrDefault("name", "N/A"), payload.getOrDefault("external_id", "N/A"));
    }

     // --- Helper to build multipart form data map ---
    private Map<Object, Object> buildMultipartFormData(Map<String, Object> payload, Path filePath, String boundary) throws JsonProcessingException {
        Map<Object, Object> data = new java.util.LinkedHashMap<>(); // Preserves order

        // Add form fields from payload
        payload.forEach((key, value) -> {
             if (value != null && !"metadata".equals(key)) { // Metadata handled separately
                data.put(key, value.toString());
             }
        });

        // Add partition if configured
        config.getPartitionOptional().ifPresent(p -> data.put("partition", p));


        // Add metadata as JSON string if present
        if (payload.containsKey("metadata") && payload.get("metadata") instanceof Map) {
             try {
                String metadataJson = MAPPER.writeValueAsString(payload.get("metadata"));
                data.put("metadata", metadataJson);
             } catch (JsonProcessingException e) {
                  LOGGER.error("Failed to serialize metadata to JSON: {}", e.getMessage());
                  // Decide: throw or just omit metadata? Throwing is safer.
                  throw e;
             }
        }

        // Add the file itself
        data.put("file", filePath);

        return data;
    }


    // --- JSON Upload Helper ---
    private void handleJsonUpload(Map<String, Object> payload, String docIdLog) throws RagieApiException, IOException, InterruptedException {
         String endpoint = DOCUMENTS_RAW_ENDPOINT;
         HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(baseUri + endpoint))
                .header("Authorization", "Bearer " + config.getApiKey())
                .header("Accept", "application/json")
                .header("Content-Type", "application/json") // Explicitly set for JSON
                .header("X-source", "airbyte-destination-ragie")
                .timeout(Duration.ofSeconds(60)); // Standard timeout

         // Add partition header if configured
         config.getPartitionOptional().ifPresent(p -> requestBuilder.header("partition", p));

        try {
            String jsonBody = MAPPER.writeValueAsString(payload);
            requestBuilder.POST(BodyPublishers.ofString(jsonBody));
        } catch (JsonProcessingException e) {
             LOGGER.error("Failed to serialize JSON payload for document '{}': {}", docIdLog, e.getMessage());
             throw new RagieApiException("Failed to create JSON request body for " + docIdLog, e);
        }

        LOGGER.debug("Indexing JSON document via {}: Name='{}', ExternalID='{}'",
                    endpoint, payload.getOrDefault("name", "N/A"), payload.getOrDefault("external_id", "N/A"));
        HttpResponse<String> response = httpClient.send(requestBuilder.build(), BodyHandlers.ofString());
        handleApiResponse(response, "JSON Indexing", docIdLog);
         LOGGER.debug("Successfully requested indexing for JSON document: Name='{}', ExternalID='{}'",
                     payload.getOrDefault("name", "N/A"), payload.getOrDefault("external_id", "N/A"));
    }


    // --- Common API Response Handling ---
    private void handleApiResponse(HttpResponse<String> response, String operation, String contextId) throws RagieApiException {
        int statusCode = response.statusCode();
        LOGGER.debug("{} Response Status Code: {}", operation, statusCode);

        if (statusCode < 200 || statusCode >= 300) { // Check for non-2xx status
            String errorBody = response.body() != null ? response.body() : "No response body";
            String errorMessage = String.format("%s failed for '%s' with status %d. Response: %s",
                    operation, contextId, statusCode, errorBody.substring(0, Math.min(500, errorBody.length())));
            LOGGER.error(errorMessage);

            // Emit trace message based on status code
            if (statusCode >= 400 && statusCode < 500 && statusCode != 404 && statusCode != 429) {
                 AirbyteTraceMessageUtility.emitConfigErrorTrace(new RagieApiException(errorMessage), "Operation failed likely due to configuration or input data. Context: " + contextId);
            } else {
                 AirbyteTraceMessageUtility.emitSystemErrorTrace(new RagieApiException(errorMessage), "Operation failed due to API error or transient issue. Context: " + contextId);
            }
            throw new RagieApiException(errorMessage);
        }
        // Success case, potentially log parts of the response if useful
        // e.g., JsonNode responseJson = MAPPER.readTree(response.body()); LOGGER.debug("Success Response: {}", responseJson);
    }


    // --- Filter/Delete Methods (Requires translating Python _build_filter_json, pagination, etc.) ---
    // Placeholder - Implement similarly to Python version, using makeRequest/handleApiResponse
    public List<String> findIdsByMetadata(Map<String, Object> filterConditions) throws RagieApiException {
        LOGGER.warn("findIdsByMetadata not fully implemented yet.");
        // TODO: Implement filtering and pagination logic
        return Collections.emptyList();
    }

    public List<JsonNode> findDocsByMetadata(Map<String, Object> filterConditions, List<String> fields) throws RagieApiException {
        LOGGER.warn("findDocsByMetadata not fully implemented yet.");
        // TODO: Implement filtering, field selection, and pagination logic
        return Collections.emptyList();
    }

     public void deleteDocumentsById(List<String> internalIds) throws RagieApiException {
        if (internalIds == null || internalIds.isEmpty()) {
            return;
        }
        LOGGER.info("Attempting to delete {} documents by internal ID.", internalIds.size());
        int successfulDeletes = 0;
        int failedDeletes = 0;

        for (String internalId : internalIds) {
            if (internalId == null || internalId.isBlank()) {
                LOGGER.warn("Invalid internal ID received for deletion: '{}'. Skipping.", internalId);
                failedDeletes++;
                continue;
            }

            String endpoint = DOCUMENTS_GENERAL_ENDPOINT + "/" + internalId;
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(baseUri + endpoint))
                    .DELETE()
                    .header("Authorization", "Bearer " + config.getApiKey())
                    .header("Accept", "application/json")
                    .header("X-source", "airbyte-destination-ragie");

            config.getPartitionOptional().ifPresent(p -> requestBuilder.header("partition", p));

            try {
                HttpResponse<String> response = httpClient.send(requestBuilder.build(), BodyHandlers.ofString());
                int statusCode = response.statusCode();
                if (statusCode >= 200 && statusCode < 300) {
                    successfulDeletes++;
                    LOGGER.debug("Successfully deleted document with internal_id: {}", internalId);
                } else if (statusCode == 404) {
                     successfulDeletes++; // Count 404 as success for deletion
                     LOGGER.warn("Document internal_id {} not found for deletion (404). Assuming deleted.", internalId);
                } else {
                    // Handle other errors via handleApiResponse logic
                     handleApiResponse(response, "Delete", internalId); // This will throw RagieApiException
                     failedDeletes++; // Should not be reached if handleApiResponse throws
                }
            } catch (RagieApiException e) {
                 // Exception already logged and traced by handleApiResponse or caught below
                 failedDeletes++;
            } catch (IOException | InterruptedException e) {
                LOGGER.error("Network error deleting document internal_id {}: {}", internalId, e.getMessage());
                 Thread.currentThread().interrupt();
                 failedDeletes++;
                 // Optional: Re-throw wrapped exception if needed to fail fast
                 // throw new RagieApiException("Network error during deletion of " + internalId, e);
            } catch (Exception e) {
                LOGGER.error("Unexpected error deleting document internal_id {}: {}", internalId, e.getMessage(), e);
                 failedDeletes++;
            }
        }

        LOGGER.info("Deletion result: {} successful (incl 404s), {} failures.", successfulDeletes, failedDeletes);
        if (failedDeletes > 0) {
             // Throw exception if any non-404 failures occurred
             throw new RagieApiException(String.format("Failed to delete %d out of %d documents (excluding 404s). Check logs.", failedDeletes, internalIds.size()));
        }
    }

    // --- Multipart Body Publisher Helper (Needed for File Uploads) ---
    // Source: Adapted from Java docs examples - https://openjdk.java.net/groups/net/httpclient/recipes.html#upload
    private static class MultipartBodyPublisher {
        private List<byte[]> byteArrays = new ArrayList<>();
        private String boundary;

        private MultipartBodyPublisher() {} // Private constructor

        public static BodyPublisher ofMimeMultipartData(Map<Object, Object> data, String boundary) throws IOException {
            MultipartBodyPublisher publisher = new MultipartBodyPublisher();
            publisher.boundary = boundary;
            byte[] separator = ("--" + boundary + "\r\nContent-Disposition: form-data; name=").getBytes(StandardCharsets.UTF_8);
            byte[] footer = ("--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8);

            for (Map.Entry<Object, Object> entry : data.entrySet()) {
                publisher.byteArrays.add(separator);

                if (entry.getValue() instanceof Path) {
                    Path path = (Path) entry.getValue();
                    String mimeType = Files.probeContentType(path);
                    if (mimeType == null) {
                        mimeType = "application/octet-stream"; // Default fallback
                    }
                    byte[] fileHeader = ("\"" + entry.getKey() + "\"; filename=\"" + path.getFileName()
                            + "\"\r\nContent-Type: " + mimeType + "\r\n\r\n").getBytes(StandardCharsets.UTF_8);
                    publisher.byteArrays.add(fileHeader);
                    publisher.byteArrays.add(Files.readAllBytes(path));
                    publisher.byteArrays.add("\r\n".getBytes(StandardCharsets.UTF_8));
                } else {
                    byte[] valueBytes = entry.getValue().toString().getBytes(StandardCharsets.UTF_8);
                    byte[] fieldHeader = ("\"" + entry.getKey() + "\"\r\n\r\n").getBytes(StandardCharsets.UTF_8);
                    publisher.byteArrays.add(fieldHeader);
                    publisher.byteArrays.add(valueBytes);
                    publisher.byteArrays.add("\r\n".getBytes(StandardCharsets.UTF_8));
                }
            }
            publisher.byteArrays.add(footer);
            return BodyPublishers.ofByteArrays(publisher.byteArrays);
        }
    }
}