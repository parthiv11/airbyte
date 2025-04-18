package io.airbyte.integrations.destination.ragie;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.cdk.integrations.BaseConnector;
import io.airbyte.cdk.integrations.base.AirbyteMessageConsumer;
import io.airbyte.cdk.integrations.base.Destination;
import io.airbyte.cdk.integrations.base.IntegrationRunner;
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
import java.util.Optional;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RagieDestination extends BaseConnector implements Destination {

    private static final Logger LOGGER = LoggerFactory.getLogger(RagieDestination.class);

    public RagieDestination() {} // Public constructor needed

    @Override
    public ConnectorSpecification spec() throws Exception {
        return loadSpec("spec.json"); // Helper method from BaseConnector
    }

    @Override
    public AirbyteConnectionStatus check(JsonNode config) throws Exception {
        try {
            RagieConfig ragieConfig = RagieConfig.get(config);
            RagieClient client = new RagieClient(ragieConfig);
            Optional<String> errorMessage = client.checkConnection();

            if (errorMessage.isPresent()) {
                LOGGER.error("Connection check failed: {}", errorMessage.get());
                return new AirbyteConnectionStatus()
                        .withStatus(AirbyteConnectionStatus.Status.FAILED)
                        .withMessage(errorMessage.get());
            } else {
                LOGGER.info("Connection check successful.");
                return new AirbyteConnectionStatus().withStatus(AirbyteConnectionStatus.Status.SUCCEEDED);
            }
        } catch (IllegalArgumentException e) {
             LOGGER.error("Configuration validation error during check: {}", e.getMessage(), e);
             return new AirbyteConnectionStatus()
                        .withStatus(AirbyteConnectionStatus.Status.FAILED)
                        .withMessage("Configuration validation failed: " + e.getMessage());
        } catch (Exception e) {
            LOGGER.error("Unexpected error during connection check: {}", e.getMessage(), e);
            return new AirbyteConnectionStatus()
                    .withStatus(AirbyteConnectionStatus.Status.FAILED)
                    .withMessage("An unexpected error occurred during check: " + e.getMessage());
        }
    }

    @Override
    public AirbyteMessageConsumer getConsumer(
            JsonNode config,
            ConfiguredAirbyteCatalog catalog,
            Consumer<AirbyteMessage> outputRecordCollector) throws Exception {

        RagieConfig ragieConfig = RagieConfig.get(config);
        RagieClient client = new RagieClient(ragieConfig);

        return new RagieMessageConsumer(client, ragieConfig, catalog, outputRecordCollector);
    }

    // Main method for running the connector directly
    public static void main(String[] args) throws Exception {
        final Destination destination = new RagieDestination();
        LOGGER.info("Starting destination: {}", RagieDestination.class);
        new IntegrationRunner(destination).run(args);
        LOGGER.info("Completed destination: {}", RagieDestination.class);
    }
}