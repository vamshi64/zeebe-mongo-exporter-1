package com.optum.optima.zeebe.exporter;

import  io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.exporter.api.ExporterException;
import io.camunda.zeebe.exporter.api.context.Context;
import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.ValueType;

import java.time.Duration;

import org.slf4j.Logger;

// implements https://github.com/zeebe-io/zeebe/issues/1331
public class MongoExporter implements Exporter {
    // by default, the bulk request may not be bigger than 100MB
    private static final int RECOMMENDED_MAX_BULK_MEMORY_LIMIT = 100 * 1024 * 1024;

    private Logger log;
    private Controller controller;

    private MongoExporterConfiguration configuration;
    private ZeebeMongoClient client;

    private long lastPosition = -1;
    private boolean colsCreated;

    @Override
    public void configure(Context context) {
        log = context.getLogger();
        configuration =
                context.getConfiguration().instantiate(MongoExporterConfiguration.class);
        log.debug("Exporter configured with {}", configuration);
        validate(configuration);
        context.setFilter(new MongoRecordFilter(configuration));
    }

    @Override
    public void open(Controller controller) {
        this.controller = controller;

        log.debug("starting to create client");
        client = createClient();
        log.debug("client created");

        scheduleDelayedFlush();
        log.info("Exporter opened");
    }

    @Override
    public void close() {

        try {
            flush();
        } catch (final Exception e) {
            log.warn("Failed to flush records before closing exporter.", e);
        }

        try {
            client.close();
        } catch (final Exception e) {
            log.warn("Failed to close elasticsearch client", e);
        }

        log.info("Exporter closed");
    }

    @Override
    public void export(final Record record) {
        if (!colsCreated) {
            log.info("starting to create columns");
            createCols();
        }
       // Field[] fields = Record.class.getFields();

        //record.
        log.info("Mongo record with recordType: {}, Intent: {}, Value: {}, record: {}, ", record.getRecordType(), record.getIntent(), record.getValue(), record);
        client.insert(record);
        lastPosition = record.getPosition();

        if (client.shouldFlush()) {
            flush();
        }
    }

    private void validate(final MongoExporterConfiguration configuration) {
        if (configuration.col.prefix != null && configuration.col.prefix.contains("_")) {
            throw new ExporterException(
                    String.format(
                            "Mongo prefix must not contain underscore. Current value: %s",
                            configuration.col.prefix));
        }

        if (configuration.bulk.memoryLimit > RECOMMENDED_MAX_BULK_MEMORY_LIMIT) {
            log.warn(
                    "The bulk memory limit is set to more than {} bytes. It is recommended to set the limit between 5 to 15 MB.",
                    RECOMMENDED_MAX_BULK_MEMORY_LIMIT);
        }
    }

    protected ZeebeMongoClient createClient() {
        return new ZeebeMongoClient(configuration, log);
    }

    private void flushAndReschedule() {
        try {
            flush();
        } catch (final Exception e) {
            log.error(
                    "Unexpected exception occurred on periodically flushing bulk, will retry later.", e);
        }
        scheduleDelayedFlush();
    }

    private void scheduleDelayedFlush() {
        controller.scheduleCancellableTask(Duration.ofSeconds(configuration.bulk.delay), this::flushAndReschedule);
    }

    private void flush() {
        client.flush();
        controller.updateLastExportedRecordPosition(lastPosition);
    }

    private void createCols() {
        final MongoExporterConfiguration.ColConfiguration col = configuration.col;

        if (col.createCollections) {
            if (col.deployment) {
                createValueCol(ValueType.DEPLOYMENT);
            }
            if (col.error) {
                createValueCol(ValueType.ERROR);
            }
            if (col.incident) {
                createValueCol(ValueType.INCIDENT);
            }
            if (col.job) {
                createValueCol(ValueType.JOB);
            }
            if (col.jobBatch) {
                createValueCol(ValueType.JOB_BATCH);
            }
            if (col.message) {
                createValueCol(ValueType.MESSAGE);
            }
            if (col.messageSubscription) {
                createValueCol(ValueType.MESSAGE_SUBSCRIPTION);
            }
            if (col.variable) {
                createValueCol(ValueType.VARIABLE);
            }
            if (col.variableDocument) {
                createValueCol(ValueType.VARIABLE_DOCUMENT);
            }
            if (col.workflowInstance) {
                createValueCol(ValueType.PROCESS_INSTANCE);
            }
            if (col.workflowInstanceCreation) {
                createValueCol(ValueType.PROCESS_INSTANCE_CREATION);
            }
            if (col.workflowInstanceSubscription) {
                createValueCol(ValueType.PROCESS_MESSAGE_SUBSCRIPTION);
            }
        }

        colsCreated = true;
    }

    private void createValueCol(final ValueType valueType) {
       log.info("createcolumns initiated for :{}",valueType.name());
        if (!client.createCollection(valueType)) {
            log.warn("Put index template for value type {} was not acknowledged", valueType);
        }
    }

    private static class MongoRecordFilter implements Context.RecordFilter {

        private final MongoExporterConfiguration configuration;

        MongoRecordFilter(final MongoExporterConfiguration configuration) {
            this.configuration = configuration;
        }

        @Override
        public boolean acceptType(final RecordType recordType) {
            return configuration.shouldInsertRecordType(recordType);
        }

        @Override
        public boolean acceptValue(final ValueType valueType) {
            return configuration.shouldInsertValueType(valueType);
        }
    }
}
