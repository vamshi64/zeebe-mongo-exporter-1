package com.optum.optima.zeebe.exporter;

import com.google.gson.Gson;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import com.mongodb.client.model.UpdateOneModel;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.value.*;
import io.camunda.zeebe.protocol.record.value.deployment.DecisionRecordValue;
import io.camunda.zeebe.protocol.record.value.deployment.DeploymentResource;
import io.camunda.zeebe.protocol.record.value.deployment.ProcessMetadataValue;
import org.bson.Document;
import org.slf4j.Logger;
import io.camunda.zeebe.protocol.record.ValueType;

import java.util.*;
import java.util.stream.Collectors;


import com.mongodb.client.MongoClients;

class Tuple<X, Y> {
    public final X x;
    public final Y y;
    public Tuple(X x, Y y) {
        this.x = x;
        this.y = y;
    }
}

public class ZeebeMongoClient {
    private final Logger log;
    //    private final DateTimeFormatter formatter;
    private final List<Tuple<String, UpdateOneModel<Document>>> bulkOperations;
    private final MongoExporterConfiguration configuration;
    private final MongoClient client;
    public static final String COL_DELIMITER = "-";

    public ZeebeMongoClient(
            final MongoExporterConfiguration configuration, final Logger log) {
        this(configuration, log, new ArrayList<>());
    }

    ZeebeMongoClient(
            final MongoExporterConfiguration configuration,
            final Logger log,
            final List<Tuple<String,UpdateOneModel<Document>>> bulkOperations) {
        this.configuration = configuration;
        this.log = log;
        this.client = createClient();
        this.bulkOperations = bulkOperations;
//        this.formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
    }

    public MongoClient createClient() {
        return MongoClients.create(configuration.url);
    }

    public void close() {
        client.close();
    }

    public void insert(Record<?> record) {
        bulk(newReplaceCommand(record));
    }

    public void bulk(final List<Tuple<String, UpdateOneModel<Document>>> bulkOperation) {
        // TODO: generalise, cache, etc
        if (bulkOperation == null) {
            return;
        }

        bulkOperations.addAll(bulkOperation);
    }

    /**
     * @throws MongoExporterException if not all items of the bulk were flushed successfully
     */
    public void flush() {
        if (bulkOperations.isEmpty()) {
            return;
        }

//        final int bulkSize = bulkRequest.size();
//        metrics.recordBulkSize(bulkSize);
//        final var bulkMemorySize = getBulkMemorySize();
//        metrics.recordBulkMemorySize(bulkMemorySize);

        try {
            exportBulk();
        } catch (final MongoExporterException e) {
            throw new MongoExporterException("Failed to flush bulk", e);
        }
    }
    /**
     * @throws MongoExporterException if not all items of the bulk were flushed successfully
     */
    private void exportBulk() {
        MongoDatabase db = client.getDatabase(configuration.dbName);
//        var bulkResponse = new BulkResponse();

        // Split the operations to collections
        var ops = new HashMap<String, List<UpdateOneModel<Document>>>();
        for (var op : bulkOperations) {
            if (!ops.containsKey(op.x)) {
                ops.put(op.x, new ArrayList<>());
                ops.get(op.x).add(op.y);
            }
            else {
                var opList = ops.get(op.x);
                if (!opList.get(opList.size() - 1).equals(op.y)) {
                    opList.add(op.y);
                }
            }
        }

        var success = true;
        Exception ex = null;
        String exCollectionName = null;

        for (var collectionName : ops.keySet()) {
            final var operationsPerCollection = ops.get(collectionName);
            if (!operationsPerCollection.isEmpty()) {
                try {
                    var collection = db.getCollection(collectionName);
                    var bulkWriteResult = collection.bulkWrite(operationsPerCollection);
                    operationsPerCollection.clear();
                    log.info("Flushed to collection {}, {} inserted, {} updated", collectionName, bulkWriteResult.getUpserts().size(), bulkWriteResult.getModifiedCount());
                }
                catch (Exception e) {
                    log.warn(
                            "Failed to flush {} item(s) of bulk request [collection: {}, reason: {}]",
                            operationsPerCollection.size(),
                            collectionName,
                            e.getMessage());
                    success = false;
                    ex = e;
                    exCollectionName = collectionName;
//                    throw new MongoExporterException("Failed to bulk write to collection: " + collectionName, e);
                }
            }
        }

        bulkOperations.clear();
        for (var collectionName : ops.keySet()) {
            final var operationsPerCollection = ops.get(collectionName);
            if (!operationsPerCollection.isEmpty()) {
                List<Tuple<String, UpdateOneModel<Document>>> converted = operationsPerCollection.stream()
                        .map(y -> new Tuple<>(collectionName, y))
                        .collect(Collectors.toCollection(LinkedList::new));

                bulkOperations.addAll(converted);
            }
        }

        if (!success &&  exCollectionName != null) {
            throw new MongoExporterException("Failed to bulk write to collection: " + exCollectionName, ex);
        }
//        return new BulkResponse();
    }


    public boolean shouldFlush() {
        return bulkOperations.size() >= configuration.bulk.size
                || getBulkMemorySize() >= configuration.bulk.memoryLimit;
    }

    // TODO : this never triggers the flush
    private int getBulkMemorySize() {
        return 0;
    }

    public boolean createCollection(ValueType valueType) {
        return true;
    }

    private String getCollectionName(final Record<?> record) {
        return getCollectionName(record.getValueType().name().toLowerCase());
    }

    private String getCollectionName(final String baseName) {
        return configuration.col.prefix + COL_DELIMITER + baseName;
    }

    private List<Tuple<String, UpdateOneModel<Document>>> newReplaceCommand(final Record<?> record) {
        final var valueType = record.getValueType();

        switch (valueType) {
            case JOB: return handleJobEvent(record);
            case DEPLOYMENT: return handleDeploymentEvent(record);
            case PROCESS_INSTANCE: return handleWorkflowInstanceEvent(record);
            case INCIDENT: return handleIncidentEvent(record);
            case MESSAGE: return handleMessageEvent(record);
            case MESSAGE_SUBSCRIPTION: return handleMessageSubscriptionEvent(record);
            case PROCESS_MESSAGE_SUBSCRIPTION: return handleWorkflowInstanceSubscriptionEvent(record);
            case TIMER: return handleTimerEvent(record);
            case MESSAGE_START_EVENT_SUBSCRIPTION: return handleMessageSubscriptionStartEvent(record);
            case VARIABLE: return handleVariableEvent(record);
            case JOB_BATCH: return jobBatchReplaceCommand(record);
            case PROCESS_INSTANCE_CREATION: return handleProcessInstanceCreationEvent(record);
            case ERROR: return handleErrorEvent(record);
            case VARIABLE_DOCUMENT: return handleVariableDocumentEvent(record);
            case PROCESS_INSTANCE_RESULT: return handleProcessInstanceResultEvent(record);
            case DECISION_EVALUATION: return handleDecisionEvaluationEvent(record);

            default: return null;
        }
    }

    private List<Tuple<String, UpdateOneModel<Document>>> handleDecisionEvaluationEvent(Record<?> record) {
        var result = new ArrayList<Tuple<String, UpdateOneModel<Document>>>();
        var castRecord = (DecisionEvaluationRecordValue) record.getValue();
        var document = new Document("_id", castRecord.getDecisionKey())
                .append("decisionId", castRecord.getDecisionId())
                .append("decisionName", castRecord.getDecisionName())
                .append("decisionKey", castRecord.getDecisionKey())
                .append("decisionRequirementsId", castRecord.getDecisionRequirementsId())
                .append("decisionRequirementsKey", castRecord.getDecisionRequirementsKey())
                .append("tenantId", castRecord.getTenantId())
                .append("bpmnProcessId", castRecord.getBpmnProcessId())
                .append("decisionOutput", castRecord.getDecisionOutput())
                .append("decisionVersion", castRecord.getDecisionVersion())
                .append("elementId", castRecord.getElementId())
                .append("elementInstanceKey", castRecord.getElementInstanceKey())
                .append("evaluatedDecisions", castRecord.getEvaluatedDecisions())
                .append("evaluationFailureMessage", castRecord.getEvaluationFailureMessage())
                .append("failedDecisionId", castRecord.getFailedDecisionId())
                .append("variables", castRecord.getVariables())
                .append("processInstanceKey", castRecord.getProcessInstanceKey());

        result.add(new Tuple<>( getCollectionName("decision-evaluation"), new UpdateOneModel<>(
                new Document("_id", castRecord.getProcessInstanceKey()),
                new Document("$set", document),
                new UpdateOptions().upsert(true))));
        return result;

    }

    private List<Tuple<String, UpdateOneModel<Document>>> handleProcessInstanceResultEvent(Record<?> record) {
        if(record.getIntent().name().equals("CREATED")) {
            var result = new ArrayList<Tuple<String, UpdateOneModel<Document>>>();
            var castRecord = (ProcessInstanceResultRecordValue) record.getValue();
            var document = new Document()
                    .append("bpmnProcessId", castRecord.getBpmnProcessId())
                    .append("version", castRecord.getVersion())
                    .append("processDefinitionKey", castRecord.getProcessDefinitionKey())
                    .append("variables", castRecord.getVariables())
                    .append("processInstanceKey", castRecord.getProcessInstanceKey())
                    .append("tenantId", castRecord.getTenantId());
            result.add(new Tuple<>( getCollectionName("process-instance-result"), new UpdateOneModel<>(
                    new Document("_id", castRecord.getProcessInstanceKey()),
                    new Document("$set", document),
                    new UpdateOptions().upsert(true))));
            return result;
        }
        return null;
    }

    private List<Tuple<String, UpdateOneModel<Document>>> handleVariableDocumentEvent(Record<?> record) {
        var result = new ArrayList<Tuple<String, UpdateOneModel<Document>>>();
        var castRecord = (VariableDocumentRecordValue) record.getValue();
        var document = new Document()
                .append("scopeKey", castRecord.getScopeKey())
                .append("variables", castRecord.getVariables())
                .append("tenantId", castRecord.getTenantId())
                .append("updateSemantics", castRecord.getUpdateSemantics());
        result.add(new Tuple<>( getCollectionName("variable-document"), new UpdateOneModel<>(
                new Document("_id", record.getPartitionId()+"-"+record.getPosition()),
                new Document("$set", document),
                new UpdateOptions().upsert(true))));
        return result;
    }

    private List<Tuple<String, UpdateOneModel<Document>>> handleErrorEvent(Record<?> record) {
        var result = new ArrayList<Tuple<String, UpdateOneModel<Document>>>();
        var castRecord = (ErrorRecordValue) record.getValue();
        var document = new Document()
                .append("errorEventPosition", castRecord.getErrorEventPosition())
                .append("processInstanceKey", castRecord.getProcessInstanceKey())
                .append("stacktrace", castRecord.getStacktrace())
                .append("exceptionMessage", castRecord.getExceptionMessage());
        result.add(new Tuple<>( getCollectionName("error"), new UpdateOneModel<>(
                new Document("_id", record.getPartitionId()+"-"+record.getPosition()),
                new Document("$set", document),
                new UpdateOptions().upsert(true))));
        return result;
    }

    private List<Tuple<String, UpdateOneModel<Document>>> jobBatchReplaceCommand(Record<?> record) {
        var result = new ArrayList<Tuple<String, UpdateOneModel<Document>>>();
        var castRecord = (JobBatchRecordValue) record.getValue();
        var document = new Document()
                .append("jobs", castRecord.getJobs())
                .append("type", castRecord.getType())
                .append("jobKeys", castRecord.getJobKeys())
                .append("worker", castRecord.getWorker())
                .append("timeout", castRecord.getTimeout())
                .append("maxJobsToActivate", castRecord.getMaxJobsToActivate())
                .append("tenantIds", castRecord.getTenantIds());
        result.add(new Tuple<>( getCollectionName("job-batch"), new UpdateOneModel<>(
                new Document("_id", record.getPartitionId()+"-"+record.getPosition()),
                new Document("$set", document),
                new UpdateOptions().upsert(true))));
        return result;
    }

    private List<Tuple<String, UpdateOneModel<Document>>> handleProcessInstanceCreationEvent(final Record<?> record) {
        if(record.getIntent().name().equals("CREATED")) {
            var result = new ArrayList<Tuple<String, UpdateOneModel<Document>>>();
            var castRecord = (ProcessInstanceCreationRecordValue) record.getValue();
            var document = new Document()
                    .append("bpmnProcessId", castRecord.getBpmnProcessId())
                    .append("version", castRecord.getVersion())
                    .append("processDefinitionKey", castRecord.getProcessDefinitionKey())
                    .append("processDefinitionKey", castRecord.getProcessDefinitionKey())
                    .append("variables", castRecord.getVariables())
                    .append("startInstructions", castRecord.getStartInstructions())
                    .append("tenantId", castRecord.getTenantId());
            result.add(new Tuple<>( getCollectionName("process-instance-creation"), new UpdateOneModel<>(
                    new Document("_id", castRecord.getProcessInstanceKey()),
                    new Document("$set", document),
                    new UpdateOptions().upsert(true))));
                return result;
        }
        return null;

    }

    private Object parseJsonValue(final String value) {
        var json = "{ value : " + value + "}";

        Map<String,Object> result = new Gson().fromJson(json, Map.class);

        return  result.get("value");
    }

    private List<Tuple<String, UpdateOneModel<Document>>> handleWorkflowInstanceEvent(final Record<?> record) {
        var result = new ArrayList<Tuple<String, UpdateOneModel<Document>>>();
        var timestamp = new Date(record.getTimestamp());

//        var castRecord = (ProcessInstanceRecordValue) record.getValue();
//        if (record.getKey() == castRecord.getProcessInstanceKey()) {
//            result.add(workflowInstanceReplaceCommand(record, timestamp));
//        }
        result.add(elementInstanceReplaceCommand(record, timestamp));
        result.add(elementInstanceStateTransitionReplaceCommand(record, timestamp));

        return  result;
    }

    private Tuple<String, UpdateOneModel<Document>> workflowInstanceReplaceCommand(final Record<?> record, Date timestamp) {
        var castRecord = (ProcessInstanceRecordValue) record.getValue();
        var document = new Document()
                .append("bpmnProcessId", castRecord.getBpmnProcessId())
                .append("version", castRecord.getVersion())
                .append("processDefinitionKey", castRecord.getProcessDefinitionKey());


        if (castRecord.getParentProcessInstanceKey() > 0) {
            document.append("parentWorkflowInstanceKey", castRecord.getParentProcessInstanceKey());
        }

        if (castRecord.getParentElementInstanceKey() > 0) {
            document.append("parentElementInstanceKey", castRecord.getParentElementInstanceKey());
        }

        System.out.println("Intent name: " + record.getIntent().name() + " Key: " + record.getKey() + " Workflow instance Key: " + castRecord.getProcessInstanceKey());
        switch (record.getIntent().name()) {
            case "ELEMENT_ACTIVATED":
                document.append("state", "active").append("startTime", timestamp);
                break;
            case "ELEMENT_COMPLETED":
                document.append("state", "completed").append("endedTime", timestamp);
                break;
            case "ELEMENT_TERMINATED":
                document.append("state", "canceled").append("endedTime", timestamp);
                break;
        }

        return new Tuple<>( getCollectionName("flow-instance"), new UpdateOneModel<>(
                new Document("_id", castRecord.getProcessInstanceKey()),
                new Document("$set", document),
                new UpdateOptions().upsert(true)
        ));
    }

    private Tuple<String, UpdateOneModel<Document>> elementInstanceReplaceCommand(final Record<?> record, Date timestamp) {
        var castRecord = (ProcessInstanceRecordValue) record.getValue();

        var document = new Document()
                .append("bpmnElementType", castRecord.getBpmnElementType().name())
                .append("elementId", castRecord.getElementId())
                .append("state", getElementInstanceState(record))
                .append("processInstanceKey", castRecord.getProcessInstanceKey())
                .append("processDefinitionKey", castRecord.getProcessDefinitionKey());

        switch (record.getIntent().name()) {
            case "ELEMENT_ACTIVATING":
                document.append("startTime", timestamp);
                break;
            case "ELEMENT_COMPLETED":
            case "ELEMENT_TERMINATED":
                document.append("endedTime", timestamp);
                break;
            case "SEQUENCE_FLOW_TAKEN":
                document.append("startTime", timestamp).append("endedTime", timestamp);
                break;
        }

        return new Tuple<>(getCollectionName("process-instance") , new UpdateOneModel<>(
                new Document("_id", record.getKey()),
                new Document("$set", document),
                new UpdateOptions().upsert(true)
        ));

    }

    private String getElementInstanceState(Record<?> record) {
        switch (record.getIntent().name()) {
            case "ELEMENT_ACTIVATING":
                return "ACTIVATING";
            case "ELEMENT_ACTIVATED":
                return "ACTIVATED";
            case "ELEMENT_COMPLETING":
                return "COMPLETING";
            case "ELEMENT_COMPLETED":
                return "COMPLETED";
            case "ELEMENT_TERMINATING":
                return "TERMINATING";
            case "ELEMENT_TERMINATED":
                return "TERMINATED";
            case "EVENT_OCCURRED":
                return "EVENT_OCCURRED";
            case "SEQUENCE_FLOW_TAKEN":
                return "FLOW_TAKEN";
            default:
                return "";
        }
    }

    private Tuple<String, UpdateOneModel<Document>> elementInstanceStateTransitionReplaceCommand(final Record<?> record, Date timestamp) {
        var castRecord = (ProcessInstanceRecordValue) record.getValue();

        var document = new Document()
                .append("elementInstanceKey", record.getKey())
                .append("state", getElementInstanceState(record))
                .append("timestamp", timestamp);


        return new Tuple<>(getCollectionName("element-instance-state-transition") , new UpdateOneModel<>(
                new Document("_id", record.getPosition()),
                new Document("$set", document),
                new UpdateOptions().upsert(true)
        ));

    }

    private List<Tuple<String, UpdateOneModel<Document>>> handleJobEvent(final Record<?> record) {
        var castRecord = (JobRecordValue) record.getValue();

        var document =  new Document()
                .append("jobType", castRecord.getType())
                .append("processInstanceKey", castRecord.getProcessInstanceKey())
                .append("elementInstanceKey", castRecord.getElementInstanceKey())
                .append("worker", castRecord.getWorker())
                .append("retries", castRecord.getRetries())
                .append("timestamp", new Date(record.getTimestamp()));


        switch (record.getIntent().name()) {
            case "ACTIVATED": document.append("state", "ACTIVATED"); break;
            case "FAILED": document.append("state", "FAILED"); break;
            case "COMPLETED": document.append("state", "COMPLETED"); break;
            case "CANCELED": document.append("state", "CANCELED"); break;
            case "ERROR_THROWN": document.append("state", "ERROR_THROWN"); break;
            case "CREATED":
            case "TIMED_OUT":
            case "RETRIES_UPDATED":
            default:
                document.append("state", "ACTIVATABLE"); break;
        }

        var result = new ArrayList<Tuple<String, UpdateOneModel<Document>>>();
        result.add(new Tuple<>(getCollectionName("job"), new UpdateOneModel<>(
                new Document("_id", record.getKey()),
                new Document("$set", document),
                new UpdateOptions().upsert(true)
        )));

        return result;
    }

    private List<Tuple<String, UpdateOneModel<Document>>> handleDeploymentEvent(final Record<?> record) {
            log.info("Received Deployment Event with Intent: {}", record.getIntent().name());

            var castRecord = (DeploymentRecordValue) record.getValue();

            var result = new ArrayList<Tuple<String, UpdateOneModel<Document>>>();
            var timestamp = new Date(record.getTimestamp());
            var resources = castRecord.getResources();

            log.info("Deployment Event with Intent: {}, DeploymentRecordValue: {}, PartitionId:{}, Position:{}, SourceRecordPosition:{}",
                    record.getIntent().name(), castRecord, record.getPartitionId(), record.getPosition(), record.getSourceRecordPosition());

            if(castRecord.getProcessesMetadata().size()>0) {
                for (var workflow : castRecord.getProcessesMetadata()) {
                    log.info("Creating record to insert to Process Collection with ResourceName: {}", workflow.getResourceName());
                    if(!resources.isEmpty()) {
                        var resource = resources.stream().filter(r -> r.getResourceName().equals(workflow.getResourceName())).iterator().next();
                        result.add(new Tuple<>(getCollectionName("process"), workflowReplaceCommand(workflow, resource, timestamp)));
                    }
                }
            }else{
                log.info("Creating record to insert to Process Collection with No ProcessesMetadata");

            }

            for (var decision : castRecord.getDecisionsMetadata()) {
                log.info("Creating record to insert to Decision Collection with DecisionName: {}", decision.getDecisionName());
                result.add(new Tuple<>(getCollectionName("decision"), decisionReplaceCommand(decision, timestamp)));
            }

            return result;

        //log.info("Skipping Deployment Event with Intent: {}", record.getIntent().name());
        //return null;
    }

    private UpdateOneModel<Document> workflowReplaceCommand(ProcessMetadataValue record, DeploymentResource resource, Date timestamp) {
        var document = new Document("_id", record.getProcessDefinitionKey())
                .append("bpmnProcessId", record.getBpmnProcessId())
                .append("resourceName", record.getResourceName())
                .append("version", record.getVersion())
                .append("timestamp", timestamp)
                .append("resource", resource.getResource());

        return new UpdateOneModel<>(
                new Document("_id", record.getProcessDefinitionKey()),
                new Document("$set", document),
                new UpdateOptions().upsert(true)
        );
    }

    private UpdateOneModel<Document> decisionReplaceCommand(DecisionRecordValue record, Date timestamp) {
        var document = new Document("_id", record.getDecisionKey())
                .append("decisionId", record.getDecisionId())
                .append("decisionName", record.getDecisionName())
                .append("decisionKey", record.getDecisionKey())
                .append("decisionRequirementsId", record.getDecisionRequirementsId())
                .append("decisionRequirementsKey", record.getDecisionRequirementsKey())
                .append("tenantId", record.getTenantId())
                .append("version", record.getVersion())
                .append("timestamp", timestamp);

        return new UpdateOneModel<>(
                new Document("_id", record.getDecisionKey()),
                new Document("$set", document),
                new UpdateOptions().upsert(true)
        );
    }

    private List<Tuple<String, UpdateOneModel<Document>>> handleVariableEvent(final Record<?> record) {
        var result = new ArrayList<Tuple<String, UpdateOneModel<Document>>>();
        result.add(variableReplaceCommand(record));
        result.add(variableUpdateReplaceCommand(record));

        return result;
    }

    private Tuple<String, UpdateOneModel<Document>> variableReplaceCommand(final Record<?> record) {
        var castRecord = (VariableRecordValue) record.getValue();

        var document =  new Document()
                .append("name", castRecord.getName())
                .append("processInstanceKey", castRecord.getProcessInstanceKey())
                .append("scopeKey", castRecord.getScopeKey())
                .append("value", parseJsonValue(castRecord.getValue()))
                .append("timestamp", new Date(record.getTimestamp()));

        return new Tuple<>(getCollectionName("variable"), new UpdateOneModel<>(
                new Document("_id", record.getKey()),
                new Document("$set", document),
                new UpdateOptions().upsert(true)
        ));
    }

    private Tuple<String, UpdateOneModel<Document>> variableUpdateReplaceCommand(final Record<?> record) {
        var castRecord = (VariableRecordValue) record.getValue();

        var document =  new Document()
                .append("variableKey", record.getKey())
                .append("name", castRecord.getName())
                .append("value", parseJsonValue(castRecord.getValue()))
                .append("processInstanceKey", castRecord.getProcessInstanceKey())
                .append("scopeKey", castRecord.getScopeKey())
                .append("timestamp", new Date(record.getTimestamp()));


        return new Tuple<>(getCollectionName("variable-update"), new UpdateOneModel<>(
                new Document("_id", record.getPosition()),
                new Document("$set", document),
                new UpdateOptions().upsert(true)
        ));
    }

    private List<Tuple<String, UpdateOneModel<Document>>> handleIncidentEvent(final Record<?> record) {
        var castRecord = (IncidentRecordValue) record.getValue();

        var document =  new Document()
                .append("errorType", castRecord.getErrorType().name())
                .append("errorMessage", castRecord.getErrorMessage())
                .append("processInstanceKey", castRecord.getProcessInstanceKey())
                .append("elementInstanceKey", castRecord.getElementInstanceKey());

        if (castRecord.getJobKey() > 0) {
            document.append("jobKey", castRecord.getJobKey());
        }

        var timestamp =  new Date(record.getTimestamp());

        switch (record.getIntent().name()) {
            case "CREATED":
                document.append("state", "CREATED").append("creationTime", timestamp);
                break;
            case "RESOLVED":
                document.append("state", "RESOLVED").append("resolveTime", timestamp);
                break;
        }

        var result = new ArrayList<Tuple<String, UpdateOneModel<Document>>>();
        result.add(new Tuple<>(getCollectionName("incident"), new UpdateOneModel<>(
                new Document("_id", record.getKey()),
                new Document("$set", document),
                new UpdateOptions().upsert(true)
        )));

        return result;
    }

    private List<Tuple<String, UpdateOneModel<Document>>> handleTimerEvent(final Record<?> record) {
        var castRecord = (TimerRecordValue) record.getValue();

        var timestamp =  new Date(record.getTimestamp());

        var document =  new Document()
                .append("dueDate", new Date(castRecord.getDueDate()))
                .append("timestamp", timestamp)
                .append("state", record.getIntent().name())
                .append("repetitions", castRecord.getRepetitions());

        // These only need to be set once, on insert
        var setOnInsert = new Document("creationTime", timestamp);
        if (castRecord.getProcessDefinitionKey() > 0) {
            setOnInsert.append("processDefinitionKey", castRecord.getProcessDefinitionKey());
        }

        if (castRecord.getProcessInstanceKey() > 0) {
            setOnInsert.append("processInstanceKey", castRecord.getProcessInstanceKey());
        }

        if (castRecord.getElementInstanceKey() > 0) {
            setOnInsert.append("elementInstanceKey", castRecord.getElementInstanceKey());
        }

        var result = new ArrayList<Tuple<String, UpdateOneModel<Document>>>();
        result.add(new Tuple<>(getCollectionName("timer"), new UpdateOneModel<>(
                new Document("_id", record.getKey()),
                new Document("$set", document).append("$setOnInsert", setOnInsert),
                new UpdateOptions().upsert(true)
        )));

        return result;
    }

    private List<Tuple<String, UpdateOneModel<Document>>> handleMessageEvent(final Record<?> record) {
        var castRecord = (MessageRecordValue) record.getValue();

        var document =  new Document()
                .append("name", castRecord.getName())
                .append("correlationKey", castRecord.getCorrelationKey())
                .append("messageId", castRecord.getMessageId())
                .append("timestamp", new Date(record.getTimestamp()))
                .append("state", record.getIntent().name())
                .append("timeToLive", castRecord.getTimeToLive());

        var result = new ArrayList<Tuple<String, UpdateOneModel<Document>>>();
        result.add(new Tuple<>(getCollectionName("message"), new UpdateOneModel<>(
                new Document("_id", record.getKey()),
                new Document("$set", document),
                new UpdateOptions().upsert(true)
        )));

        return result;
    }

    private List<Tuple<String, UpdateOneModel<Document>>> handleMessageSubscriptionEvent(final Record<?> record) {
        System.out.println("handleMessageSubscriptionEvent " + getCollectionName(record) + " " + record.getPosition());

        var castRecord = (MessageSubscriptionRecordValue) record.getValue();

        var document =  new Document()
                .append("messageName", castRecord.getMessageName())
                .append("correlationKey", castRecord.getCorrelationKey())
                .append("processInstanceKey", castRecord.getProcessInstanceKey())
                .append("elementInstanceKey", castRecord.getElementInstanceKey())
                .append("timestamp", new Date(record.getTimestamp()))
                .append("state", record.getIntent().name());

        var result = new ArrayList<Tuple<String, UpdateOneModel<Document>>>();
        result.add(new Tuple<>(getCollectionName(record), new UpdateOneModel<>(
                new Document("_id", record.getPosition()),
                new Document("$set", document),
                new UpdateOptions().upsert(true)
        )));

        return result;
    }

    private List<Tuple<String, UpdateOneModel<Document>>> handleMessageSubscriptionStartEvent(final Record<?> record) {
        var castRecord = (MessageStartEventSubscriptionRecordValue) record.getValue();

        // TODO: _id possibly isn't unique, using (messageName, WorkflowKey) as identifier for upsert
        var document =  new Document("_id", record.getPosition())
                .append("messageName", castRecord.getMessageName())
                .append("processDefinitionKey", castRecord.getProcessDefinitionKey())
                .append("elementId", castRecord.getStartEventId())
                .append("timestamp", new Date(record.getTimestamp()))
                .append("state", record.getIntent().name());

        var result = new ArrayList<Tuple<String, UpdateOneModel<Document>>>();
        result.add(new Tuple<>(getCollectionName("message-subscription"), new UpdateOneModel<>(
                new Document("messageName", castRecord.getMessageName()).append("processDefinitionKey", castRecord.getProcessDefinitionKey()),
                new Document("$set", document),
                new UpdateOptions().upsert(true)
        )));

        return result;
    }

    private List<Tuple<String, UpdateOneModel<Document>>> handleWorkflowInstanceSubscriptionEvent(final Record<?> record) {
        if (!record.getIntent().name().equals("CORRELATED")) {
            return null;
        }

        var castRecord = (ProcessMessageSubscriptionRecordValue) record.getValue();

        var document =  new Document("_id", record.getPosition())
                .append("messageName", castRecord.getMessageName())
                .append("messageKey", castRecord.getMessageKey())
                .append("elementInstanceKey", castRecord.getElementInstanceKey())
                .append("timestamp", new Date(record.getTimestamp()));

        var result = new ArrayList<Tuple<String, UpdateOneModel<Document>>>();
        result.add(new Tuple<>(getCollectionName("message-correlation"), new UpdateOneModel<>(
                new Document("_id", record.getPosition()),
                new Document("$set", document),
                new UpdateOptions().upsert(true)
        )));

        return result;
    }


}