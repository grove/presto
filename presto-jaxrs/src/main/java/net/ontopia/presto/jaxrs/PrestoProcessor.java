package net.ontopia.presto.jaxrs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Topic;
import net.ontopia.presto.jaxb.TopicType;
import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.jaxrs.process.AbstractProcessor;
import net.ontopia.presto.jaxrs.process.FieldDataProcessor;
import net.ontopia.presto.jaxrs.process.TopicProcessor;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView;
import net.ontopia.presto.spi.utils.Utils;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrestoProcessor {

    private static Logger log = LoggerFactory.getLogger(PrestoProcessor.class);

    private final ObjectMapper mapper = new ObjectMapper();
    
    private Presto presto;

    public static class Status {
        private boolean isValid = true;

        public boolean isValid() {
            return isValid;
        }

        public void setValid(boolean isValid) {
            this.isValid = isValid;
        }
    }

    public enum Type {
        PRE_PROCESS, POST_PROCESS;
    }
    
    PrestoProcessor(Presto presto) {
        this.presto = presto;
    }
    
    private PrestoSchemaProvider getSchemaProvider() {
        return presto.getSchemaProvider();
    }
    
    private PrestoDataProvider getDataProvider() {
        return presto.getDataProvider();
    }
    
    public Topic preProcessTopic(Topic topicData, PrestoTopic topic, PrestoType type, PrestoView view, Status status) {
        return processTopic(topicData, topic, type, view, Type.PRE_PROCESS, status);
    }

    public Topic postProcessTopic(Topic topicData, PrestoTopic topic, PrestoType type, PrestoView view, Status status) {
        return processTopic(topicData, topic, type, view, Type.POST_PROCESS, status);
    }
    
    private Topic processTopic(Topic topicData, PrestoTopic topic, PrestoType type, PrestoView view, Type processType, Status status) {

        // TODO: add errors on topic data?
//        // reset errors when pre-process
//        if (processType == Type.PRE_PROCESS) {
//            topicData.setErrors(null);
//        }

        // process the topic
        ObjectNode schemaExtra = presto.getSchemaExtraNode(presto.getSchemaProvider());
        topicData = processTopicExtra(topicData, topic, type, view, schemaExtra, processType, status);
        
        ObjectNode topicExtra = presto.getTypeExtraNode(type);
        topicData = processTopicExtra(topicData, topic, type, view, topicExtra, processType, status);
        
        ObjectNode viewExtra = presto.getViewExtraNode(view);
        topicData = processTopicExtra(topicData, topic, type, view, viewExtra, processType, status);

        // process the field data
        Collection<FieldData> fields = topicData.getFields();
        Collection<FieldData> newFields = new ArrayList<FieldData>(fields.size());

        for (FieldData fieldData : fields) {

            String fieldId = fieldData.getId();
            PrestoFieldUsage field = type.getFieldById(fieldId, view);

            // process field            
            FieldData newFieldData = processFieldData(fieldData, topic, field, processType, status);
            newFields.add(newFieldData);
        }            
        topicData.setFields(newFields);

        return topicData;
    }
    
//    public FieldData preProcessFieldData(FieldData fieldData, PrestoTopic topic, PrestoFieldUsage field, Status status) {
//        return processFieldData(fieldData, topic, field, Type.PRE_PROCESS, status);
//    }
//
//    public FieldData postProcessFieldData(FieldData fieldData, PrestoTopic topic, PrestoFieldUsage field, Status status) {
//        return processFieldData(fieldData, topic, field, Type.POST_PROCESS, status);
//    }
    
    public FieldData processFieldData(FieldData fieldData, PrestoTopic topic, PrestoFieldUsage field, Type processType, Status status) {
        // process nested data first
        PrestoSchemaProvider schemaProvider = getSchemaProvider();
        PrestoDataProvider dataProvider = getDataProvider();
        
        if (field.isEmbedded() || field.isInline()) { 

            PrestoView valueView = field.getValueView();

            Collection<Value> values = fieldData.getValues();
            if (values != null) {
                for (Value value : values) {
                    Topic embeddedTopic = presto.getEmbeddedTopic(value);
                    if (embeddedTopic != null) {
                        String topicId = embeddedTopic.getId();
        
                        PrestoTopic valueTopic = null;
                        PrestoType valueType;
                        if (topicId == null) {
                            TopicType topicType = embeddedTopic.getType();
                            valueType = schemaProvider.getTypeById(topicType.getId());
                        } else {
                            valueTopic = dataProvider.getTopicById(topicId);
                            valueType = schemaProvider.getTypeById(valueTopic.getTypeId());
                        }
                        
                        embeddedTopic = processTopic(embeddedTopic, valueTopic, valueType, valueView, processType, status);
                        value.setEmbedded(embeddedTopic);
                    }
                }
            }
        }

        // reset errors when pre-process
        if (processType == Type.PRE_PROCESS) {
            fieldData.setErrors(null);
            fieldData.setMessages(null);
        }

        // process field
        ObjectNode schemaExtra = presto.getSchemaExtraNode(presto.getSchemaProvider());
        fieldData = processFieldDataExtra(fieldData, topic, field, schemaExtra, processType, status);
        
        ObjectNode topicExtra = presto.getTypeExtraNode(field.getType());
        fieldData = processFieldDataExtra(fieldData, topic, field, topicExtra, processType, status);
        
        ObjectNode viewExtra = presto.getViewExtraNode(field.getView());
        fieldData = processFieldDataExtra(fieldData, topic, field, viewExtra, processType, status);

        ObjectNode fieldExtra = presto.getFieldExtraNode(field);
        fieldData = processFieldDataExtra(fieldData, topic, field, fieldExtra, processType, status);
        
        return fieldData;
    }

    private Topic processTopicExtra(Topic topicData, PrestoTopic topic, PrestoType type, PrestoView view, ObjectNode extraNode, Type processType, Status status) {
        if (extraNode != null) {
            JsonNode processorsNode = getTopicProcessorsNode(extraNode, processType);
            if (!processorsNode.isMissingNode()) {
                for (TopicProcessor processor : getProcessors(TopicProcessor.class, processorsNode, processType, status)) {
                    topicData = processor.processTopic(topicData, topic, type, view);
                }
            }
        }
        return topicData;
    }
    
    private FieldData processFieldDataExtra(FieldData fieldData, PrestoTopic topic, PrestoFieldUsage field, ObjectNode extraNode, Type processType, Status status) {
        if (extraNode != null) {
            JsonNode processorsNode = getFieldDataProcessorsNode(extraNode, processType);
            if (!processorsNode.isMissingNode()) {
                for (FieldDataProcessor processor : getProcessors(FieldDataProcessor.class, processorsNode, processType, status)) {
                    fieldData = processor.processFieldData(fieldData, topic, field);                
                }
            }
        }
        return fieldData;
    }
    
    private JsonNode getTopicProcessorsNode(ObjectNode extraNode, Type processType) {
        if (processType == Type.PRE_PROCESS) {
            return extraNode.path("topicPreProcessors");            
        } else {
            return extraNode.path("topicPostProcessors");
        }
    }
    
    private JsonNode getFieldDataProcessorsNode(ObjectNode extraNode, Type processType) {
        if (processType == Type.PRE_PROCESS) {
            return extraNode.path("fieldPreProcessors");            
        } else {
            return extraNode.path("fieldPostProcessors");
        }
    }

    private <T extends AbstractProcessor> Iterable<T> getProcessors(Class<T> klass, JsonNode processorsNode, Type processType, Status status) {
        if (processorsNode.isArray()) {
            List<T> result = new ArrayList<T>();
            for (JsonNode processorNode : processorsNode) {
                T processor = getProcessor(klass, processorNode, processType, status);
                if (processor != null) {
                    result.add(processor);
                }
            }
            return result;
        } else {
            T processor = getProcessor(klass, processorsNode, processType, status);
            if (processor != null) {
                return Collections.singleton(processor);
            }
        }
        return Collections.emptyList();
    }

    private <T extends AbstractProcessor> T getProcessor(Class<T> klass, JsonNode processorNode, Type processType, Status status) {
        if (processorNode.isTextual()) {
            String className = processorNode.getTextValue();
            if (className != null) {
                return getProcessorInstance(klass, className, null, processType, status);
            }
        } else if (processorNode.isObject()) {
            if (processorNode.has("class")) {
                String className = processorNode.path("class").getTextValue();
                if (className != null) {
                    return getProcessorInstance(klass, className, (ObjectNode)processorNode, processType, status);
                }
            } else if (processorNode.has("processor")) {
                String ref = processorNode.path("processor").getTextValue();
                ObjectNode extra = (ObjectNode)getSchemaProvider().getExtra();
                if (extra != null) {
                    JsonNode globalProcessorNode = extra.path("processors").path(ref);
                    ObjectNode mergedProcessorNode = mergeObjectNodes(globalProcessorNode, processorNode);
                    mergedProcessorNode.remove("processor"); // remove 'processor' entry to avoid recursion
                    return getProcessor(klass, mergedProcessorNode, processType, status);
                }
            } 
        }
        log.warn("Could not find processor for config: {}", processorNode);
        return null;
    }
    
    private ObjectNode mergeObjectNodes(JsonNode n1, JsonNode n2) {
        if (n1.isObject() && n2.isObject()) {
            ObjectNode result = mapper.createObjectNode();
            result.putAll((ObjectNode)n1);
            result.putAll((ObjectNode)n2);
            return result;
        } else if (n1.isObject()) {
            ObjectNode result = mapper.createObjectNode();
            result.putAll((ObjectNode)n1);
            return result;
        } else if (n2.isObject()) {
            ObjectNode result = mapper.createObjectNode();
            result.putAll((ObjectNode)n2);
            return result;
        } else {
            return null;
        }
    }

    private <T extends AbstractProcessor> T getProcessorInstance(Class<T> klass, String className, ObjectNode processorConfig, Type processType, Status status) {
        T processor = Utils.newInstanceOf(className, klass);
        if (processor != null) {
            processor.setPresto(presto);
            processor.setType(processType);
            processor.setStatus(status);
            processor.setConfig(processorConfig);
        }
        return processor;
    }

}
