package net.ontopia.presto.jaxrs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
import org.codehaus.jackson.node.ObjectNode;

public class PrestoProcessor {

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
    
    private FieldData processFieldData(FieldData fieldData, PrestoTopic topic, PrestoFieldUsage field, Type processType, Status status) {
        // process nested data first
        PrestoSchemaProvider schemaProvider = getSchemaProvider();
        PrestoDataProvider dataProvider = getDataProvider();
        
        if (field.isEmbedded() || field.isInline()) { 

            PrestoView valueView = field.getValueView();

            for (Value value : fieldData.getValues()) {
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
            for (TopicProcessor processor : getProcessors(TopicProcessor.class, processorsNode, processType, status)) {
                topicData = processor.processTopic(topicData, topic, type, view);
            }
        }
        return topicData;
    }
    
    private FieldData processFieldDataExtra(FieldData fieldData, PrestoTopic topic, PrestoFieldUsage field, ObjectNode extraNode, Type processType, Status status) {
        if (extraNode != null) {
            JsonNode processorsNode = getFieldDataProcessorsNode(extraNode, processType);
            for (FieldDataProcessor processor : getProcessors(FieldDataProcessor.class, processorsNode, processType, status)) {
                fieldData = processor.processFieldData(fieldData, topic, field);                
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
        if (processorsNode.isTextual()) {
            String className = processorsNode.getTextValue();
            T processor = getProcessorInstance(klass, className, processType, status);
            if (processor != null) {
                return Collections.singleton(processor);
            }
        } else if (processorsNode.isArray()) {
            for (JsonNode processorNode : processorsNode) {
                List<T> result = new ArrayList<T>();
                if (processorNode.isTextual()) {
                    String className = processorNode.getTextValue();
                    T processor = getProcessorInstance(klass, className, processType, status);
                    if (processor != null) {
                        result.add(processor);
                    }
                }
                return result;
            }
        }
        return Collections.emptyList();
    }

    private <T extends AbstractProcessor> T getProcessorInstance(Class<T> klass, String className, Type processType, Status status) {
        T processor = Utils.newInstanceOf(className, klass);
        if (processor != null) {
            processor.setPresto(presto);
            processor.setType(processType);
            processor.setStatus(status);
        }
        return processor;
    }

}
