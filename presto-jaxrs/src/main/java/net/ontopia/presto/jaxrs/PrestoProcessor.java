package net.ontopia.presto.jaxrs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Topic;
import net.ontopia.presto.jaxb.TopicType;
import net.ontopia.presto.jaxb.Value;
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
        ObjectNode topicExtra = presto.getTypeExtraNode(type);
        if (topicExtra != null) {
            topicData = processTopicExtra(topicData, topic, type, view, topicExtra, processType, status);
        }    
        ObjectNode viewExtra = presto.getViewExtraNode(view);
        if (viewExtra != null) {
            topicData = processTopicExtra(topicData, topic, type, view, viewExtra, processType, status);
        }    

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

    private Topic processTopicExtra(Topic topicData, PrestoTopic topic, PrestoType type, PrestoView view, ObjectNode extraNode, Type processType, Status status) {
        Map<String, Object> params = presto.getExtraParamsMap(extraNode);
        if (params != null) {
            topicData.setParams(params);
        }
        JsonNode processorNode = getProcessorNode(extraNode, processType);
        if (processorNode.isTextual()) {
            String className = processorNode.getTextValue();
            TopicProcessor processor = Utils.newInstanceOf(className, TopicProcessor.class);
            if (processor != null) {
                processor.setPresto(presto);
                processor.setType(processType);
                processor.setStatus(status);
                topicData = processor.processTopic(topicData, topic, type, view);
            }
        }
        return topicData;
    }
    
    private JsonNode getProcessorNode(ObjectNode extraNode, Type processType) {
        if (processType == Type.PRE_PROCESS) {
            return extraNode.path("preProcessor");            
        } else {
            return extraNode.path("postProcessor");
        }
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
        }

        // process field
        ObjectNode extraNode = presto.getFieldExtraNode(field);
        if (extraNode != null) {
            Map<String, Object> params = presto.getExtraParamsMap(extraNode);
            if (params != null) {
                fieldData.setParams(params);
            }
            JsonNode processorNode = getProcessorNode(extraNode, processType);
            if (processorNode.isTextual()) {
                String className = processorNode.getTextValue();
                FieldDataProcessor processor = Utils.newInstanceOf(className, FieldDataProcessor.class);
                if (processor != null) {
                    processor.setPresto(presto);
                    processor.setType(processType);
                    processor.setStatus(status);
                    fieldData = processor.processFieldData(fieldData, topic, field);
                }
            }
////            // TODO: should be replaced by a post processor
//            if (processType == Type.POST_PROCESS) {
//                JsonNode messagesNode = extraNode.path("messages");
//                if (messagesNode.isArray()) {
//                    List<FieldData.Message> messages = new ArrayList<FieldData.Message>();
//                    for (JsonNode messageNode : messagesNode) {
//                        String type = messageNode.get("type").getTextValue();
//                        String message = messageNode.get("message").getTextValue();
//                        messages.add(new FieldData.Message(type, message));
//                    }
//                    if (fieldData.getMessages() != null) {
//                        fieldData.getMessages().addAll(messages);
//                    } else {
//                        fieldData.setMessages(messages);
//                    }
//                }
//            }
        }
        return fieldData;
    }

}
