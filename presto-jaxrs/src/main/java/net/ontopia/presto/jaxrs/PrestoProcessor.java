package net.ontopia.presto.jaxrs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Topic;
import net.ontopia.presto.jaxb.TopicView;
import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.jaxrs.process.AbstractProcessor;
import net.ontopia.presto.jaxrs.process.FieldDataProcessor;
import net.ontopia.presto.jaxrs.process.TopicProcessor;
import net.ontopia.presto.jaxrs.process.TopicViewProcessor;
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
    
    public Topic preProcessTopic(Topic topicData, PrestoContext context, Status status) {
        return processTopic(topicData, context, Type.PRE_PROCESS, status);
    }

    public Topic postProcessTopic(Topic topicData, PrestoContext context, Status status) {
        return processTopic(topicData, context, Type.POST_PROCESS, status);
    }
    
    private Topic processTopic(Topic topicData, PrestoContext context, Type processType, Status status) {

        // process the topic
        ObjectNode schemaExtra = presto.getSchemaExtraNode(presto.getSchemaProvider());
        topicData = processTopicExtra(topicData, context, schemaExtra, processType, status);
        
        PrestoTopic topic = context.getTopic();
        PrestoType type = context.getType();
        
        ObjectNode topicExtra = presto.getTypeExtraNode(type);
        topicData = processTopicExtra(topicData, context, topicExtra, processType, status);

        // process the topic views
        Collection<TopicView> views = topicData.getViews();
        Collection<TopicView> newViews = new ArrayList<TopicView>(views.size());

        for (TopicView topicView : views) {
            String viewId = topicView.getId();
            PrestoView specificView = type.getViewById(viewId);
            PrestoContext subcontext = PrestoContext.create(topic, type, specificView, context.isReadOnly());
            
            TopicView newView = processTopicView(topicView, subcontext, processType, status);
            if (newView != null) {
                newViews.add(newView);
            }
        }            
        topicData.setViews(newViews);

        return topicData;
    }
    
    public TopicView preProcessTopicView(TopicView topicView, PrestoContext context, Status status) {
        return processTopicView(topicView, context, Type.PRE_PROCESS, status);
    }

    public TopicView postProcessTopicView(TopicView topicView, PrestoContext context, Status status) {
        return processTopicView(topicView, context, Type.POST_PROCESS, status);
    }
    
    private TopicView processTopicView(TopicView topicView, PrestoContext context, Type processType, Status status) {

        // process the topic
        ObjectNode schemaExtra = presto.getSchemaExtraNode(presto.getSchemaProvider());
        topicView = processTopicViewExtra(topicView, context, schemaExtra, processType, status);
        
        ObjectNode topicExtra = presto.getTypeExtraNode(context.getType());
        topicView = processTopicViewExtra(topicView, context, topicExtra, processType, status);
        
        ObjectNode viewExtra = presto.getViewExtraNode(context.getView());
        topicView = processTopicViewExtra(topicView, context, viewExtra, processType, status);

        // process the field data
        Collection<FieldData> fields = topicView.getFields();
        if (fields != null) {
            Collection<FieldData> newFields = new ArrayList<FieldData>(fields.size());
            
            for (FieldData fieldData : fields) {
    
                String fieldId = fieldData.getId();
                PrestoFieldUsage field = context.getFieldById(fieldId);
    
                // process field            
                FieldData newField = processFieldData(fieldData, context, field, processType, status);
                if (newField != null) {
                    newFields.add(newField);
                }
            
            }
            topicView.setFields(newFields);
        }

        return topicView;
    }
    
//    public FieldData preProcessFieldData(FieldData fieldData, PrestoContext context, PrestoFieldUsage field, Status status) {
//        return processFieldData(fieldData, context, field, Type.PRE_PROCESS, status);
//    }

    public FieldData postProcessFieldData(FieldData fieldData, PrestoContext context, PrestoFieldUsage field, Status status) {
        return processFieldData(fieldData, context, field, Type.POST_PROCESS, status);
    }
    
    public FieldData processFieldData(FieldData fieldData, PrestoContext context, PrestoFieldUsage field, Type processType, Status status) {
        // process nested data first
        PrestoSchemaProvider schemaProvider = getSchemaProvider();
        PrestoDataProvider dataProvider = getDataProvider();
        
        if (field.isEmbedded()) { 

            Collection<Value> values = fieldData.getValues();
            if (values != null) {
                for (Value value : values) {
                    TopicView embeddedTopic = presto.getEmbeddedTopic(value);
                    if (embeddedTopic != null) {
                        String topicId = embeddedTopic.getTopicId();
        
                        PrestoTopic valueTopic = null;
                        PrestoType valueType;
                        if (topicId == null) {
                            String topicTypeId = embeddedTopic.getTopicTypeId();
                            valueType = schemaProvider.getTypeById(topicTypeId);
                        } else {
                            if (field.isInline()) {
                                valueTopic = presto.buildInlineTopic(context, embeddedTopic);
//                                valueTopic = presto.findInlineTopicById(context.getTopic(), field, topicId);
                            } else {
                                valueTopic = dataProvider.getTopicById(topicId);
                            }
                            valueType = schemaProvider.getTypeById(valueTopic.getTypeId());
                        }

                        PrestoView valueView = field.getValueView(valueType);

                        PrestoContext subcontext = PrestoContext.createSubContext(context, field, valueTopic, valueType, valueView, context.isReadOnly());
                        value.setEmbedded(processTopicView(embeddedTopic, subcontext, processType, status));
                        
                    } else {
                        throw new RuntimeException("Expected embedded topic for field '" + field.getId() + "'");
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
        fieldData = processFieldDataExtra(fieldData, context, field, schemaExtra, processType, status);
        
        ObjectNode topicExtra = presto.getTypeExtraNode(context.getType());
        fieldData = processFieldDataExtra(fieldData, context, field, topicExtra, processType, status);
        
        ObjectNode viewExtra = presto.getViewExtraNode(context.getView());
        fieldData = processFieldDataExtra(fieldData, context, field, viewExtra, processType, status);

        ObjectNode fieldExtra = presto.getFieldExtraNode(field);
        fieldData = processFieldDataExtra(fieldData, context, field, fieldExtra, processType, status);
        
        return fieldData;
    }

    private Topic processTopicExtra(Topic topicData, PrestoContext context, ObjectNode extraNode, Type processType, Status status) {
        if (extraNode != null) {
            JsonNode processorsNode = getTopicProcessorsNode(extraNode, processType);
            if (!processorsNode.isMissingNode()) {
                for (TopicProcessor processor : getProcessors(TopicProcessor.class, processorsNode, processType, status)) {
                    topicData = processor.processTopic(topicData, context);
                }
            }
        }
        return topicData;
    }

    private TopicView processTopicViewExtra(TopicView topicView, PrestoContext context, ObjectNode extraNode, Type processType, Status status) {
        if (extraNode != null) {
            JsonNode processorsNode = getTopicViewProcessorsNode(extraNode, processType);
            if (!processorsNode.isMissingNode()) {
                for (TopicViewProcessor processor : getProcessors(TopicViewProcessor.class, processorsNode, processType, status)) {
                    topicView = processor.processTopicView(topicView, context);
                }
            }
        }
        return topicView;
    }
    
    private FieldData processFieldDataExtra(FieldData fieldData, PrestoContext context, PrestoFieldUsage field, ObjectNode extraNode, Type processType, Status status) {
        if (extraNode != null) {
            JsonNode processorsNode = getFieldDataProcessorsNode(extraNode, processType);
            if (!processorsNode.isMissingNode()) {
                for (FieldDataProcessor processor : getProcessors(FieldDataProcessor.class, processorsNode, processType, status)) {
                    fieldData = processor.processFieldData(fieldData, context, field);                
                }
            }
        }
//        fieldData.setValues(processValuesExtra(fieldData.getValues(), context, field, extraNode, processType, status));
        return fieldData;
    }

//    public Collection<Value> postProcessValues(Collection<Value> values, PrestoContext context, PrestoFieldUsage field, Status status) {
//        
//        ObjectNode schemaExtra = presto.getSchemaExtraNode(getSchemaProvider());
//        values = processValuesExtra(values, context, field, schemaExtra, Type.POST_PROCESS, status);
//        
//        ObjectNode topicExtra = presto.getTypeExtraNode(context.getType());
//        values = processValuesExtra(values, context, field, topicExtra, Type.POST_PROCESS, status);
//        
//        ObjectNode viewExtra = presto.getViewExtraNode(context.getView());
//        values = processValuesExtra(values, context, field, viewExtra, Type.POST_PROCESS, status);
//
//        ObjectNode fieldExtra = presto.getFieldExtraNode(field);
//        values = processValuesExtra(values, context, field, fieldExtra, Type.POST_PROCESS, status);
//
//        return values;
//    }
    
//    private Collection<Value> processValuesExtra(Collection<Value> values, PrestoContext context, PrestoFieldUsage field, ObjectNode extraNode, Type processType, Status status) {
//        if (extraNode != null) {
//            JsonNode processorsNode = getValueProcessorsNode(extraNode, processType);
//            if (!processorsNode.isMissingNode()) {
//                for (ValuesProcessor processor : getProcessors(ValuesProcessor.class, processorsNode, processType, status)) {
//                    values = processor.processValues(values, context, field);
//                }
//            }
//        }
//        return values;
//    }
    
    private JsonNode getTopicProcessorsNode(ObjectNode extraNode, Type processType) {
        if (processType == Type.PRE_PROCESS) {
            return extraNode.path("topicPreProcessors");            
        } else {
            return extraNode.path("topicPostProcessors");
        }
    }
    
    private JsonNode getTopicViewProcessorsNode(ObjectNode extraNode, Type processType) {
        if (processType == Type.PRE_PROCESS) {
            return extraNode.path("topicViewPreProcessors");            
        } else {
            return extraNode.path("topicViewPostProcessors");
        }
    }
    
    private JsonNode getFieldDataProcessorsNode(ObjectNode extraNode, Type processType) {
        if (processType == Type.PRE_PROCESS) {
            return extraNode.path("fieldPreProcessors");            
        } else {
            return extraNode.path("fieldPostProcessors");
        }
    }
    
//    private JsonNode getValueProcessorsNode(ObjectNode extraNode, Type processType) {
//        if (processType == Type.PRE_PROCESS) {
//            return extraNode.path("valuePreProcessors");            
//        } else {
//            return extraNode.path("valuePostProcessors");
//        }
//    }

    public <T extends AbstractProcessor> Iterable<T> getProcessors(Class<T> klass, JsonNode processorsNode) {
        return getProcessors(klass, processorsNode, null, null);
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
    
    public <T extends AbstractProcessor> T getProcessor(Class<T> klass, JsonNode processorNode) {
        return getProcessor(klass, processorNode, null, null);
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
