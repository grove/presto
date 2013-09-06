package net.ontopia.presto.jaxrs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Topic;
import net.ontopia.presto.jaxb.TopicView;
import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.jaxrs.process.FieldDataProcessor;
import net.ontopia.presto.jaxrs.process.SubmittedState;
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

    private static final ObjectMapper mapper = new ObjectMapper();
    
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
    
    public Topic preProcessTopic(Topic topicData, PrestoContextRules rules, Status status) {
        return processTopic(topicData, rules, Type.PRE_PROCESS, status);
    }

    public Topic postProcessTopic(Topic topicData, PrestoContextRules rules, Status status) {
        return processTopic(topicData, rules, Type.POST_PROCESS, status);
    }
    
    private Topic processTopic(Topic topicData, PrestoContextRules rules, Type processType, Status status) {

        // process the topic
        ObjectNode schemaExtra = presto.getSchemaExtraNode(presto.getSchemaProvider());
        topicData = processTopicExtra(topicData, rules, schemaExtra, processType, status);
        
        PrestoContext context = rules.getContext();
        PrestoTopic topic = context.getTopic();
        PrestoType type = context.getType();
        
        ObjectNode topicExtra = presto.getTypeExtraNode(type);
        topicData = processTopicExtra(topicData, rules, topicExtra, processType, status);

        // process the topic views
        Collection<TopicView> views = topicData.getViews();
        Collection<TopicView> newViews = new ArrayList<TopicView>(views.size());

        for (TopicView topicView : views) {
            String viewId = topicView.getId();
            PrestoView specificView = type.getViewById(viewId);
            PrestoContext subcontext = PrestoContext.create(topic, type, specificView);
            PrestoContextRules subrules = presto.getPrestoContextRules(subcontext);
            
            TopicView newView = processTopicView(topicView, subrules, processType, status);
            if (newView != null) {
                newViews.add(newView);
            }
        }            
        topicData.setViews(newViews);

        return topicData;
    }
    
    public TopicView preProcessTopicView(TopicView topicView, PrestoContextRules rules, Status status) {
        return processTopicView(topicView, rules, Type.PRE_PROCESS, status);
    }

    public TopicView postProcessTopicView(TopicView topicView, PrestoContextRules rules, Status status) {
        return processTopicView(topicView, rules, Type.POST_PROCESS, status);
    }
    
    private TopicView processTopicView(TopicView topicView, PrestoContextRules rules, Type processType, Status status) {
        SubmittedState sstate;
        if (processType == Type.PRE_PROCESS) {
            sstate = new SubmittedState(topicView);
        } else {
            sstate = null;
        }
        
        PrestoContext context = rules.getContext();
        
        // process the topic
        ObjectNode schemaExtra = presto.getSchemaExtraNode(presto.getSchemaProvider());
        topicView = processTopicViewExtra(topicView, rules, schemaExtra, processType, status);
        
        ObjectNode topicExtra = presto.getTypeExtraNode(context.getType());
        topicView = processTopicViewExtra(topicView, rules, topicExtra, processType, status);
        
        ObjectNode viewExtra = presto.getViewExtraNode(context.getView());
        topicView = processTopicViewExtra(topicView, rules, viewExtra, processType, status);

        // process the field data
        Collection<FieldData> fields = topicView.getFields();
        if (fields != null) {
            Collection<FieldData> newFields = new ArrayList<FieldData>(fields.size());
            
            for (FieldData fieldData : fields) {
    
                String fieldId = fieldData.getId();
                PrestoFieldUsage field = context.getFieldById(fieldId);
    
                // process field            
                FieldData newField = processFieldData(sstate, fieldData, rules, field, processType, status);
                if (newField != null) {
                    newFields.add(newField);
                }
            
            }
            topicView.setFields(newFields);
        }

        return topicView;
    }
    
    public FieldData preProcessFieldData(FieldData fieldData, PrestoContextRules rules, PrestoFieldUsage field, Status status) {
        SubmittedState sstate = null;
        return processFieldData(sstate, fieldData, rules, field, Type.PRE_PROCESS, status);
    }

    public FieldData postProcessFieldData(FieldData fieldData, PrestoContextRules rules, PrestoFieldUsage field, Status status) {
        SubmittedState sstate = null;
        return processFieldData(sstate, fieldData, rules, field, Type.POST_PROCESS, status);
    }
    
    public FieldData processFieldData(SubmittedState sstate, FieldData fieldData, PrestoContextRules rules, PrestoFieldUsage field, Type processType, Status status) {
        // process nested data first
        PrestoSchemaProvider schemaProvider = getSchemaProvider();
        PrestoDataProvider dataProvider = getDataProvider();
        PrestoContext context = rules.getContext();

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
                                valueTopic = presto.buildInlineTopic(context, field, embeddedTopic);
                            } else {
                                valueTopic = dataProvider.getTopicById(topicId);
                            }
                            valueType = schemaProvider.getTypeById(valueTopic.getTypeId());
                        }

                        PrestoView valueView = field.getValueView(valueType);

                        PrestoContext subcontext = PrestoContext.createSubContext(context, field, valueTopic, valueType, valueView);
                        PrestoContextRules subrules = presto.getPrestoContextRules(subcontext);
                        
                        value.setEmbedded(processTopicView(embeddedTopic, subrules, processType, status));
                        
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
        fieldData = processFieldDataExtra(sstate, fieldData, rules, field, schemaExtra, processType, status);
        
        ObjectNode topicExtra = presto.getTypeExtraNode(context.getType());
        fieldData = processFieldDataExtra(sstate, fieldData, rules, field, topicExtra, processType, status);
        
        ObjectNode viewExtra = presto.getViewExtraNode(context.getView());
        fieldData = processFieldDataExtra(sstate, fieldData, rules, field, viewExtra, processType, status);

        ObjectNode fieldExtra = presto.getFieldExtraNode(field);
        fieldData = processFieldDataExtra(sstate, fieldData, rules, field, fieldExtra, processType, status);
        
        return fieldData;
    }

    private Topic processTopicExtra(Topic topicData, PrestoContextRules rules, ObjectNode extraNode, Type processType, Status status) {
        if (extraNode != null) {
            JsonNode processorsNode = getTopicProcessorsNode(extraNode, processType);
            if (!processorsNode.isMissingNode()) {
                for (TopicProcessor processor : getHandlers(presto, TopicProcessor.class, processorsNode)) {
                    processor.setType(processType);
                    processor.setStatus(status);
                    topicData = processor.processTopic(topicData, rules);
                }
            }
        }
        return topicData;
    }

    private TopicView processTopicViewExtra(TopicView topicView, PrestoContextRules rules, ObjectNode extraNode, Type processType, Status status) {
        if (extraNode != null) {
            JsonNode processorsNode = getTopicViewProcessorsNode(extraNode, processType);
            if (!processorsNode.isMissingNode()) {
                for (TopicViewProcessor processor : getHandlers(presto, TopicViewProcessor.class, processorsNode)) {
                    processor.setType(processType);
                    processor.setStatus(status);
                    topicView = processor.processTopicView(topicView, rules);
                }
            }
        }
        return topicView;
    }
    
    private FieldData processFieldDataExtra(SubmittedState sstate, FieldData fieldData, PrestoContextRules rules, PrestoFieldUsage field, ObjectNode extraNode, Type processType, Status status) {
        if (extraNode != null) {
            JsonNode processorsNode = getFieldDataProcessorsNode(extraNode, processType);
            if (!processorsNode.isMissingNode()) {
                for (FieldDataProcessor processor : getHandlers(presto, FieldDataProcessor.class, processorsNode)) {
                    processor.setType(processType);
                    processor.setStatus(status);
                    processor.setSubmittedState(sstate); // NOTE: only done on FieldProcessors for now
                    fieldData = processor.processFieldData(fieldData, rules, field);                
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
    
    public static <T extends AbstractHandler> Iterable<T> getHandlers(Presto session, Class<T> klass, JsonNode processorsNode) {
        if (processorsNode.isArray()) {
            List<T> result = new ArrayList<T>();
            for (JsonNode processorNode : processorsNode) {
                T processor = getHandler(session, klass, processorNode);
                if (processor != null) {
                    result.add(processor);
                }
            }
            return result;
        } else {
            T processor = getHandler(session, klass, processorsNode);
            if (processor != null) {
                return Collections.singleton(processor);
            }
        }
        return Collections.emptyList();
    }
    
    public static <T extends AbstractHandler> T getHandler(Presto session, Class<T> klass, JsonNode processorNode) {
        T handler = getHandler(session.getSchemaProvider(), klass, processorNode);
        if (handler != null) {
            handler.setPresto(session);
        }
        return handler;
    }
    
    private static <T extends AbstractHandler> T getHandler(PrestoSchemaProvider schemaProvider, Class<T> klass, JsonNode processorNode) {
        if (processorNode.isTextual()) {
            String className = processorNode.getTextValue();
            if (className != null) {
                return getHandlerInstance(klass, className, null);
            }
        } else if (processorNode.isObject()) {
            if (processorNode.has("class")) {
                String className = processorNode.path("class").getTextValue();
                if (className != null) {
                    return getHandlerInstance(klass, className, (ObjectNode)processorNode);
                }
            } else if (processorNode.has("processor")) {
                String ref = processorNode.path("processor").getTextValue();
                ObjectNode extra = (ObjectNode)schemaProvider.getExtra();
                if (extra != null) {
                    JsonNode globalProcessorNode = extra.path("processors").path(ref);
                    ObjectNode mergedProcessorNode = mergeObjectNodes(globalProcessorNode, processorNode);
                    mergedProcessorNode.remove("processor"); // remove 'processor' entry to avoid recursion
                    return getHandler(schemaProvider, klass, mergedProcessorNode);
                }
            } 
        }
        log.warn("Could not find processor for config: {}", processorNode);
        return null;
    }
    
    private static ObjectNode mergeObjectNodes(JsonNode n1, JsonNode n2) {
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

    private static <T extends AbstractHandler> T getHandlerInstance(Class<T> klass, String className, ObjectNode processorConfig) {
        T processor = Utils.newInstanceOf(className, klass);
        if (processor != null) {
            processor.setConfig(processorConfig);
        }
        return processor;
    }

}
