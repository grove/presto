package net.ontopia.presto.spi.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoSchemaProvider;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

public class AbstractHandler implements Handler {

    private ObjectNode config;
    private PrestoDataProvider dataProvider;
    private PrestoSchemaProvider schemaProvider;
    
    @Override
    public PrestoDataProvider getDataProvider() {
        return dataProvider;
    }

    @Override
    public void setDataProvider(PrestoDataProvider dataProvider) {
        this.dataProvider = dataProvider;
    }
    
    @Override
    public PrestoSchemaProvider getSchemaProvider() {
        return schemaProvider;
    }

    @Override
    public void setSchemaProvider(PrestoSchemaProvider schemaProvider) {
        this.schemaProvider = schemaProvider;
    }
    
    @Override
    public ObjectNode getConfig() {
        return config;
    }

    @Override
    public void setConfig(ObjectNode config) {
        this.config = config;
    }
    
    // -- statics

    private static final ObjectMapper mapper = new ObjectMapper();

    public static <T extends Handler> Iterable<T> getHandlers(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, Class<T> klass, JsonNode processorsNode) {
        if (processorsNode.isArray()) {
            List<T> result = new ArrayList<T>();
            for (JsonNode processorNode : processorsNode) {
                T processor = getHandler(dataProvider, schemaProvider, klass, processorNode);
                if (processor != null) {
                    result.add(processor);
                }
            }
            return result;
        } else {
            T processor = getHandler(dataProvider, schemaProvider, klass, processorsNode);
            if (processor != null) {
                return Collections.singleton(processor);
            }
        }
        return Collections.emptyList();
    }
    
    public static <T extends Handler> T getHandler(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, Class<T> klass, JsonNode processorNode) {
        if (processorNode.isTextual()) {
            String className = processorNode.getTextValue();
            if (className != null) {
                return getHandlerInstance(dataProvider, schemaProvider, klass, className, null);
            }
        } else if (processorNode.isObject()) {
            if (processorNode.has("class")) {
                String className = processorNode.path("class").getTextValue();
                if (className != null) {
                    return getHandlerInstance(dataProvider, schemaProvider, klass, className, (ObjectNode)processorNode);
                }
            } else if (processorNode.has("processor")) {
                String ref = processorNode.path("processor").getTextValue();
                ObjectNode extra = (ObjectNode)schemaProvider.getExtra();
                if (extra != null) {
                    JsonNode globalProcessorNode = extra.path("processors").path(ref);
                    ObjectNode mergedProcessorNode = mergeObjectNodes(globalProcessorNode, processorNode);
                    mergedProcessorNode.remove("processor"); // remove 'processor' entry to avoid recursion
                    return getHandler(dataProvider, schemaProvider, klass, mergedProcessorNode);
                }
            } 
        }
//        log.warn("Could not find processor for config: {}", processorNode);
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

    public static <T extends Handler> T getHandlerInstance(PrestoDataProvider dataProvider, PrestoSchemaProvider schemaProvider, Class<T> klass, String className, ObjectNode processorConfig) {
        T processor = Utils.newInstanceOf(className, klass);
        if (processor != null) {
            processor.setConfig(processorConfig);
            processor.setDataProvider(dataProvider);
            processor.setSchemaProvider(schemaProvider);
        }
        return processor;
    }

}
