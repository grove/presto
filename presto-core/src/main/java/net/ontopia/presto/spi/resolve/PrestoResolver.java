package net.ontopia.presto.spi.resolve;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Projection;
import net.ontopia.presto.spi.jackson.JacksonDataProvider;
import net.ontopia.presto.spi.utils.PrestoPagedValues;
import net.ontopia.presto.spi.utils.PrestoTopicFieldVariableResolver;
import net.ontopia.presto.spi.utils.PrestoVariableContext;
import net.ontopia.presto.spi.utils.PrestoVariableResolver;
import net.ontopia.presto.spi.utils.Utils;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PrestoResolver {

    private static Logger log = LoggerFactory.getLogger(PrestoResolver.class);

    protected abstract JacksonDataProvider getDataProvider();

    public List<? extends Object>  resolveValues(PrestoTopic topic, PrestoField field) {
        // get field values from data provider
        ObjectNode extra = (ObjectNode)field.getExtra();
        if (extra != null && extra.has("resolve")) {
            JsonNode resolveConfig = extra.get("resolve");
            Projection projection = null;
            PrestoVariableResolver variableResolver = new PrestoTopicFieldVariableResolver(field.getSchemaProvider());
            return resolveValues(Collections.singleton(topic), field, projection, resolveConfig, variableResolver).getValues();
        }
        return topic.getStoredValues(field);
    }

    public PagedValues resolveValues(PrestoTopic topic, PrestoField field, Projection projection) {
        // get field values from data provider
        ObjectNode extra = (ObjectNode)field.getExtra();
        if (extra != null && extra.has("resolve")) {
            JsonNode resolveConfig = extra.get("resolve");
            PrestoVariableResolver variableResolver = new PrestoTopicFieldVariableResolver(field.getSchemaProvider());
            return resolveValues(Collections.singleton(topic), field, projection, resolveConfig, variableResolver);
        }
        return topic.getStoredValues(field, projection);
    }

    public PagedValues resolveValues(Collection<? extends Object> objects, 
            PrestoField field, Projection projection, JsonNode resolveConfig, PrestoVariableResolver variableResolver) {
        if (resolveConfig.isArray()) {
            ArrayNode resolveArray = (ArrayNode)resolveConfig;
            return resolveValues(objects, field, resolveArray, projection, variableResolver);
        } else {
            throw new RuntimeException("resolve on field " + field.getId() + " is not an array: " + resolveConfig);
        }
    }

    private PagedValues resolveValues(Collection<? extends Object> objects, 
            PrestoField field, ArrayNode resolveArray, Projection projection, PrestoVariableResolver variableResolver) {
        PagedValues result = null;
        int size = resolveArray.size();
        for (int i=0; i < size; i++) {
            boolean isLast = (i == size-1);
            boolean isReference = field.isReferenceField() || !isLast;
            ObjectNode resolveConfig = (ObjectNode)resolveArray.get(i);
            result = resolveValues(objects, field, isReference, resolveConfig, projection, variableResolver);
            objects = result.getValues();
        }
        return result;
    }

    private PagedValues resolveValues(Collection<? extends Object> objects,
            PrestoField field, boolean isReference, ObjectNode resolveConfig, 
            Projection projection, PrestoVariableResolver variableResolver) {

        PrestoFieldResolver resolver = createFieldResolver(field.getSchemaProvider(), resolveConfig);
        if (resolver == null) {
            return new PrestoPagedValues(Collections.emptyList(), projection, 0);            
        } else {
            return resolver.resolve(objects, field, isReference, projection, variableResolver);
        }
    }

    protected PrestoFieldResolver createFieldResolver(PrestoSchemaProvider schemaProvider, ObjectNode resolveConfig) {
        String type = resolveConfig.get("type").getTextValue();
        if (type == null) {
            log.error("'type' not specified on resolve item: " + resolveConfig);
            return null;
        } else {
            return createFieldResolver(type, schemaProvider, resolveConfig);
        }
    }

    protected PrestoFieldResolver createFieldResolver(String type, PrestoSchemaProvider schemaProvider, ObjectNode resolveConfig) {
        String className = getFieldResolverClassName(type, schemaProvider, resolveConfig);
        if (className != null) {
            PrestoFieldResolver fieldResolver = Utils.newInstanceOf(className, PrestoFieldResolver.class);
            JacksonDataProvider dataProvider = getDataProvider();
            PrestoVariableContext context = new PrestoVariableContext(schemaProvider, dataProvider, dataProvider.getObjectMapper());
            fieldResolver.setVariableContext(context);
            fieldResolver.setConfig(resolveConfig);
            return fieldResolver;
        }
        log.warn("Unknown field resolver: type={}, config={}", type, resolveConfig);
        return null;
    }

    protected String getFieldResolverClassName(String type, PrestoSchemaProvider schemaProvider, ObjectNode config) {
        ObjectNode extra = (ObjectNode)schemaProvider.getExtra();
        if (extra != null) {
            JsonNode resolvers = extra.path("resolvers");
            if (resolvers.isObject()) {
                JsonNode classNode = resolvers.path(type).path("class");
                if (classNode.isTextual()) {
                    return classNode.getTextValue();
                }
            }
        }
        return null;
    }

}
