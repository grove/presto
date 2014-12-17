package net.ontopia.presto.jaxrs.resolve;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.ontopia.presto.jaxrs.process.impl.ExpressionValueFactory;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.rules.PathExpressions;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ExpressionAvailableFieldValuesResolver extends AvailableFieldValuesResolver {

    @Override
    public Collection<? extends Object> getAvailableFieldValues(PrestoContextRules rules, PrestoField field, String query) {
        PrestoDataProvider dataProvider = getDataProvider();
        return getValues(dataProvider, getSchemaProvider(), rules, field, getConfig());
    }

    private List<? extends Object> getValues(PrestoDataProvider dataProvider, 
            PrestoSchemaProvider schemaProvider, PrestoContextRules rules, PrestoField field, ObjectNode config) {

        List<? extends Object> values = ExpressionValueFactory.getValues(dataProvider, schemaProvider, rules, field, config);
        
        JsonNode excludePathNode = config.path("excludePath");
        if (!excludePathNode.isMissingNode()) {
            String excludePath = excludePathNode.textValue();
            List<? extends Object> excludeValues = PathExpressions.getValues(rules, excludePath);
            if (!excludeValues.isEmpty()) {
                
                Set<String> existingSet = null;
                
                PrestoContext context = rules.getContext();
                if (!context.isNewTopic()) {
                    List<? extends Object> existingValues = context.resolveValues(field);
                    if (existingValues != null && !excludeValues.isEmpty()) {
                        existingSet = new HashSet<String>(existingValues.size());
                        for (Object value : existingValues) {
                            existingSet.add(getKey(value));
                        }
                    }
                }
 
                Map<String,Object> keyMap = new HashMap<String,Object>(values.size());
                for (Object value : values) {
                    keyMap.put(getKey(value), value);
                }
                for (Object value : excludeValues) {
                    String excludeKey = getKey(value);
                    if (existingSet == null || !existingSet.contains(excludeKey)) {
                        keyMap.remove(excludeKey);
                    }
                }
                return new ArrayList<Object>(keyMap.values());
            }
        }
        return values;
        
//        JsonNode fieldNode = config.path("field");
//        if (fieldNode.isTextual()) {
//            PrestoContext context = rules.getContext();
//            PrestoContext parentContext = context.getParentContext();
//            PrestoContextRules parentRules = rules.getPrestoContextRules(parentContext);
//            String fieldId = fieldNode.textValue();
//            return PathExpressions.getValues(dataProvider, schemaProvider, parentRules, fieldId);
//        } 
//        throw new RuntimeException("Not able to find field from configuration: " + config);
    }

    private String getKey(Object value) {
        if (value instanceof PrestoTopic) {
            PrestoTopic topic = (PrestoTopic)value;
            return topic.getId();
        } else {
            return value.toString();
        }
    }
    
}
