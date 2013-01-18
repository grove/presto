package net.ontopia.presto.spi.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Paging;
import net.ontopia.presto.spi.PrestoType;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrestoTraverseResolver extends PrestoFieldResolver {

    private static Logger log = LoggerFactory.getLogger(PrestoTraverseResolver.class);

    @Override
    public PagedValues resolve(Collection<? extends Object> objects,
            PrestoField field, boolean isReference, Paging paging, PrestoVariableResolver variableResolver) {
        
        ObjectNode config = getConfig();
        
        if (config.has("path")) {
            JsonNode pathNode = config.get("path");
            if (pathNode.isArray()) {
                Collection<Object> rs = new HashSet<Object>(objects);
                for (JsonNode fieldItem : pathNode) {
                    // TODO: allow optional recursion
                    String fieldId = fieldItem.getTextValue();
                    rs = traverseField(rs, fieldId);
                }
                List<Object> result = new ArrayList<Object>(rs);
                return new PrestoPagedValues(result, paging, result.size());
            }
        }
        return new PrestoPagedValues(Collections.emptyList(), paging, 0);        
    }

    private Collection<Object> traverseField(Collection<Object> objects, String fieldId) {
        PrestoVariableContext context = getVariableContext();
        
        Collection<Object> result = new HashSet<Object>();
        for (Object object : objects) {
            if (object instanceof PrestoTopic) {
                PrestoTopic topic = (PrestoTopic)object;
                String typeId = topic.getTypeId();
                PrestoType type = context.getSchemaProvider().getTypeById(typeId);
                try {
                    PrestoField field = type.getFieldById(fieldId);
                    List<? extends Object> values = topic.getValues(field);
                    result.addAll(values);
                } catch (Exception e) {
                    log.warn("Object " + topic.getId() + " does not have field '" + fieldId + "'");
                }
            } else {
                log.warn("Value " + object + " does not have field '" + fieldId + "'");
            }
        }
        return result;
    }

}
