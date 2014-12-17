package net.ontopia.presto.spi.impl.couchdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Projection;
import net.ontopia.presto.spi.resolve.PrestoFieldResolver;
import net.ontopia.presto.spi.resolve.PrestoResolver;
import net.ontopia.presto.spi.utils.PrestoPagedValues;
import net.ontopia.presto.spi.utils.PrestoVariableContext;
import net.ontopia.presto.spi.utils.PrestoVariableResolver;

import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;
import org.ektorp.ViewResult.Row;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class CouchViewResolver extends PrestoFieldResolver {

    @Override
    public PagedValues resolve(Collection<? extends Object> objects,
            PrestoField field, boolean isReference, Projection projection, 
            PrestoResolver prestoResolver, PrestoVariableResolver variableResolver) {

        PrestoVariableContext context = getVariableContext();
        ObjectNode config = getConfig();
        
        String designDocId = config.get("designDocId").textValue();
        String viewName = config.get("viewName").textValue();

        boolean includeDocs = config.has("includeDocs") && config.get("includeDocs").booleanValue();

        ViewQuery query = new ViewQuery()
        .designDocId(designDocId)
        .viewName(viewName)
        .staleOk(true)
        .reduce(false)
        .includeDocs(includeDocs);

        Collection<?> keys = new ArrayList<Object>();
        Object startKey = null;
        Object endKey = null;

        if (config.has("key")) {
            keys = context.replaceVariables(variableResolver, objects, config.get("key"));
            if (keys.isEmpty()) {
                return new PrestoPagedValues(Collections.emptyList(), projection, 0);
            }
            query = query.keys(keys);

        } else if (config.has("startKey") && config.has("endKey")) {
            Collection<?> startKeys = context.replaceVariables(variableResolver, objects, config.get("startKey"));            
            Collection<?> endKeys = context.replaceVariables(variableResolver, objects, config.get("endKey"));
            
            if (startKeys.size() != endKeys.size()) {
                throw new RuntimeException("startKey and endKey of different sizes: " + startKeys + " and " + endKeys);
            }
            
            if (startKeys.isEmpty()) {
                return new PrestoPagedValues(Collections.emptyList(), projection, 0);
            }
            
            if (startKeys.size() > 1) {
                throw new RuntimeException("startKey or endKey not a single value: " + startKeys + " and " + endKeys);
            }

            startKey = startKeys.iterator().next();
            query = query.startKey(startKey);
            
            endKey = endKeys.iterator().next();
            query = query.endKey(endKey);

        } else {
            Collection<String> _keys = new ArrayList<String>(objects.size());
            for (Object topic : objects) {
                if (topic instanceof PrestoTopic) {
                    _keys.add(((PrestoTopic)topic).getId());
                }
            }
            keys = _keys;
            query = query.keys(keys);
        }

        if (projection != null) {
            int offset = projection.getOffset();
            if (offset > 0) {
                query = query.skip(offset);
            }
            int limit = projection.getLimit();
            if (limit > 0) {
                query = query.limit(limit);
            }
        }

        List<Object> result = new ArrayList<Object>();

        CouchDataProvider dataProvider = (CouchDataProvider)getDataProvider();
        ViewResult viewResult = dataProvider.getCouchConnector().queryView(query);

        if (includeDocs) {
            for (Row row : viewResult.getRows()) {
                JsonNode docNode = (JsonNode)row.getDocAsNode();
                if (docNode != null) {
                    if (docNode.isObject()) {
                        result.add(dataProvider.existing((ObjectNode)docNode));
                    } else {
                        result.add(docNode.textValue());
                    }
                }
            }
        } else {
            List<String> values = new ArrayList<String>();        
            for (Row row : viewResult.getRows()) {
                JsonNode valueNode = row.getValueAsNode();
                if (valueNode != null) {
                    if (valueNode.isTextual()) {
                        String textValue = valueNode.textValue();
                        if (textValue != null) {
                            result.add(textValue);
                        }
                    } else {
                        result.add(valueNode.toString());
                    }
                }
            }
            if (isReference) {
                result.addAll(dataProvider.getTopicsByIds(values));
            } else {
                result.addAll(values);
            }
        }
        if (config.has("excludeSelf") && config.get("excludeSelf").booleanValue()) {
            result.removeAll(objects);
        }
        int totalSize = viewResult.getSize();
        if (projection != null) {
            if (totalSize >= projection.getLimit()) {
                if (config.has("count") && config.get("count").textValue().equals("reduce-value")) {
                    ViewQuery countQuery = new ViewQuery()
                    .designDocId(designDocId)
                    .viewName(viewName)                
                    .startKey(startKey)
                    .endKey(endKey)
                    .staleOk(true)
                    .reduce(true);
                    ViewResult countViewResult = dataProvider.getCouchConnector().queryView(countQuery);
                    for (Row row : countViewResult.getRows()) {
                        totalSize = row.getValueAsInt();
                    }
                }
            } else {
                totalSize += projection.getOffset();
            }
        }
        return new PrestoPagedValues(result, projection, totalSize);
    }

}
