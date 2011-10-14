package net.ontopia.presto.spi.impl.couchdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.utils.PrestoPagedValues;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoFieldResolver;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;
import org.ektorp.ViewResult.Row;

public class CouchQueryResolver implements PrestoFieldResolver {

    private final CouchDataProvider dataProvider;
    private final PrestoContext context;

    CouchQueryResolver(CouchDataProvider dataProvider, PrestoSchemaProvider schemaProvider) {
        this.dataProvider = dataProvider;
        this.context = new PrestoContext(dataProvider, schemaProvider, dataProvider.getObjectMapper());
    }

    @Override
    public PagedValues resolve(Collection<? extends Object> objects,
            PrestoType type, PrestoField field, boolean isReference, ObjectNode resolveItem, 
            boolean paging, int _limit, int offset, int limit) {
        String designDocId = resolveItem.get("designDocId").getTextValue();
        String viewName = resolveItem.get("viewName").getTextValue();

        boolean includeDocs = resolveItem.has("includeDocs") && resolveItem.get("includeDocs").getBooleanValue();

        ViewQuery query = new ViewQuery()
        .designDocId(designDocId)
        .viewName(viewName)
        .staleOk(true)
        .reduce(false)
        .includeDocs(includeDocs);

        Collection<?> keys = new ArrayList<Object>();
        Object startKey = null;
        Object endKey = null;

        if (resolveItem.has("key")) {
            keys = context.replaceKeyVariables(objects, resolveItem.get("key"));
            if (keys.isEmpty()) {
                return new PrestoPagedValues(Collections.emptyList(), 0, _limit,0);
            }
            query = query.keys(keys);

        } else if (resolveItem.has("startKey") && resolveItem.has("endKey")) {

            Collection<?> startKeys = context.replaceKeyVariables(objects, resolveItem.get("startKey"));            
            Collection<?> endKeys = context.replaceKeyVariables(objects, resolveItem.get("endKey"));
            
            if (startKeys.size() != endKeys.size()) {
                throw new RuntimeException("startKey and endKey of different sizes: " + startKeys + " and " + endKeys);
            }
            
            if (startKeys.isEmpty()) {
                return new PrestoPagedValues(Collections.emptyList(), 0, _limit,0);
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

        if (paging) {
            if (offset > 0) {
                query = query.skip(offset);
            }
            if (limit > 0) {
                query = query.limit(limit);
            }
        }

        List<Object> result = new ArrayList<Object>();        
        ViewResult viewResult = dataProvider.getCouchConnector().queryView(query);

        if (includeDocs) {
            for (Row row : viewResult.getRows()) {
                JsonNode docNode = (JsonNode)row.getDocAsNode();
                if (docNode != null) {
                    if (docNode.isObject()) {
                        result.add(dataProvider.existing((ObjectNode)docNode));
                    } else {
                        result.add(docNode.getTextValue());
                    }
                }
            }
        } else {
            List<String> values = new ArrayList<String>();        
            for (Row row : viewResult.getRows()) {
                JsonNode valueNode = row.getValueAsNode();
                if (valueNode != null) {
                    if (valueNode.isTextual()) {
                        String textValue = valueNode.getTextValue();
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
        if (resolveItem.has("excludeSelf") && resolveItem.get("excludeSelf").getBooleanValue()) {
            result.removeAll(objects);
        }
        int totalSize = viewResult.getSize();
        if (paging && !(totalSize < limit)) {
            if (resolveItem.has("count") && resolveItem.get("count").getTextValue().equals("reduce-value")) {
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
        }
        return new PrestoPagedValues(result, offset, limit, totalSize);
    }

}
