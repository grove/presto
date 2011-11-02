package net.ontopia.presto.spi.impl.couchdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Paging;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoFieldResolver;
import net.ontopia.presto.spi.utils.PrestoPagedValues;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;
import org.ektorp.ViewResult.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CouchViewResolver implements PrestoFieldResolver {

    @SuppressWarnings("unused")
    private static Logger log = LoggerFactory.getLogger(CouchViewResolver.class.getName());

    private final CouchDataProvider dataProvider;
    private final PrestoContext context;
    private final ObjectNode config;

    CouchViewResolver(CouchDataProvider dataProvider, PrestoContext context, ObjectNode config) {
        this.dataProvider = dataProvider;
        this.context = context;
        this.config = config;
    }

    @Override
    public PagedValues resolve(Collection<? extends Object> objects,
            PrestoType type, PrestoField field, boolean isReference, Paging paging) {
        String designDocId = config.get("designDocId").getTextValue();
        String viewName = config.get("viewName").getTextValue();

        boolean includeDocs = config.has("includeDocs") && config.get("includeDocs").getBooleanValue();

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
            keys = context.replaceVariables(objects, config.get("key"));
            if (keys.isEmpty()) {
                return new PrestoPagedValues(Collections.emptyList(), paging, 0);
            }
            query = query.keys(keys);

        } else if (config.has("startKey") && config.has("endKey")) {

            Collection<?> startKeys = context.replaceVariables(objects, config.get("startKey"));            
            Collection<?> endKeys = context.replaceVariables(objects, config.get("endKey"));
            
            if (startKeys.size() != endKeys.size()) {
                throw new RuntimeException("startKey and endKey of different sizes: " + startKeys + " and " + endKeys);
            }
            
            if (startKeys.isEmpty()) {
                return new PrestoPagedValues(Collections.emptyList(), paging, 0);
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

        if (paging != null) {
            int offset = paging.getOffset();
            if (offset > 0) {
                query = query.skip(offset);
            }
            int limit = paging.getLimit();
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
        if (config.has("excludeSelf") && config.get("excludeSelf").getBooleanValue()) {
            result.removeAll(objects);
        }
        int totalSize = viewResult.getSize();
        if (paging != null) {
            if (totalSize >= paging.getLimit()) {
                if (config.has("count") && config.get("count").getTextValue().equals("reduce-value")) {
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
                totalSize += paging.getOffset();
            }
        }
        return new PrestoPagedValues(result, paging, totalSize);
    }

}
