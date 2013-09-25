package net.ontopia.presto.spi.impl.solr;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Paging;
import net.ontopia.presto.spi.resolve.PrestoFieldResolver;
import net.ontopia.presto.spi.utils.PrestoPagedValues;
import net.ontopia.presto.spi.utils.PrestoVariableContext;
import net.ontopia.presto.spi.utils.PrestoVariableResolver;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.SortClause;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SolrFieldResolver extends PrestoFieldResolver {

    private static Logger log = LoggerFactory.getLogger(SolrFieldResolver.class);

    public abstract URL getSolrServerUrl();

    public abstract SolrServer getSolrServer();

    @Override
    public PagedValues resolve(Collection<? extends Object> objects,
            PrestoField field, boolean isReference,
            Paging paging, PrestoVariableResolver variableResolver) {

        ObjectNode config = getConfig();
        
        CharSequence q_query = expandQuery(variableResolver, " AND ", objects, config.path("q"));
        if (isEmpty(q_query)) {
            return new PrestoPagedValues(Collections.emptyList(), paging, 0);
        }

        SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQuery(q_query.toString());

        String qt = getStringValue("qt", config, null);
        if (qt != null) {
            solrQuery.setRequestHandler(qt);
        }

        CharSequence fq_query = expandQuery(variableResolver, " AND ", objects, config.path("fq"));
        if (!isEmpty(fq_query)) {
            solrQuery.setFilterQueries(fq_query.toString());
        }

        String qf = getStringValue("qf", config, null);
        if (qf != null) {
            solrQuery.setParam("qf", qf);
        }
        
        String idField = getStringValue("idField", config, "id");
        solrQuery.setFields(idField);

        String orderBy = getStringValue("orderBy", config, null);
        if (orderBy != null) {
            solrQuery.addSort(SortClause.asc(orderBy));
        }
        if (paging != null) {
            solrQuery.setStart(paging.getOffset());
            solrQuery.setRows(paging.getLimit());
        } else {
            int rows = getIntValue("rows", config, 100);
            solrQuery.setRows(rows);
        }

        PrestoVariableContext context = getVariableContext();
        
        SolrServer solrServer = getSolrServer();
        URL solrServerUrl = getSolrServerUrl();

        try {
            QueryResponse qr;
            if (log.isDebugEnabled()) {
                long start = System.currentTimeMillis();
                qr = solrServer.query(solrQuery, METHOD.POST);
                long numFound = qr.getResults().getNumFound();
                log.debug("Q: {}ms {} {}/select?{}&echoParams=explicit", new Object[] { System.currentTimeMillis()-start, numFound, solrServerUrl, solrQuery });
            } else {
                qr = solrServer.query(solrQuery);
            }

            SolrDocumentList results = qr.getResults();
            List<String> values = new ArrayList<String>(results.size());
            for (SolrDocument doc : results) {
                Object value = doc.getFirstValue(idField);
                if (value != null && value instanceof String) {
                    values.add((String)value);
                }
            }

            List<Object> result = new ArrayList<Object>(results.size());
            if (isReference) {
                result.addAll(context.getDataProvider().getTopicsByIds(values));
            } else {
                result.addAll(values);
            }

            int total = (int)results.getNumFound();
            return new PrestoPagedValues(result, paging, total);

        } catch (SolrServerException e) {
            log.error("QE: " + solrServerUrl + "/select?" + solrQuery + "&echoParams=explicit", e);
            return new PrestoPagedValues(Collections.emptyList(), paging, 0);
        }
    }

    private boolean isEmpty(CharSequence c) {
        return c == null || c.length() == 0;
    }
    
    private CharSequence expandQuery(PrestoVariableResolver variableResolver, String sep, Collection<? extends Object> objects, JsonNode q) {
        StringBuilder sb  = new StringBuilder();
        if (q.isObject()) {
            ObjectNode qo = (ObjectNode)q;

            if (qo.size() == 1 && qo.has("AND")) {
                CharSequence cs = expandQuery(variableResolver, " AND ", objects, qo.path("AND"));
                if (!isEmpty(cs)) {
                    sb.append(cs);
                }

            } else if (qo.size() == 1 && qo.has("OR")) {
                CharSequence cs = expandQuery(variableResolver, " OR ", objects, qo.path("OR"));
                if (!isEmpty(cs)) {
                    sb.append(cs);
                }

            } else if (qo.size() == 1 && qo.has("NOT")) {                
                sb.append("NOT ");
                CharSequence cs = expandQuery(variableResolver, " AND ", objects, qo.path("NOT"));
                if (!isEmpty(cs)) {
                    sb.append(cs);
                }

            } else {
                if (!expandVariable(sb, variableResolver, sep, objects, qo)) {
                    return null;
                }
            }
        } else if (q.isArray()) {
            ArrayNode ao = (ArrayNode)q;
            int size = ao.size();
            boolean foundItems = false;
            for (int i=0; i < size; i++) {
                CharSequence cs = expandQuery(variableResolver, " OR ", objects, ao.get(i));
                if (isEmpty(cs)) {
                    if (sep.equals(" AND ")) {
                        return null;
                    } else {
                        continue;
                    }
                } else {
                    if (i > 0 && foundItems) {
                        sb.append(sep);
                    }
                    sb.append(cs);
                    foundItems = true;
                }
            }
        } else if (q.isTextual()) {
            if (!expandVariable(sb, variableResolver, sep, objects, q)) {
                return null;
            }
        } else if (q.isMissingNode()) {
            // ignore
        } else {
            log.warn("Unknown query node: " + q);
        }
        return sb;
    }

    private boolean expandVariable(StringBuilder sb, PrestoVariableResolver variableResolver, String sep, Collection<? extends Object> objects, JsonNode qo) {
        Collection<JsonNode> qvalues = getVariableContext().replaceVariables(variableResolver, objects, qo);
        if (qvalues.isEmpty()) {
            return false;
        }
        Iterator<JsonNode> qviter = qvalues.iterator();
        if (qvalues.size() > 1) {
            sb.append('(');
        }
        boolean foundItems = false;
        while (qviter.hasNext()) {
            JsonNode jn = qviter.next();
            if (foundItems) {
                sb.append(sep);
            }
            if (jn == null || jn.isNull()) {
                continue;
            } else if (jn.isObject()) {
                ObjectNode qv = (ObjectNode)jn;
                Iterator<String> fieldNames = qv.getFieldNames();
                if (qv.size() > 1) {
                    sb.append('(');
                }
                while (fieldNames.hasNext()) {
                    String fieldName = fieldNames.next();
                    String fieldValue = qv.get(fieldName).getTextValue();
                    sb.append(fieldName).append(':').append(ClientUtils.escapeQueryChars(fieldValue));
                    foundItems = true;
                    if (fieldNames.hasNext()) {
                        sb.append(" AND ");
                    }
                }
                if (qv.size() > 1) {
                    sb.append(')');
                }
            } else if (jn.isTextual()) {
                sb.append(jn.getTextValue());
            } else {
                log.warn("Unknown node: {}", jn);
            }
        }
        if (qvalues.size() > 1) {
            sb.append(')');
        }
        return true;
    }

    @SuppressWarnings("unused")
    private String getStringValue(String field, ObjectNode config) {
        JsonNode fieldNode = config.path(field);
        if (fieldNode.isTextual()) {
            return fieldNode.getTextValue();
        } else {
            throw new RuntimeException("'" + field + "' missing on field resolver: " + config);
        }
    }

    private String getStringValue(String field, ObjectNode config, String default_) {
        JsonNode fieldNode = config.path(field);
        if (fieldNode.isTextual()) {
            return fieldNode.getTextValue();
        } else {
            return default_;
        }
    }

    private int getIntValue(String field, ObjectNode config, int default_) {
        JsonNode fieldNode = config.path(field);
        if (fieldNode.isNumber()) {
            return fieldNode.getIntValue();
        } else {
            return default_;
        }
    }

}
