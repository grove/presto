package net.ontopia.presto.spi.impl.solr;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Paging;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoFieldResolver;
import net.ontopia.presto.spi.utils.PrestoPagedValues;

import org.apache.commons.httpclient.HttpClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrFieldResolver implements PrestoFieldResolver {

    private static Logger log = LoggerFactory.getLogger(SolrFieldResolver.class.getName());

    private final ObjectNode config;

    private final SolrServer solrServer;

    private final PrestoContext context;

    private URL solrServerUrl;

    public SolrFieldResolver(PrestoContext context, ObjectNode config, HttpClient client) {
        this.context = context;
        this.config = config;
        this.solrServerUrl = createSolrServerUrl(config);
        this.solrServer = createSolrServer(solrServerUrl, client);
    }

    public SolrFieldResolver(PrestoContext context, ObjectNode config, URL solrServerUrl, SolrServer solrServer) {
        this.context = context;
        this.config = config;
        this.solrServerUrl = solrServerUrl;
        this.solrServer = solrServer;
    }
    
    protected URL createSolrServerUrl(ObjectNode config) {
        JsonNode urlNode = config.path("url");
        if (urlNode.isTextual()) {
            try {
                return new URL(urlNode.getTextValue());
            } catch (MalformedURLException e) {
                throw new RuntimeException("Invalid url: " + config);
            }
        } else {
            throw new RuntimeException("Url is missing: " + config);
        }
    }
    
    protected SolrServer createSolrServer(URL solrServerUrl, HttpClient client) {
        return new CommonsHttpSolrServer(solrServerUrl, client);
    }

    @Override
    public PagedValues resolve(Collection<? extends Object> objects,
            PrestoType type, PrestoField field, boolean isReference,
            Paging paging) {

        SolrQuery solrQuery = new SolrQuery();

        JsonNode qNode = config.path("q");
        Collection<JsonNode> qnodes = context.replaceVariables(objects, qNode);

        StringBuilder sb = new StringBuilder();
        for (JsonNode qn : qnodes) {
            ObjectNode qObject = (ObjectNode)qn;
            Iterator<String> qFields = qObject.getFieldNames();
            while (qFields.hasNext()) {
                String fieldName = qFields.next();
                String fieldValue = qObject.get(fieldName).getTextValue();
                sb.append(fieldName).append(':').append(ClientUtils.escapeQueryChars(fieldValue));
                if (qFields.hasNext()) {
                    sb.append(" AND ");
                }
            }
        }
        solrQuery.setQuery(sb.toString());

        String idField = getStringValue("idField", config);

        String orderBy = getStringValue("orderBy", config, null);
        if (orderBy != null) {
            solrQuery.addSortField(orderBy, ORDER.asc);
        }
        if (paging != null) {
            solrQuery.setStart(paging.getOffset());
            solrQuery.setRows(paging.getLimit());
        } else {
            solrQuery.setRows(100);
        }
        try {
            QueryResponse response;
            if (log.isDebugEnabled()) {
                long start = System.currentTimeMillis();
                response = solrServer.query(solrQuery, METHOD.POST);
                log.debug("Q: {}ms {}/select?{}", new Object[] { System.currentTimeMillis()-start, solrServerUrl, solrQuery});
            } else {
                response = solrServer.query(solrQuery);
            }
            
            SolrDocumentList results = response.getResults();
            List<Object> values = new ArrayList<Object>(results.size());
            for (SolrDocument doc : results) {
                Object value = doc.getFirstValue(idField);
                if (value != null && value instanceof String) {
                    if (field.isReferenceField()) {
                        PrestoTopic topic = context.getDataProvider().getTopicById((String)value);
                        if (topic != null) {
                            values.add(topic);
                        }
                    } else {
                        values.add(value);
                    }
                }
            }
            int total = (int)results.getNumFound();
            return new PrestoPagedValues(values, paging, total);

        } catch (SolrServerException e) {
            log.error("QE: " + solrServerUrl + "/select?" + solrQuery, e);
            return new PrestoPagedValues(Collections.emptyList(), paging, 0);
        }
    }

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

}
