package net.ontopia.presto.spi.impl.solr;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic.PagedValues;
import net.ontopia.presto.spi.PrestoTopic.Paging;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoFieldResolver;
import net.ontopia.presto.spi.utils.PrestoPagedValues;

import org.apache.commons.httpclient.HttpClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrFieldResolver implements PrestoFieldResolver {

    private static Logger log = LoggerFactory.getLogger(SolrFieldResolver.class.getName());

    @SuppressWarnings("unused")
    private final PrestoDataProvider dataProvider;
    @SuppressWarnings("unused")
    private final PrestoContext context;
    private final ObjectNode config;

    private final HttpClient client;


    public SolrFieldResolver(PrestoDataProvider dataProvider, PrestoContext context, ObjectNode config, 
            URL solrServerUrl, HttpClient client) {
        this.dataProvider = dataProvider;
        this.context = context;
        this.config = config;
        this.client = client;
    }

    protected SolrServer getSolrServer() {
        JsonNode urlNode = config.path("url");
        if (urlNode.isTextual()) {
            try {
                return createSolrServer(new URL(urlNode.getTextValue()));
            } catch (MalformedURLException e) {
                throw new RuntimeException("Invalid url: " + config);
            }
        } else {
            throw new RuntimeException("Url is missing: " + config);
        }
    }

    protected SolrServer createSolrServer(URL serverUrl) {
        return new CommonsHttpSolrServer(serverUrl, client);
    }
    
    protected void setQueryParameters(SolrQuery query, JsonNode paramsNode) {
        if (paramsNode.isObject()) {
            ObjectNode oNode = (ObjectNode)paramsNode;
            Iterator<Entry<String, JsonNode>> fields = oNode.getFields();
            while (fields.hasNext()) {
                Entry<String, JsonNode> entry = fields.next();
                String paramName = entry.getKey();
                JsonNode paramValue = entry.getValue();
                if (paramValue.isTextual()) {
                    query.set(paramName, paramValue.getTextValue());
                } else if (paramValue.isArray()) {
                    for (JsonNode apValue : (ArrayNode)paramValue) {
                        if (apValue.isTextual()) {
                            query.add(paramName, apValue.getTextValue());
                        } else {
                            log.warn("Unsupported parameter value: " + apValue + " in " + config);
                        }
                    }
                }
            }
        }
    }

    @Override
    public PagedValues resolve(Collection<? extends Object> objects,
            PrestoType type, PrestoField field, boolean isReference,
            Paging paging) {

        SolrServer server = getSolrServer();
        SolrQuery query = new SolrQuery();

        JsonNode paramsNode = config.path("params");
        //        Collection<JsonNode> replaceVariables = context.replaceVariables(objects, paramsNode);

        setQueryParameters(query, paramsNode);

        if (paging != null) {
            query.setStart(paging.getOffset());
            query.setRows(paging.getLimit());
        }
        try {
            QueryResponse response = server.query(query);

            SolrDocumentList results = response.getResults();
            List<Object> values = new ArrayList<Object>(results.size());
            for (SolrDocument doc : results) {
                Object value = doc.getFirstValue("id");
                values.add(value);
            }
            return new PrestoPagedValues(values, paging, (int)results.getNumFound());

        } catch (SolrServerException e) {
            log.error("Could not execute Solr query.", e);
            return new PrestoPagedValues(Collections.emptyList(), paging, 0);
        }
    }

}
