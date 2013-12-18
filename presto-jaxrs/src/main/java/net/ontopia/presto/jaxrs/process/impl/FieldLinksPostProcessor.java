package net.ontopia.presto.jaxrs.process.impl;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import javax.ws.rs.core.UriBuilder;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Link;
import net.ontopia.presto.jaxrs.ExtraUtils;
import net.ontopia.presto.jaxrs.PathParser;
import net.ontopia.presto.jaxrs.Presto;
import net.ontopia.presto.jaxrs.process.FieldDataProcessor;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoView;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

public class FieldLinksPostProcessor extends FieldDataProcessor {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public FieldData processFieldData(FieldData fieldData, PrestoContextRules rules, PrestoFieldUsage field) {
        ObjectNode extraNode = ExtraUtils.getFieldExtraNode(field);
        if (extraNode != null) {
            JsonNode linksNode = extraNode.path("links");
            if (linksNode.isArray()) {
                Collection<Link> links = fieldData.getLinks();
                if (links == null) {
                    links = new ArrayList<Link>(linksNode.size());
                }
                for (JsonNode linkNode : linksNode) {
                    if (linkNode.isObject()) {
                        Link link;
                        if (linkNode.has("action")) {
                            String actionId = linkNode.path("action").getTextValue();
                            String rel = "action";
                            String href = getActionLink(rules.getContext(), field, actionId);
                            String name = linkNode.path("name").getTextValue();
                            link = new Link(rel, href);
                            link.setName(name);
                            Map<String, Object> params = ExtraUtils.getParamsMap(linkNode.path("params"));
                            link.setParams(params);
                            // TODO: support nested links?
                        } else {
                            link = getLink(linkNode);
                        }
                        if (link != null) {
                            links.add(link);
                        }
                    }
                }
                fieldData.setLinks(links);
            }
        }
        return fieldData;
    }
    
    private String getActionLink(PrestoContext context, PrestoFieldUsage field, String actionId) {
        Presto session = getPresto();
        URI baseUri = session.getBaseUri();
        String databaseId = session.getDatabaseId();
        
        PrestoContext parentContext = context.getParentContext();
        PrestoFieldUsage parentField = context.getParentField();

        String topicId = context.getTopicId();
        PrestoView view = context.getView();
        
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        String viewId = view.getId();
        String fieldId = field.getId();
        
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("execute-field-action").path(databaseId).path(path).path(PathParser.skull(topicId)).path(viewId).path(fieldId).path(actionId);
        return builder.build().toString();        
    }

    private Link getLink(JsonNode link) {
        try {
            return mapper.readValue(link, Link.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
