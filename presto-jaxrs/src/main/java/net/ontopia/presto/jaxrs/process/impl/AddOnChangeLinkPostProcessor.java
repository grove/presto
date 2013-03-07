package net.ontopia.presto.jaxrs.process.impl;

import java.util.Collection;
import java.util.LinkedHashSet;

import javax.ws.rs.core.UriBuilder;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Link;
import net.ontopia.presto.jaxrs.Presto;
import net.ontopia.presto.jaxrs.process.FieldDataProcessor;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoView;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class AddOnChangeLinkPostProcessor extends FieldDataProcessor {

    @Override
    public FieldData processFieldData(FieldData fieldData, PrestoTopic topic, PrestoFieldUsage field) {
        
        ObjectNode processorConfig = getConfig();
        if (processorConfig != null) {
            JsonNode validateNode = processorConfig.path("validateOnChange");
            if (validateNode.isBoolean() && validateNode.asBoolean()) {
                return addOnChangeLink(fieldData, topic, field);
            }
        }
        
        return fieldData;
    }

    private FieldData addOnChangeLink(FieldData fieldData, PrestoTopic topic,
            PrestoFieldUsage field) {
        // TODO: may want to support this for inline *new* topics as well
        if (topic != null) {
            Collection<Link> links = fieldData.getLinks();
            if (links == null) {
                links = new LinkedHashSet<Link>();
            }
            
            Presto presto = getPresto();
            PrestoView view = field.getView();
            
            UriBuilder builder = UriBuilder.fromUri(presto.getBaseUri())
                    .path("editor/validate-topic/")
                    .path(presto.getDatabaseId())
                    .path(topic.getId())
                    .path(view.getId());
            String href = builder.build().toString();
    
            Link link = new Link();
            link.setRel("onchange");
            link.setHref(href);
    
            if (!links.contains(link)) {
                links.add(link);
            }
            fieldData.setLinks(links);
        }
        return fieldData;
    }

}
