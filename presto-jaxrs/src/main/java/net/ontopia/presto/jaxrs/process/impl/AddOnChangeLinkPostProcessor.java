package net.ontopia.presto.jaxrs.process.impl;

import java.util.Collection;
import java.util.LinkedHashSet;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Link;
import net.ontopia.presto.jaxrs.Links;
import net.ontopia.presto.jaxrs.Presto;
import net.ontopia.presto.jaxrs.process.FieldDataProcessor;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class AddOnChangeLinkPostProcessor extends FieldDataProcessor {

    @Override
    public FieldData processFieldData(FieldData fieldData, PrestoContextRules rules, PrestoFieldUsage field) {
        
        ObjectNode processorConfig = getConfig();
        if (processorConfig != null) {
            JsonNode validateNode = processorConfig.path("validateOnChange");
            if (validateNode.isBoolean() && validateNode.asBoolean()) {
                return addOnChangeLink(fieldData, rules, field);
            }
        }
        
        return fieldData;
    }

    private FieldData addOnChangeLink(FieldData fieldData, PrestoContextRules rules, PrestoFieldUsage field) {
        // TODO: may want to support this for inline *new* topics as well
        PrestoContext context = rules.getContext();
        PrestoTopic topic = context.getTopic();
        if (topic != null) {
            Collection<Link> links = fieldData.getLinks();
            if (links == null) {
                links = new LinkedHashSet<Link>();
            }
            
            Presto presto = getPresto();
            
            String topicId = topic.getId();
            String typeId = topic.getTypeId();
            PrestoType type = getSchemaProvider().getTypeById(typeId);
            PrestoView view = field.getView();
            PrestoContext parentContext = context.getParentContext();
            PrestoFieldUsage parentField = context.getParentField();
            
            Links lx = presto.getLinks();
            String href = lx.topicValidateLink(parentContext, parentField, topicId, type, view);
    
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
