package net.ontopia.presto.jaxrs.process.impl;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

import javax.ws.rs.core.UriBuilder;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Link;
import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.jaxrs.PathParser;
import net.ontopia.presto.jaxrs.Presto;
import net.ontopia.presto.jaxrs.Presto.FieldDataValues;
import net.ontopia.presto.jaxrs.PrestoProcessor;
import net.ontopia.presto.jaxrs.process.FieldDataProcessor;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoTopic.Projection;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView;
import net.ontopia.presto.spi.utils.FieldValues;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules;
import net.ontopia.presto.spi.utils.Utils;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class ValueFieldsPostProcessor extends FieldDataProcessor {

    @Override
    public FieldData processFieldData(FieldData fieldData, PrestoContextRules rules, PrestoField field, Projection projection) {
        
        // ISSUE: using first value type for now
        Collection<PrestoType> availableFieldValueTypes = field.getAvailableFieldValueTypes();
        if (availableFieldValueTypes.isEmpty()) {
            throw new RuntimeException("No availableFieldValueTypes for field '" + field.getId() + "'");
        }
        PrestoType type = availableFieldValueTypes.iterator().next();
        
        PrestoView valueView = field.getValueView(type);
        List<PrestoField> fields = type.getFields(valueView);

        Presto presto = getPresto();
        PrestoProcessor processor = presto.getProcessor();
        URI baseUri = presto.getBaseUri();
        PrestoContext context = rules.getContext();
        
        PrestoContext subcontext = PrestoContext.createSubContext(context, context.getParentField(), type, valueView);
        PrestoContextRules subrules = rules.getPrestoContextRules(subcontext);

        boolean sortableColumns = false;
        ObjectNode config = getConfig();
        if (config != null) {
            sortableColumns = config.path("sortable-columns").asBoolean(false);
        }
        // assign column value fields
        List<FieldData> valueFields = new ArrayList<FieldData>();
        for (PrestoField valueField : fields) {
            if (!subrules.isHiddenField(valueField)) {
                FieldData fd = presto.getFieldDataNoValues(rules, valueField);
                
                // process the value field
                fd = processor.processFieldData(fd, rules, valueField, projection, getType(), getStatus());
    
                // add column sorting links
                if (sortableColumns) {
                    String databaseId = presto.getDatabaseId();
                    String path = PathParser.getInlineTopicPath(context.getParentContext(), context.getParentField());
                    String topicId = context.getTopicId();
                    String viewId = context.getView().getId();
                    String fieldId = field.getId();
                    String start = "0";
                    String limit = Integer.toString(FieldValues.DEFAULT_LIMIT);
                    Collection<Link> links = fd.getLinks();
                    if (links == null) {
                        links = new LinkedHashSet<Link>();
                    }
                    String valueFieldId = valueField.getId();
                    String sortAscUrl = UriBuilder.fromUri(baseUri)
                            .path("editor").path("paging-field").path(databaseId)
                            .path(path).path(topicId).path(viewId).path(fieldId)
                            .path(start).path(limit)
                            .queryParam("orderBy", valueFieldId + " asc").queryParam("format", "topic-view").toString();
                    Link sortAscLink = new Link("sort-asc", sortAscUrl);
                    links.add(sortAscLink);
                    String sortDescUrl = UriBuilder.fromUri(baseUri)
                            .path("editor").path("paging-field").path(databaseId)
                            .path(path).path(topicId).path(viewId).path(fieldId)
                            .path(start).path(limit)
                            .queryParam("orderBy", valueFieldId + " desc").queryParam("format", "topic-view").toString();
                    Link sortDescLink = new Link("sort-desc", sortDescUrl);
                    links.add(sortDescLink);
                    fd.setLinks(links);
                }
                valueFields.add(fd);
            }
        }
        fieldData.setValueFields(valueFields);
 
        // retrieve field values
        // NOTE: have already done paging

        Collection<Value> values = fieldData.getValues();
        int size = values.size();

        Collection<Value> newValues = new ArrayList<Value>(size);
        for (Value outputValue : values) {
            newValues.add(postProcessValue(rules, field, outputValue, fields));
        }
        fieldData.setValues(newValues);
        
        return fieldData;
    }
    
    private Value postProcessValue(PrestoContextRules rules, PrestoField field, Value outputValue, List<PrestoField> fields) {

        if (field.isReferenceField()) {
            List<Value> values = new ArrayList<Value>();
            String valueId = outputValue.getValue();
            PrestoTopic valueTopic = Utils.getContextualValueTopic(rules, field, valueId);

            PrestoContext context = rules.getContext();
            PrestoContext subcontext = PrestoContext.createSubContext(context, field, valueTopic);
            PrestoContextRules subrules = getPresto().getPrestoContextRules(subcontext);
            Presto presto = getPresto();
            
            for (PrestoField valueField : fields) {
                if (rules.isHiddenField(valueField)) {
                    continue;
                }
                Projection projection = null;
                FieldDataValues fieldDataValues = presto.setFieldDataValues(subrules, valueField, projection, null);
                if  (fieldDataValues.size() > 0) {
                    Value v = fieldDataValues.getOutputValue(0);
                    values.add(v);
                } else {
                    values.add(Value.getNullValue());
                }
            }

            outputValue.setValues(values);
            
            return outputValue;
       } else {
           throw new RuntimeException("ValueFieldsProcessor can only be used with reference fields. (fieldId=" + field.getId() + ")");
       }
     }

}
