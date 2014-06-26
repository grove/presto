package net.ontopia.presto.jaxrs.process.impl;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.ws.rs.core.UriBuilder;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Link;
import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.jaxrs.Presto;
import net.ontopia.presto.jaxrs.Presto.FieldDataValues;
import net.ontopia.presto.jaxrs.PrestoProcessor;
import net.ontopia.presto.jaxrs.process.FieldDataProcessor;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoTopic.Projection;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules;

public class ValueFieldsPostProcessor extends FieldDataProcessor {

    @Override
    public FieldData processFieldData(FieldData fieldData, PrestoContextRules rules, PrestoField field) {
        
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

        // assign column value fields
        List<FieldData> valueFields = new ArrayList<FieldData>();
        for (PrestoField valueField : fields) {
            if (!subrules.isHiddenField(valueField)) {
                FieldData fd = presto.getFieldDataNoValues(rules, valueField);
                
                // process the value field
                fd = processor.processFieldData(fd, rules, valueField, getType(), getStatus());
    
                // add column sorting links
                String sortAscUrl = UriBuilder.fromUri(baseUri).path("editor").toString();
                Link link = new Link("sort-asc", sortAscUrl);
                
                valueFields.add(fd);
            }
        }
        fieldData.setValueFields(valueFields);
 
        // retrieve field values

        // NOTE: have already done paging
        FieldDataValues fieldDataValues = setFieldDataValues(rules, field, fieldData);
        
        // post process field values
        int size = fieldDataValues.size();
        
        Collection<Value> newValues = new ArrayList<Value>(size);
        for (int i=0; i < size; i++) {
            Object inputValue = fieldDataValues.getInputValue(i);
            Value outputValue = fieldDataValues.getOutputValue(i);
            newValues.add(postProcessValue(rules, field, inputValue, outputValue, fields));
        }
        fieldData.setValues(newValues);
        
        return fieldData;
    }

    private FieldDataValues setFieldDataValues(PrestoContextRules rules, PrestoField field, FieldData fieldData) {
        Projection projection = null;
        return getPresto().setFieldDataValues(rules, field, projection, fieldData);
    }
    
    private Value postProcessValue(PrestoContextRules rules, PrestoField field, Object inputValue, Value outputValue, List<PrestoField> fields) {

        if (field.isReferenceField()) {
            List<Value> values = new ArrayList<Value>();
            PrestoTopic valueTopic = (PrestoTopic)inputValue;
            PrestoContext context = rules.getContext();
            PrestoContext subcontext = PrestoContext.createSubContext(getDataProvider(), getSchemaProvider(), context, field, valueTopic);
            PrestoContextRules subrules = getPresto().getPrestoContextRules(subcontext);
            
            for (PrestoField valueField : fields) {
                FieldDataValues fieldDataValues = setFieldDataValues(subrules, valueField, null);
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
