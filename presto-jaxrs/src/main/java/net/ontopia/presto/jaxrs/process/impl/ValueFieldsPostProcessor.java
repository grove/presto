package net.ontopia.presto.jaxrs.process.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.jaxrs.Presto;
import net.ontopia.presto.jaxrs.Presto.FieldDataValues;
import net.ontopia.presto.jaxrs.PrestoContext;
import net.ontopia.presto.jaxrs.process.FieldDataProcessor;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;

public class ValueFieldsPostProcessor extends FieldDataProcessor {

    @Override
    public FieldData processFieldData(FieldData fieldData, PrestoContext context, PrestoFieldUsage field) {
        
        // NOTE: use first value type
        Collection<PrestoType> availableFieldValueTypes = field.getAvailableFieldValueTypes();
        if (availableFieldValueTypes.isEmpty()) {
            throw new RuntimeException("No availableFieldValueTypes for field '" + field.getId() + "'");
        }
        PrestoType type = availableFieldValueTypes.iterator().next();
        
        List<PrestoFieldUsage> fields = type.getFields(field.getValueView());

        // assign column value fields
        List<FieldData> valueFields = new ArrayList<FieldData>();
        for (PrestoFieldUsage valueField : fields) {
            FieldData fd = getPresto().getFieldDataNoValues(context, valueField);
            
            // process the value field
            fd = getPresto().getProcessor().processFieldData(fd, context, valueField, getType(), getStatus());

            valueFields.add(fd);
        }
        fieldData.setValueFields(valueFields);
 
        // retrieve field values

        // NOTE: have already done paging
        FieldDataValues fieldDataValues = setFieldDataValues(context, field, fieldData);
        
        // post process field values
        int size = fieldDataValues.size();
        
        Collection<Value> newValues = new ArrayList<Value>(size);
        for (int i=0; i < size; i++) {
            Object inputValue = fieldDataValues.getInputValue(i);
            Value outputValue = fieldDataValues.getOutputValue(i);
            newValues.add(postProcessValue(context, field, inputValue, outputValue, fields));
        }
        fieldData.setValues(newValues);
        
        return fieldData;
    }

    private FieldDataValues setFieldDataValues(PrestoContext context, PrestoFieldUsage field, FieldData fieldData) {
        int offset = 0;
        int limit = Presto.DEFAULT_LIMIT;
        return getPresto().setFieldDataValues(offset, limit, context, field, fieldData);
    }
    
    private Value postProcessValue(PrestoContext context, PrestoFieldUsage field, Object inputValue, Value outputValue, List<PrestoFieldUsage> fields) {

        if (field.isReferenceField()) {
            List<Value> values = new ArrayList<Value>();
            PrestoTopic valueTopic = (PrestoTopic)inputValue;
            PrestoContext subcontext = PrestoContext.create(getPresto(), valueTopic, context.isReadOnly());
            
            for (PrestoFieldUsage valueField : fields) {
                FieldDataValues fieldDataValues = setFieldDataValues(subcontext, valueField, null);
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
