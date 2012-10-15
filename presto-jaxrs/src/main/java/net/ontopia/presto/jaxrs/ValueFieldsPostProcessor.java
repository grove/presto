package net.ontopia.presto.jaxrs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;

public class ValueFieldsPostProcessor extends FieldDataPostProcessor {

    @Override
    public FieldData postProcess(FieldData fieldData, PrestoTopic topic, PrestoFieldUsage field) {

        // NOTE: use first value type
        Collection<PrestoType> availableFieldValueTypes = field.getAvailableFieldValueTypes();
        PrestoType type = availableFieldValueTypes.iterator().next();
        
        List<PrestoFieldUsage> fields = type.getFields(field.getValueView());

        // assign column value fields
        List<FieldData> valueFields = new ArrayList<FieldData>();
        for (PrestoFieldUsage valueField : fields) {
            FieldData fd = new FieldData();
            fd.setId(valueField.getId());
            fd.setName(valueField.getName());
            fd.setReadOnly(valueField.isReadOnly());
            // TODO: add more fields
            valueFields.add(fd);
        }
        fieldData.setValueFields(valueFields);
 
        // retrieve field values
        boolean readOnlyMode = false; // ISSUE: Presto should know this?
        List<Value> oldValues = getPresto().setFieldDataValues(readOnlyMode, topic, field, null);
        
        // post process field values
        Collection<Value> newValues = new ArrayList<Value>(oldValues.size());
        for (Value oldValue : oldValues) {
            newValues.add(postProcessValue(oldValue, fields));
        }
        fieldData.setValues(newValues);
        
        return fieldData;
    }

    private Value postProcessValue(Value value, List<PrestoFieldUsage> fields) {

        String valueTopicId = value.getValue();
        PrestoTopic valueTopic = getDataProvider().getTopicById(valueTopicId);
        
        List<Value> values = new ArrayList<Value>();

        for (PrestoFieldUsage valueField : fields) {
            boolean readOnlyMode = false;
            List<Value> nestedValues = getPresto().setFieldDataValues(readOnlyMode, valueTopic, valueField, null);
            if (!nestedValues.isEmpty()) {
                Value v = nestedValues.iterator().next();
                values.add(v);
            }
        }

        value.setValues(values);
        
        return value;
    }

}
