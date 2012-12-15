package net.ontopia.presto.jaxrs.process.impl;

import java.util.Collection;

import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.jaxrs.process.FieldDataProcessor;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;

public class FieldCardinalityValidator extends FieldDataProcessor {

    @Override
    public FieldData processFieldData(FieldData fieldData, PrestoTopic topic, PrestoFieldUsage field) {
        int minCardinality = field.getMinCardinality();
        int maxCardinality = field.getMaxCardinality();
        
        Collection<Value> values = fieldData.getValues();
        int cardinality = values.size();
        if (minCardinality > 0 && maxCardinality > 0) {
            if (cardinality < minCardinality || cardinality > maxCardinality) {
                setValid(false);
                if (minCardinality == maxCardinality) {
                    addError(fieldData, "Field must have exactly " + minCardinality + getValuesString(minCardinality));
                } else {
                    addError(fieldData, "Field must have between " + minCardinality + " and " + maxCardinality + getValuesString(maxCardinality));
                }
            }
        } else {
            if (cardinality < minCardinality) {
                setValid(false);
                addError(fieldData, "Field must have at least " + minCardinality + getValuesString(minCardinality));
            }
            if (cardinality > maxCardinality) {
                setValid(false);
                addError(fieldData, "Field must have no more than " + maxCardinality + getValuesString(maxCardinality));
            }
        }
        return fieldData;
    }

    protected String getValuesString(int cardinality) {
        if (cardinality == 1) {
            return " value";
        } else {
            return " values";
        }
    }

}
