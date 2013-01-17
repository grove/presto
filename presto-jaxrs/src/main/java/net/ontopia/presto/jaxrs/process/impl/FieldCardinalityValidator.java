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
                    addError(fieldData, getErrorMessage("cardinality-exact", field, "Field must have exactly {0} {1}", minCardinality, getValuesString(minCardinality)));
                } else {
                    addError(fieldData, getErrorMessage("cardinality-between", field, "Field must have between {0} and {1} {2}", minCardinality, maxCardinality, getValuesString(maxCardinality)));
                }
            }
        } else {
            if (minCardinality > 0 && cardinality < minCardinality) {
                setValid(false);
                addError(fieldData, getErrorMessage("cardinality-at-least", field, "Field must have at least {0} {1}", minCardinality, getValuesString(minCardinality)));
            }
            if (maxCardinality > 0 && cardinality > maxCardinality) {
                setValid(false);
                addError(fieldData, getErrorMessage("cardinality-no-more-than", field, "Field must have no more than {0} {1}", maxCardinality, getValuesString(maxCardinality)));
            }
        }
        return fieldData;
    }

    protected String getValuesString(int cardinality) {
        if (cardinality == 1) {
            return "value";
        } else {
            return "values";
        }
    }

}
