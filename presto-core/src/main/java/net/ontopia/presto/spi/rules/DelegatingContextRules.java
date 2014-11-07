package net.ontopia.presto.spi.rules;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoView;
import net.ontopia.presto.spi.utils.AbstractHandler;
import net.ontopia.presto.spi.utils.ExtraUtils;
import net.ontopia.presto.spi.utils.PrestoContextRules;
import net.ontopia.presto.spi.utils.PrestoContextRules.ContextRulesHandler;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldFlag;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldRule;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldValueFlag;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldValueRule;
import net.ontopia.presto.spi.utils.PrestoContextRules.TypeFlag;
import net.ontopia.presto.spi.utils.PrestoContextRules.TypeRule;
import net.ontopia.presto.spi.utils.PrestoContextRules.ViewFlag;
import net.ontopia.presto.spi.utils.PrestoContextRules.ViewRule;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class DelegatingContextRules extends ContextRulesHandler {

    @Override
    public Boolean getValue(TypeFlag flag, PrestoContextRules rules) {
        JsonNode flagNode = getFlagNode(flag, rules);
        if (flagNode != null && !flagNode.isMissingNode()) {
            for (TypeRule handler : AbstractHandler.getHandlers(getDataProvider(), getSchemaProvider(), TypeRule.class, flagNode)) {
                Boolean result = handler.getValue(flag, rules);
                if (result != null) {
                    return result;
                }
            }
        }
        return null;
    }

    @Override
    public Boolean getValue(ViewFlag flag, PrestoContextRules rules, PrestoView view) {
        JsonNode flagNode = getFlagNode(flag, rules, view);
        if (flagNode != null && !flagNode.isMissingNode()) {
            for (ViewRule handler : AbstractHandler.getHandlers(getDataProvider(), getSchemaProvider(), ViewRule.class, flagNode)) {
                Boolean result = handler.getValue(flag, rules, view);
                if (result != null) {
                    return result;
                }
            }
        }
        return null;
    }

    @Override
    public Boolean getValue(FieldFlag flag, PrestoContextRules rules, PrestoField field) {
        JsonNode flagNode = getFlagNode(flag, rules, field);
        if (flagNode != null && !flagNode.isMissingNode()) {
            for (FieldRule handler : AbstractHandler.getHandlers(getDataProvider(), getSchemaProvider(), FieldRule.class, flagNode)) {
                Boolean result = handler.getValue(flag, rules, field);
                if (result != null) {
                    return result;
                }
            }
        }
        return null;
    }

    @Override
    public Boolean getValue(FieldValueFlag flag, PrestoContextRules rules, PrestoField field, Object value) {
        JsonNode flagNode = getFlagNode(flag, rules, field, value);
        if (flagNode != null && !flagNode.isMissingNode()) {
            for (FieldValueRule handler : AbstractHandler.getHandlers(getDataProvider(), getSchemaProvider(), FieldValueRule.class, flagNode)) {
                Boolean result = handler.getValue(flag, rules, field, value);
                if (result != null) {
                    return result;
                }
            }
        }
        return null;
    }

    protected JsonNode getFlagNode(TypeFlag flag, PrestoContextRules rules) {
        ObjectNode config = getConfig();
        if (config != null) {
            return config.path(flag.name());
        }
        return null;
    }

    protected JsonNode getFlagNode(ViewFlag flag, PrestoContextRules rules, PrestoView view) {
        ObjectNode extra = ExtraUtils.getViewExtraNode(view); // getConfig();
        if (extra != null) {
            return extra.path("viewRules").path(flag.name());
        }
        return null;
    }

    protected JsonNode getFlagNode(FieldFlag flag, PrestoContextRules rules, PrestoField field) {
        // get config from field extra
        ObjectNode extra = (ObjectNode)field.getExtra();
        if (extra != null) {
            return extra.path("fieldRules").path(flag.name());
        }
        return null;
    }

    protected JsonNode getFlagNode(FieldValueFlag flag, PrestoContextRules rules, PrestoField field, Object value) {
        // get config from field extra
        ObjectNode extra = (ObjectNode)field.getExtra();
        if (extra != null) {
            return extra.path("fieldValueRules").path(flag.name());
        }
        return null;
    }
    
}