package net.ontopia.presto.spi.rules;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoView;
import net.ontopia.presto.spi.utils.AbstractHandler;
import net.ontopia.presto.spi.utils.ExtraUtils;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules.ContextRulesHandler;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldFlag;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldRule;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldValueFlag;
import net.ontopia.presto.spi.utils.PrestoContextRules.FieldValueRule;
import net.ontopia.presto.spi.utils.PrestoContextRules.TypeFlag;
import net.ontopia.presto.spi.utils.PrestoContextRules.TypeRule;
import net.ontopia.presto.spi.utils.PrestoContextRules.ViewFlag;
import net.ontopia.presto.spi.utils.PrestoContextRules.ViewRule;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class DelegatingContextRules extends ContextRulesHandler {

    @Override
    public Boolean getValue(TypeFlag flag, PrestoContext context) {
        JsonNode flagNode = getFlagNode(flag, context);
        if (flagNode != null && !flagNode.isMissingNode()) {
            for (TypeRule handler : AbstractHandler.getHandlers(getDataProvider(), getSchemaProvider(), TypeRule.class, flagNode)) {
                Boolean result = handler.getValue(flag, context);
                if (result != null) {
                    return result;
                }
            }
        }
        return null;
    }

    @Override
    public Boolean getValue(ViewFlag flag, PrestoContext context, PrestoView view) {
        JsonNode flagNode = getFlagNode(flag, context, view);
        if (flagNode != null && !flagNode.isMissingNode()) {
            for (ViewRule handler : AbstractHandler.getHandlers(getDataProvider(), getSchemaProvider(), ViewRule.class, flagNode)) {
                Boolean result = handler.getValue(flag, context, view);
                if (result != null) {
                    return result;
                }
            }
        }
        return null;
    }

    @Override
    public Boolean getValue(FieldFlag flag, PrestoContext context, PrestoField field) {
        JsonNode flagNode = getFlagNode(flag, context, field);
        if (flagNode != null && !flagNode.isMissingNode()) {
            for (FieldRule handler : AbstractHandler.getHandlers(getDataProvider(), getSchemaProvider(), FieldRule.class, flagNode)) {
                Boolean result = handler.getValue(flag, context, field);
                if (result != null) {
                    return result;
                }
            }
        }
        return null;
    }

    @Override
    public Boolean getValue(FieldValueFlag flag, PrestoContext context, PrestoField field, Object value) {
        JsonNode flagNode = getFlagNode(flag, context, field, value);
        if (flagNode != null && !flagNode.isMissingNode()) {
            for (FieldValueRule handler : AbstractHandler.getHandlers(getDataProvider(), getSchemaProvider(), FieldValueRule.class, flagNode)) {
                Boolean result = handler.getValue(flag, context, field, value);
                if (result != null) {
                    return result;
                }
            }
        }
        return null;
    }

    protected JsonNode getFlagNode(TypeFlag flag, PrestoContext context) {
        ObjectNode config = getConfig();
        if (config != null) {
            return config.path(flag.name());
        }
        return null;
    }

    protected JsonNode getFlagNode(ViewFlag flag, PrestoContext context, PrestoView view) {
        ObjectNode extra = ExtraUtils.getViewExtraNode(view); // getConfig();
        if (extra != null) {
            return extra.path("viewRules").path(flag.name());
        }
        return null;
    }

    protected JsonNode getFlagNode(FieldFlag flag, PrestoContext context, PrestoField field) {
        // get config from field extra
        ObjectNode extra = (ObjectNode)field.getExtra();
        if (extra != null) {
            return extra.path("fieldRules").path(flag.name());
        }
        return null;
    }

    protected JsonNode getFlagNode(FieldValueFlag flag, PrestoContext context, PrestoField field, Object value) {
        // get config from field extra
        ObjectNode extra = (ObjectNode)field.getExtra();
        if (extra != null) {
            return extra.path("fieldValueRules").path(flag.name());
        }
        return null;
    }
    
}