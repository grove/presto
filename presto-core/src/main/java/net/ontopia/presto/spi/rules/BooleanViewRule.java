package net.ontopia.presto.spi.rules;

import net.ontopia.presto.spi.PrestoView;
import net.ontopia.presto.spi.utils.AbstractHandler;
import net.ontopia.presto.spi.utils.PrestoContextRules;
import net.ontopia.presto.spi.utils.PrestoContextRules.ViewFlag;
import net.ontopia.presto.spi.utils.PrestoContextRules.ViewRule;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public abstract class BooleanViewRule extends AbstractHandler implements ViewRule {

    @Override
    public Boolean getValue(ViewFlag flag, PrestoContextRules rules, PrestoView view) {
        ObjectNode config = getConfig();
        if (config != null) {
            return evaluateView(flag, rules, config, view);
        }
        return null;
    }
    
    protected boolean evaluateView(ViewFlag flag, PrestoContextRules rules, ObjectNode config, PrestoView view) {
        return inverse(getResult(flag, rules, view, config), config);
    }

    private boolean inverse(boolean value, ObjectNode config) {
        JsonNode inverseNode = config.path("inverse");
        if (inverseNode.isBoolean() && inverseNode.getBooleanValue()) {
            return !value;
        } else {
            return value;
        }
    }
    
    protected abstract boolean getResult(ViewFlag flag, PrestoContextRules rules, PrestoView view, ObjectNode config);

}