package net.ontopia.presto.jaxrs.action;

import org.codehaus.jackson.node.ObjectNode;

import net.ontopia.presto.jaxb.TopicView;
import net.ontopia.presto.jaxrs.Presto;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextRules;

public abstract class FieldAction {

    private ObjectNode config;

    private Presto presto;

    public abstract boolean isActive(PrestoContextRules rules, PrestoFieldUsage field, String actionId);
    
    public abstract TopicView executeAction(PrestoContext context, TopicView topicView, PrestoFieldUsage field, String actionId);

    public ObjectNode getConfig() {
        return config;
    }

    public void setConfig(ObjectNode config) {
        this.config = config;
    }

    public Presto getPresto() {
        return presto;
    }

    public void setPresto(Presto presto) {
        this.presto = presto;
    }
    
}
