package net.ontopia.presto.jaxrs;

import org.codehaus.jackson.node.ObjectNode;

import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.utils.PrestoContext;

public class NotRemovableValueConstraintException extends DefaultConstraintException {

    private Object removableValue;

    public NotRemovableValueConstraintException(PrestoContext context, PrestoFieldUsage field, Object removableValue) {
        super(context, field);
        this.removableValue = removableValue;
    }

    @Override
    protected String[] getMessageKeys() {
        return new String[] { "not-removable-value" };
    }

    protected ObjectNode[] getExtraNodes() {
        if (removableValue instanceof PrestoTopic) {
            PrestoTopic removableTopic = (PrestoTopic)removableValue;
            PrestoType removeableType = schemaProvider.getTypeById(removableTopic.getTypeId());
            return new ObjectNode[] {
                    field == null ? null : (ObjectNode)field.getExtra(),
                    (ObjectNode)removeableType.getExtra(),
                    context == null ? null : (ObjectNode)context.getView().getExtra(),
                    context == null ? null : (ObjectNode)context.getType().getExtra(),
                    (ObjectNode)schemaProvider.getExtra()
            };
            
        }
        return super.getExtraNodes();
    }

}
