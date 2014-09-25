package net.ontopia.presto.jaxrs.constraints;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.utils.PrestoContext;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class NotMovableValueConstraintException extends DefaultConstraintException {

    private Object movableValue;

    public NotMovableValueConstraintException(PrestoContext context, PrestoField field, Object movableValue) {
        super(context, field);
        this.movableValue = movableValue;
    }

    @Override
    protected String[] getMessageKeys() {
        return new String[] { "not-movable-value" };
    }

    protected ObjectNode[] getExtraNodes() {
        if (movableValue instanceof PrestoTopic) {
            PrestoTopic movableTopic = (PrestoTopic)movableValue;
            PrestoType movableType = schemaProvider.getTypeById(movableTopic.getTypeId());
            return new ObjectNode[] {
                    field == null ? null : (ObjectNode)field.getExtra(),
                    (ObjectNode)movableType.getExtra(),
                    context == null ? null : (ObjectNode)context.getView().getExtra(),
                    context == null ? null : (ObjectNode)context.getType().getExtra(),
                    (ObjectNode)schemaProvider.getExtra()
            };
            
        }
        return super.getExtraNodes();
    }

}
