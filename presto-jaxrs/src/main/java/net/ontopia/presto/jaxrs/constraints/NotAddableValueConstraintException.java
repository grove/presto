package net.ontopia.presto.jaxrs.constraints;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.utils.PrestoContext;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class NotAddableValueConstraintException extends DefaultConstraintException {

    private Object addableValue;

    public NotAddableValueConstraintException(PrestoContext context, PrestoField field, Object addableValue) {
        super(context, field);
        this.addableValue = addableValue;
    }

    @Override
    protected String[] getMessageKeys() {
        return new String[] { "not-addable-value" };
    }

    protected ObjectNode[] getExtraNodes() {
        if (addableValue instanceof PrestoTopic) {
            PrestoTopic addableTopic = (PrestoTopic)addableValue;
            PrestoType addableType = schemaProvider.getTypeById(addableTopic.getTypeId());
            return new ObjectNode[] {
                    field == null ? null : (ObjectNode)field.getExtra(),
                    (ObjectNode)addableType.getExtra(),
                    context == null ? null : (ObjectNode)context.getView().getExtra(),
                    context == null ? null : (ObjectNode)context.getType().getExtra(),
                    (ObjectNode)schemaProvider.getExtra()
            };
            
        }
        return super.getExtraNodes();
    }

}
