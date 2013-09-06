package net.ontopia.presto.jaxrs;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoType;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

public class PrestoContextRules {

    public enum TypeFlag {
        isReadOnlyType,
        isUpdatableType,
        isCreatableType,
        isRemovableType
    }

    public enum FieldFlag {
        isHiddenField,
        isTraverableField,
        isSortedField,
        isSortedAscendingField,
        isPageableField,
        isReadOnlyField,
        isEditableField,
        isCreatableField,
        isAddableField,
        isRemovableField
    }
    
    public static abstract class Handler extends AbstractHandler {
 
        public Boolean getValue(TypeFlag flag, PrestoContext context) {
            return null;
        }
        
        public Boolean getValue(FieldFlag flag, PrestoContext context, PrestoField field) {
            return null;
        }
        
    }
    
    private Handler handler;
    private boolean readOnlyType;
    
    private PrestoContext context;
    private PrestoType type;

    public PrestoContextRules(Presto session, PrestoContext context) {
        this.context = context;
        this.type = context.getType();
        
        ObjectNode extra = (ObjectNode)type.getExtra();
        if (extra != null) {
            JsonNode contextRules = extra.path("context-rules");
            if (!contextRules.isMissingNode()) {
                this.handler = PrestoProcessor.getHandler(session, Handler.class, contextRules);
                this.readOnlyType = isTypeHandlerFlag(TypeFlag.isReadOnlyType, false);
            }
        }
    }
    
    public PrestoContext getContext() {
        return context;
    }
    
    private boolean isTypeHandlerFlag(TypeFlag flag, boolean defaultValue) {
        if (handler != null) {
            Boolean result = handler.getValue(flag, context);
            if (result != null) {
                return result;
            }
        }
        return defaultValue;
    }
    
    private boolean isFieldHandlerFlag(FieldFlag flag, PrestoField field, boolean defaultValue) {
        if (handler != null) {
            Boolean result = handler.getValue(flag, context, field);
            if (result != null) {
                return result;
            }
        }
        return defaultValue;
    }
    
    public boolean isReadOnlyType() {
        return readOnlyType;
    }
    
    public boolean isUpdatableType() {
        return isTypeHandlerFlag(TypeFlag.isUpdatableType, true);
    }
    
    public boolean isCreatableType() {
        return isTypeHandlerFlag(TypeFlag.isCreatableType, type.isCreatable());
    }
    
    public boolean isRemovableType() {
        return isTypeHandlerFlag(TypeFlag.isRemovableType, type.isRemovable());
    }
    
    
    // characteristics

//    public boolean isInline(PrestoField field) {
//        return field.isInline();
//    }
//
//    public boolean isEmbedded(PrestoField field) {
//        return field.isEmbedded();
//    }

    public boolean isHiddenField(PrestoField field) {
        return isFieldHandlerFlag(FieldFlag.isHiddenField, field, field.isHidden());
    }

    public boolean isTraversableField(PrestoField field) {
        return isFieldHandlerFlag(FieldFlag.isTraverableField, field, field.isTraversable());
    }

    public boolean isSortedField(PrestoField field) {
        return isFieldHandlerFlag(FieldFlag.isSortedField, field, field.isSorted());
    }

    public boolean isSortedAscendingField(PrestoField field) {
        return isFieldHandlerFlag(FieldFlag.isSortedAscendingField, field, field.isSortedAscending());
    }

    public boolean isPageableField(PrestoField field) {
        return isFieldHandlerFlag(FieldFlag.isPageableField, field, field.isPageable());
    }

    // mutability

    public boolean isReadOnlyField(PrestoField field) {
        return isReadOnlyType() || isFieldHandlerFlag(FieldFlag.isReadOnlyField, field, field.isReadOnly());
    }
    
    public boolean isEditableField(PrestoField field) {
        return isFieldHandlerFlag(FieldFlag.isEditableField, field, field.isEditable());
    }
    
    public boolean isCreatableField(PrestoField field) {
        return isFieldHandlerFlag(FieldFlag.isCreatableField, field, field.isCreatable());
    }
    
    public boolean isAddableField(PrestoField field) {
        return isFieldHandlerFlag(FieldFlag.isAddableField, field, field.isAddable());
    }
    
    public boolean isRemovableField(PrestoField field) {
        return isFieldHandlerFlag(FieldFlag.isRemovableField, field, field.isRemovable());
    }

    public boolean isRemovableFieldValue(PrestoField field, Object value) {
        return isRemovableField(field);
    }

//    public boolean isCascadingDeleteField(PrestoField field) {
//        return field.isCascadingDelete();
//    }

}
