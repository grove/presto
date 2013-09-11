package net.ontopia.presto.jaxrs;

import net.ontopia.presto.jaxrs.rules.DelegatingContextRules;
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

    public enum FieldValueFlag {
        isAddableFieldValue,
        isRemovableFieldValue,
        isEditableFieldValue,
        isIgnorableFieldValue
    }

    public static interface TypeRule extends Handler {

        public Boolean getValue(TypeFlag flag, PrestoContext context);

    }

    public static interface FieldRule extends Handler {

        public Boolean getValue(FieldFlag flag, PrestoContext context, PrestoField field);

    }

    public static interface FieldValueRule extends Handler {

        public Boolean getValue(FieldValueFlag flag, PrestoContext context, PrestoField field, Object value);

    }

    public static abstract class ContextRulesHandler extends AbstractHandler implements TypeRule, FieldRule, FieldValueRule {
    }

    private ContextRulesHandler handler;
    private boolean readOnlyType;

    private PrestoContext context;
    private PrestoType type;

    public PrestoContextRules(Presto session, PrestoContext context) {
        this.context = context;
        this.type = context.getType();

        ObjectNode config = null;

        ObjectNode extra = (ObjectNode)type.getExtra();
        if (extra != null) {
            JsonNode contextRules = extra.path("contextRules");
            if (contextRules.isObject()) {
                this.handler = PrestoProcessor.getHandler(session, ContextRulesHandler.class, contextRules);
                config = (ObjectNode)contextRules;
            }
        }
        if (this.handler == null) {
            this.handler = new DelegatingContextRules();
            this.handler.setPresto(session);
            this.handler.setConfig(config);
        }
        this.readOnlyType = isTypeHandlerFlag(TypeFlag.isReadOnlyType, false);
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

    private boolean isFieldValueHandlerFlag(FieldValueFlag flag, PrestoField field, Object value, boolean defaultValue) {
        if (handler != null) {
            Boolean result = handler.getValue(flag, context, field, value);
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

    public boolean isAddableFieldValue(PrestoField field, Object value) {
        return isFieldValueHandlerFlag(FieldValueFlag.isAddableFieldValue, field, value, field.isAddable());
    }

    public boolean isRemovableFieldValue(PrestoField field, Object value) {
        return isFieldValueHandlerFlag(FieldValueFlag.isRemovableFieldValue, field, value, field.isRemovable());
    }

    public boolean isEditableFieldValue(PrestoField field, Object value) {
        return isFieldValueHandlerFlag(FieldValueFlag.isEditableFieldValue, field, value, field.isEditable());
    }

    public boolean isIgnorableFieldValue(PrestoField field, Object value) {
        return isFieldValueHandlerFlag(FieldValueFlag.isIgnorableFieldValue, field, value, false);
    }

    //    public boolean isCascadingDeleteField(PrestoField field) {
    //        return field.isCascadingDelete();
    //    }

}
