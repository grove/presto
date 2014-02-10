package net.ontopia.presto.jaxrs;

import net.ontopia.presto.jaxb.Link;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView;
import net.ontopia.presto.spi.utils.PrestoContext;

public interface Links {

    public abstract Link topicEditByIdLink();

    public abstract Link topicEditLink(String topicId, PrestoType type,
            PrestoView view, boolean readOnly);

    public abstract Link topicEditInlineLink(PrestoContext parentContext,
            PrestoField parentField, String topicId, PrestoType type,
            PrestoView view, boolean readOnly);

    public abstract Link topicViewUpdateLink(PrestoContext parentContext,
            PrestoField parentField, String topicId, PrestoType type,
            PrestoView view);

    public abstract Link topicViewDeleteLink(PrestoContext parentContext,
            PrestoField parentField, String topicId, PrestoType type,
            PrestoView view);

    public abstract String topicViewHref(PrestoContext parentContext,
            PrestoField parentField, String topicId, PrestoType type,
            PrestoView view, boolean readOnly);

    public abstract Link fieldOnChangeLink(PrestoContext parentContext,
            PrestoField parentField, String topicId, PrestoType type,
            PrestoView view, PrestoField field);

    public abstract Link topicViewCreateLink(PrestoType type, PrestoView view);

    public abstract Link topicViewCreateInlineLink(PrestoContext parentContext,
            PrestoField parentField, PrestoType type, PrestoView view);

    public abstract Link topicTemplateLink(PrestoType type);

    public abstract String topicTemplateHref(PrestoType type);

    public abstract Link topicTemplateFieldLink(PrestoContext parentContext,
            PrestoField parentField, PrestoType type);

    public abstract String topicTemplateFieldHref(PrestoContext parentContext,
            PrestoField parentField, PrestoType type);

    public abstract Link fieldAddValuesLink(PrestoContext parentContext,
            PrestoField parentField, String topicId, PrestoType type, PrestoView view,
            PrestoField field);

    public abstract Link fieldAddValuesAtIndexLink(PrestoContext parentContext,
            PrestoField parentField, String topicId, PrestoType type, PrestoView view,
            PrestoField field);

    public abstract Link fieldRemoveValuesLink(PrestoContext parentContext,
            PrestoField parentField, String topicId, PrestoType type, PrestoView view,
            PrestoField field);

    public abstract Link fieldMoveValuesToIndexLink(
            PrestoContext parentContext, PrestoField parentField,
            String topicId, PrestoType type, PrestoView view, PrestoField field);

    public abstract Link fieldPagingLink(PrestoContext parentContext,
            PrestoField parentField, String topicId, PrestoType type, PrestoView view,
            PrestoField field);

    public abstract Link fieldAvailableValuesLink(PrestoContext parentContext,
            PrestoField parentField, String topicId, PrestoType type, PrestoView view,
            PrestoField field, boolean query);

    public abstract String topicViewExternalHref(PrestoContext context);

}