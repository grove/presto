package net.ontopia.presto.jaxrs.links;

import java.net.URI;

import javax.ws.rs.core.UriBuilder;

import net.ontopia.presto.jaxb.Link;
import net.ontopia.presto.jaxrs.PathParser;
import net.ontopia.presto.jaxrs.Presto;
import net.ontopia.presto.jaxrs.Presto.Rel;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView;
import net.ontopia.presto.spi.utils.ExtraUtils;
import net.ontopia.presto.spi.utils.PatternValueUtils;
import net.ontopia.presto.spi.utils.PrestoContext;

import org.codehaus.jackson.node.ObjectNode;

public class DefaultLinks implements Links {
    
    private final URI baseUri;
    private final String databaseId;

    public DefaultLinks(URI baseUri, String databaseId) {
        this.baseUri = baseUri;
        this.databaseId = databaseId;
    }

    protected URI getBaseUri() {
        return baseUri;
    }
    
    protected String getDatabaseId() {
        return databaseId;
    }
    
    @Override
    public Link topicEditByIdLink() {
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("topic").path(databaseId).path("{topicId}");
        String href = builder.build().toString();
        return new Link(Rel.REL_TOPIC_EDIT_BY_ID.getRel(), href);
    }
    
    @Override
    public Link topicEditLink(String topicId, PrestoType type, PrestoView view, boolean readOnly) {
        PrestoContext parentContext = null;
        PrestoField parentField = null;
        String href = topicViewEditHref(parentContext, parentField, topicId, type, view, readOnly);
        return new Link(Rel.REL_TOPIC_EDIT.getRel(), href);
    }

    @Override
    public Link topicEditInlineLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view, boolean readOnly) {
        String href = topicViewEditHref(parentContext, parentField, topicId, type, view, readOnly);
        return new Link(Rel.REL_TOPIC_EDIT.getRel(), href);
    }
    
    protected String topicViewEditHref(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view, boolean readOnly) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        String viewId = view.getId();
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("topic").path(databaseId).path(path).path(PathParser.skull(topicId)).path(viewId);
        if (readOnly) {
            builder = builder.queryParam("readOnly", "true");
        }
        return builder.build().toString();        
    }

    @Override
    public String topicViewExternalHref(PrestoContext context) {
        PrestoType type = context.getType();
        PrestoView view = context.getView();
        String pattern = ExtraUtils.getExtraParamsStringValue((ObjectNode)view.getExtra(), "href");
        return PatternValueUtils.getValueByPattern(type.getSchemaProvider(), context, pattern);
    }

    @Override
    public Link topicViewUpdateLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view) {
        String href = topicViewHref(parentContext, parentField, topicId, type, view, false);
        return new Link(Rel.REL_TOPIC_UPDATE.getRel(), href);
    }
    
    @Override
    public Link topicViewDeleteLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view) {
        String href = topicViewHref(parentContext, parentField, topicId, type, view, false);
        return new Link(Rel.REL_TOPIC_DELETE.getRel(), href);
    }
    
    @Override
    public String topicViewHref(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view, boolean readOnly) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        String viewId = view.getId();
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("topic-view").path(databaseId).path(path).path(PathParser.skull(topicId)).path(viewId);
        if (readOnly) {
            builder = builder.queryParam("readOnly", "true");
        }
        return builder.build().toString();        
    }
    
    @Override
    public Link fieldOnChangeLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view, PrestoField field) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        String viewId = view.getId();
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("validate-topic").path(databaseId).path(path).path(PathParser.skull(topicId)).path(viewId);
        String href = builder.build().toString();
        return new Link(Presto.Rel.REL_ONCHANGE.getRel(), href);
    }

    @Override
    public Link topicViewCreateLink(PrestoType type, PrestoView view) {
        PrestoContext parentContext = null;
        PrestoField parentField = null;
        String href = topicViewCreateHref(parentContext, parentField, type, view);
        return new Link(Rel.REL_TOPIC_CREATE.getRel(), href);
    }

    @Override
    public Link topicViewCreateInlineLink(PrestoContext parentContext, PrestoField parentField, PrestoType type, PrestoView view) {
        String href = topicViewCreateHref(parentContext, parentField, type, view);
        return new Link(Rel.REL_TOPIC_CREATE.getRel(), href);
    }
    
    protected String topicViewCreateHref(PrestoContext parentContext, PrestoField parentField, PrestoType type, PrestoView view) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        String typeId = type.getId();
        String viewId = view.getId();
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("topic-view").path(databaseId).path(path).path("_" + typeId).path(viewId);
        return builder.build().toString();
    }

    @Override
    public Link topicTemplateLink(PrestoType type) {
        String href = topicTemplateHref(type);
        return new Link(Rel.REL_TOPIC_TEMPLATE.getRel(), href);
    }

    @Override
    public String topicTemplateHref(PrestoType type) {
        String typeId = type.getId();
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("topic-template").path(databaseId).path(typeId);
        return builder.build().toString();
    }

    @Override
    public Link topicTemplateFieldLink(PrestoContext parentContext, PrestoField parentField, PrestoType type) {
        String href = topicTemplateFieldHref(parentContext, parentField, type);
        return new Link(Rel.REL_TOPIC_TEMPLATE_FIELD.getRel(), href);
    }

    @Override
    public String topicTemplateFieldHref(PrestoContext parentContext, PrestoField parentField, PrestoType type) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        String typeId = type.getId();
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("topic-template-field").path(databaseId).path(path).path(typeId);
        return builder.build().toString();
    }

    @Override
    public Link fieldAddValuesLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view, PrestoField field) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        String viewId = view.getId();
        String fieldId = field.getId();
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("add-field-values").path(databaseId).path(path).path(PathParser.skull(topicId)).path(viewId).path(fieldId);
        String href = builder.build().toString();
        String rel = Rel.REL_ADD_FIELD_VALUES.getRel();
        return new Link(rel, href);
    }

    @Override
    public Link fieldAddValuesAtIndexLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view, PrestoField field) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        String viewId = view.getId();
        String fieldId = field.getId();
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("add-field-values").path(databaseId).path(path).path(PathParser.skull(topicId)).path(viewId).path(fieldId);
        builder = builder.queryParam("index", "{index}");
        String href = builder.build().toString();
        String rel = Rel.REL_ADD_FIELD_VALUES_AT_INDEX.getRel();
        return new Link(rel, href);
    }

    @Override
    public Link fieldRemoveValuesLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view, PrestoField field) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        String viewId = view.getId();
        String fieldId = field.getId();
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("remove-field-values").path(databaseId).path(path).path(PathParser.skull(topicId)).path(viewId).path(fieldId);
        String href = builder.build().toString();
        return new Link(Rel.REL_REMOVE_FIELD_VALUES.getRel(), href);
    }

    @Override
    public Link fieldMoveValuesToIndexLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view, PrestoField field) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        String viewId = view.getId();
        String fieldId = field.getId();
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("move-field-values-to-index").path(databaseId).path(path).path(PathParser.skull(topicId)).path(viewId).path(fieldId).queryParam("index", "{index}");
        String href = builder.build().toString();
        return new Link(Rel.REL_MOVE_FIELD_VALUES_TO_INDEX.getRel(), href);
    }

    @Override
    public Link fieldPagingLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view, PrestoField field) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        String viewId = view.getId();
        String fieldId = field.getId();
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("paging-field").path(databaseId).path(path).path(PathParser.skull(topicId)).path(viewId).path(fieldId).path("{start}").path("{limit}");
        String href = builder.build().toString();
        return new Link(Rel.REL_FIELD_PAGING.getRel(), href);
    }

    @Override
    public Link fieldAvailableValuesLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view, PrestoField field, boolean query) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        UriBuilder builder = UriBuilder.fromUri(baseUri);
        String viewId = view.getId();
        String fieldId = field.getId();
        builder = builder.path("editor").path("available-field-values").path(databaseId).path(path).path(PathParser.skull(topicId)).path(viewId).path(fieldId);
        if (query) {
            builder = builder.queryParam("query", "{query}");
        }
        String href = builder.build().toString();
        return new Link(Rel.REL_AVAILABLE_FIELD_VALUES.getRel(), href);
    }
    
}
