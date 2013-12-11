package net.ontopia.presto.jaxrs;

import java.net.URI;

import javax.ws.rs.core.UriBuilder;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView;
import net.ontopia.presto.spi.utils.PrestoContext;

public class Links {
    
    private final URI baseUri;
    private final String databaseId;

    public Links(URI baseUri, String databaseId) {
        this.baseUri = baseUri;
        this.databaseId = databaseId;
    }

    protected URI getBaseUri() {
        return baseUri;
    }
    
    protected String getDatabaseId() {
        return databaseId;
    }
    
    public String topicLinkById() {
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("topic").path(databaseId).path("{topicId}");
        return builder.build().toString();
    }
    
    public String topicLink(String topicId, PrestoType type, PrestoView view, boolean readOnly) {
        PrestoContext parentContext = null;
        PrestoField parentField = null;
        return topicLink(parentContext, parentField, topicId, type, view, readOnly);
    }
    
    public String topicLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view, boolean readOnly) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        String viewId = view.getId();
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("topic").path(databaseId).path(path).path(PathParser.skull(topicId)).path(viewId);
        if (readOnly) {
            builder = builder.queryParam("readOnly", "true");
        }
        return builder.build().toString();        
    }

    public String topicViewLink(String topicId, PrestoType type, PrestoView view, boolean readOnly) {
        PrestoContext parentContext = null;
        PrestoField parentField = null;
        return topicViewLink(parentContext, parentField, topicId, type, view, readOnly);
    }
    
    public String topicViewLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view, boolean readOnly) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        String viewId = view.getId();
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("topic-view").path(databaseId).path(path).path(PathParser.skull(topicId)).path(viewId);
        if (readOnly) {
            builder = builder.queryParam("readOnly", "true");
        }
        return builder.build().toString();        
    }
    
    public String topicValidateLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        String viewId = view.getId();
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("validate-topic").path(databaseId).path(path).path(PathParser.skull(topicId)).path(viewId);
        return builder.build().toString();        
    }

    public String createTopicLink(PrestoType type, PrestoView view) {
        PrestoContext parentContext = null;
        PrestoField parentField = null;
        return createTopicLink(parentContext, parentField, type, view);
    }
    
    public String createTopicLink(PrestoContext parentContext, PrestoField parentField, PrestoType type, PrestoView view) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        String typeId = type.getId();
        String viewId = view.getId();
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("topic-view").path(databaseId).path(path).path("_" + typeId).path(viewId);
        return builder.build().toString();
    }

    public String topicTemplate(PrestoType type) {
        String typeId = type.getId();
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("topic-template").path(databaseId).path(typeId);
        return builder.build().toString();
    }

    public String topicTemplateField(PrestoContext parentContext, PrestoField parentField, PrestoType type) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        String typeId = type.getId();
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("topic-template-field").path(databaseId).path(path).path(typeId);
        return builder.build().toString();
    }

//    public String addFieldValuesLink(String topicId, PrestoView parentView, PrestoField field, boolean index) {
//        PrestoContext parentContext = null;
//        PrestoField parentField = null;
//        return addFieldValuesLink(parentContext, parentField, topicId, parentView, field, index);
//    }

    public String addFieldValuesLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoView parentView, PrestoField field, boolean index) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        String parentViewId = parentView.getId();
        String fieldId = field.getId();
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("add-field-values").path(databaseId).path(path).path(PathParser.skull(topicId)).path(parentViewId).path(fieldId);
        if (index) {
            builder = builder.queryParam("index", "{index}");
        }
        return builder.build().toString();
    }

//    public String removeFieldValuesLink(String topicId, PrestoView parentView, PrestoField field) {
//        PrestoContext parentContext = null;
//        PrestoField parentField = null;
//        return removeFieldValuesLink(parentContext, parentField, topicId, parentView, field);
//    }

    public String removeFieldValuesLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoView parentView, PrestoField field) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        String parentViewId = parentView.getId();
        String fieldId = field.getId();
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("remove-field-values").path(databaseId).path(path).path(PathParser.skull(topicId)).path(parentViewId).path(fieldId);
        return builder.build().toString();
    }

//    public String moveFieldValuesToIndexLink(String topicId, PrestoView parentView, PrestoField field) {
//        PrestoContext parentContext = null;
//        PrestoField parentField = null;
//        return moveFieldValuesToIndexLink(parentContext, parentField, topicId, parentView, field);
//    }

    public String moveFieldValuesToIndexLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoView parentView, PrestoField field) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        String parentViewId = parentView.getId();
        String fieldId = field.getId();
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("move-field-values-to-index").path(databaseId).path(path).path(PathParser.skull(topicId)).path(parentViewId).path(fieldId).queryParam("index", "{index}");
        return builder.build().toString();
    }

//    public String fieldPagingLink(String topicId, PrestoView parentView, PrestoField field) {
//        PrestoContext parentContext = null;
//        PrestoField parentField = null;
//        return pagingLink(parentContext, parentField, topicId, parentView, field);
//    }

    public String fieldPagingLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoView parentView, PrestoField field) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        String parentViewId = parentView.getId();
        String fieldId = field.getId();
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("paging-field").path(databaseId).path(path).path(PathParser.skull(topicId)).path(parentViewId).path(fieldId).path("{start}").path("{limit}");
        return builder.build().toString();
    }

//    public String availableFieldValuesLink(String topicId, PrestoView parentView, PrestoField field, boolean query) {
//        PrestoContext parentContext = null;
//        PrestoField parentField = null;
//        return availableFieldValuesLink(parentContext, parentField, topicId, parentView, field, query);
//    }

    public String availableFieldValuesLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoView parentView, PrestoField field, boolean query) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        UriBuilder builder = UriBuilder.fromUri(baseUri);
        String parentViewId = parentView.getId();
        String fieldId = field.getId();
        builder = builder.path("editor").path("available-field-values").path(databaseId).path(path).path(PathParser.skull(topicId)).path(parentViewId).path(fieldId);
        if (query) {
            builder = builder.queryParam("query", "{query}");
        }
        return builder.build().toString();
    }

    @Deprecated
    public String availableTypesTreeLazyLink(PrestoType type) {
        String typeId = type.getId();
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("available-types-tree-lazy").path(databaseId).path(typeId);
        return builder.build().toString();
    }

    @Deprecated
    public String availableTypesTreeLink() {
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("available-types-tree").path(databaseId);
        return builder.build().toString();
    }
    
}
