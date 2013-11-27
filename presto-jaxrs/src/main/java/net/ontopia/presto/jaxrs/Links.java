package net.ontopia.presto.jaxrs;

import java.net.URI;

import javax.ws.rs.core.UriBuilder;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.utils.PrestoContext;

public class Links {
    
    private final URI baseUri;
    private final String databaseId;

    public Links(URI baseUri, String databaseId) {
        this.baseUri = baseUri;
        this.databaseId = databaseId;
    }

    public String topicLinkById() {
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("topic").path(databaseId).path("{topicId}");
        return builder.build().toString();
    }
    
    public String topicLink(String topicId, String viewId, boolean readOnly) {
        PrestoContext parentContext = null;
        PrestoField parentField = null;
        return topicLink(parentContext, parentField, topicId, viewId, readOnly);
    }
    
    public String topicLink(PrestoContext parentContext, PrestoField parentField, String topicId, String viewId, boolean readOnly) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("topic").path(databaseId).path(path).path(PathParser.skull(topicId)).path(viewId);
        if (readOnly) {
            builder = builder.queryParam("readOnly", "true");
        }
        return builder.build().toString();        
    }

    public String topicViewLink(String topicId, String viewId, boolean readOnly) {
        PrestoContext parentContext = null;
        PrestoField parentField = null;
        return topicViewLink(parentContext, parentField, topicId, viewId, readOnly);
    }
    
    public String topicViewLink(PrestoContext parentContext, PrestoField parentField, String topicId, String viewId, boolean readOnly) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("topic-view").path(databaseId).path(path).path(PathParser.skull(topicId)).path(viewId);
        if (readOnly) {
            builder = builder.queryParam("readOnly", "true");
        }
        return builder.build().toString();        
    }
    
    public String topicValidateLink(PrestoContext parentContext, PrestoField parentField, String topicId, String viewId) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("validate-topic").path(databaseId).path(path).path(PathParser.skull(topicId)).path(viewId);
        return builder.build().toString();        
    }

    public String createNewTopicViewLink(String typeId, String viewId) {
        PrestoContext parentContext = null;
        PrestoField parentField = null;
        return createNewTopicViewLink(parentContext, parentField, typeId, viewId);
    }
    
    public String createNewTopicViewLink(PrestoContext parentContext, PrestoField parentField, String typeId, String viewId) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("topic-view").path(databaseId).path(path).path("_" + typeId).path(viewId);
        return builder.build().toString();
    }

    public String createInstanceLink(String typeId) {
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("create-instance").path(databaseId).path(typeId);
        return builder.build().toString();
    }

    public String createFieldInstanceLink(PrestoContext parentContext, PrestoField parentField, String typeId) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("create-field-instance").path(databaseId).path(path).path(typeId);
        return builder.build().toString();
    }

    public String addFieldValuesLink(String topicId, String parentViewId, String fieldId, boolean index) {
        PrestoContext parentContext = null;
        PrestoField parentField = null;
        return addFieldValuesLink(parentContext, parentField, topicId, parentViewId, fieldId, index);
    }

    public String addFieldValuesLink(PrestoContext parentContext, PrestoField parentField, String topicId, String parentViewId, String fieldId, boolean index) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("add-field-values").path(databaseId).path(path).path(PathParser.skull(topicId)).path(parentViewId).path(fieldId);
        if (index) {
            builder = builder.queryParam("index", "{index}");
        }
        return builder.build().toString();
    }

    public String removeFieldValuesLink(String topicId, String parentViewId, String fieldId) {
        PrestoContext parentContext = null;
        PrestoField parentField = null;
        return removeFieldValuesLink(parentContext, parentField, topicId, parentViewId, fieldId);
    }

    public String removeFieldValuesLink(PrestoContext parentContext, PrestoField parentField, String topicId, String parentViewId, String fieldId) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("remove-field-values").path(databaseId).path(path).path(PathParser.skull(topicId)).path(parentViewId).path(fieldId);
        return builder.build().toString();
    }

    public String moveFieldValuesToIndexLink(String topicId, String parentViewId, String fieldId) {
        PrestoContext parentContext = null;
        PrestoField parentField = null;
        return moveFieldValuesToIndexLink(parentContext, parentField, topicId, parentViewId, fieldId);
    }

    public String moveFieldValuesToIndexLink(PrestoContext parentContext, PrestoField parentField, String topicId, String parentViewId, String fieldId) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("move-field-values-to-index").path(databaseId).path(path).path(PathParser.skull(topicId)).path(parentViewId).path(fieldId).queryParam("index", "{index}");
        return builder.build().toString();
    }

    public String pagingLink(String topicId, String parentViewId, String fieldId) {
        PrestoContext parentContext = null;
        PrestoField parentField = null;
        return pagingLink(parentContext, parentField, topicId, parentViewId, fieldId);
    }

    public String pagingLink(PrestoContext parentContext, PrestoField parentField, String topicId, String parentViewId, String fieldId) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("paging-field").path(databaseId).path(path).path(PathParser.skull(topicId)).path(parentViewId).path(fieldId).path("{start}").path("{limit}");
        return builder.build().toString();
    }

    public String availableFieldValuesLink(String topicId, String parentViewId, String fieldId, boolean query) {
        PrestoContext parentContext = null;
        PrestoField parentField = null;
        return availableFieldValuesLink(parentContext, parentField, topicId, parentViewId, fieldId, query);
    }

    public String availableFieldValuesLink(PrestoContext parentContext, PrestoField parentField, String topicId, String parentViewId, String fieldId, boolean query) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        UriBuilder builder = UriBuilder.fromUri(baseUri);
        builder = builder.path("editor").path("available-field-values").path(databaseId).path(path).path(PathParser.skull(topicId)).path(parentViewId).path(fieldId);
        if (query) {
            builder = builder.queryParam("query", "{query}");
        }
        return builder.build().toString();
    }

    @Deprecated
    public String availableTypesTreeLazyLink(String typeId) {
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("available-types-tree-lazy").path(databaseId).path(typeId);
        return builder.build().toString();
    }

    @Deprecated
    public String availableTypesTreeLink() {
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor").path("available-types-tree").path(databaseId);
        return builder.build().toString();
    }
    
}
