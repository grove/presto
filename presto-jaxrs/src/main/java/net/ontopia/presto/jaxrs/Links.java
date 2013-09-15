package net.ontopia.presto.jaxrs;

import java.net.URI;

import javax.ws.rs.core.UriBuilder;

import net.ontopia.presto.jaxb.Link;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.utils.PrestoContext;

public class Links {
    
    private Links() {
    }

    // WARN: replacing all / characters with skull character to 
    // work around http://java.net/jira/browse/JAX_RS_SPEC-70
    private static final String SKULL_CHARACTER = "\u2620";

    public static String skull(String u) {
        // NOTE: we're only patching the ids of topics
        return u.replaceAll("/", SKULL_CHARACTER);
    }
    
    public static String deskull(String u) {
        // NOTE: we're only patching the ids of topics
        return u.replaceAll(SKULL_CHARACTER, "/");
    }
    
    public static Link createLabel(String name) {
        Link link = new Link();
        link.setName(name);
        link.setRel("label");
        return link;
    }
    
    public static String getTopicEditLinkById(URI baseUri, String databaseId) {
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/topic/").path(databaseId).path("/{topicId}");
        return builder.build().toString();
    }
    
    public static String getTopicEditLink(URI baseUri, String databaseId, String topicId, String viewId, boolean readOnly) {
        PrestoContext parentContext = null;
        PrestoField parentField = null;
        return getTopicEditLink(baseUri, databaseId, parentContext, parentField, topicId, viewId, readOnly);
    }
    
    public static String getTopicEditLink(URI baseUri, String databaseId, PrestoContext parentContext, PrestoField parentField, String topicId, String viewId, boolean readOnly) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/topic/").path(databaseId).path(path).path(skull(topicId)).path(viewId);
        if (readOnly) {
            builder = builder.queryParam("readOnly", "true");
        }
        return builder.build().toString();        
    }

    public static String getTopicViewHref(URI baseUri, String databaseId, String topicId, String viewId, boolean readOnly) {
        PrestoContext parentContext = null;
        PrestoField parentField = null;
        return getTopicViewHref(baseUri, databaseId, parentContext, parentField, topicId, viewId, readOnly);
    }
    
    public static String getTopicViewHref(URI baseUri, String databaseId, PrestoContext parentContext, PrestoField parentField, String topicId, String viewId, boolean readOnly) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/topic-view/").path(databaseId).path(path).path(skull(topicId)).path(viewId);
        if (readOnly) {
            builder = builder.queryParam("readOnly", "true");
        }
        return builder.build().toString();        
    }
    
    public static String createNewTopicViewLink(URI baseUri, String databaseId, String typeId, String viewId) {
        PrestoContext parentContext = null;
        PrestoField parentField = null;
        return createNewTopicViewLink(baseUri, databaseId, parentContext, parentField, typeId, viewId);
    }
    
    public static String createNewTopicViewLink(URI baseUri, String databaseId, PrestoContext parentContext, PrestoField parentField, String typeId, String viewId) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/topic-view/").path(databaseId).path(path).path("_" + typeId).path(viewId);
        return builder.build().toString();
    }

    public static String createInstanceLink(URI baseUri, String databaseId, String typeId) {
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/create-instance/").path(databaseId).path(typeId);
        return builder.build().toString();
    }

    public static String createFieldInstanceLink(URI baseUri, String databaseId, PrestoContext parentContext, PrestoField parentField, String typeId) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/create-field-instance/").path(databaseId).path(path).path(typeId);
        return builder.build().toString();
    }

    public static String addFieldValuesLink(URI baseUri, String databaseId, String topicId, String parentViewId, String fieldId, boolean index) {
        PrestoContext parentContext = null;
        PrestoField parentField = null;
        return addFieldValuesLink(baseUri, databaseId, parentContext, parentField, topicId, parentViewId, fieldId, index);
    }

    public static String addFieldValuesLink(URI baseUri, String databaseId, PrestoContext parentContext, PrestoField parentField, String topicId, String parentViewId, String fieldId, boolean index) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/add-field-values/").path(databaseId).path(path).path(skull(topicId)).path(parentViewId).path(fieldId);
        if (index) {
            builder = builder.queryParam("index", "{index}");
        }
        return builder.build().toString();
    }

    public static String removeFieldValuesLink(URI baseUri, String databaseId, String topicId, String parentViewId, String fieldId) {
        PrestoContext parentContext = null;
        PrestoField parentField = null;
        return removeFieldValuesLink(baseUri, databaseId, parentContext, parentField, topicId, parentViewId, fieldId);
    }

    public static String removeFieldValuesLink(URI baseUri, String databaseId, PrestoContext parentContext, PrestoField parentField, String topicId, String parentViewId, String fieldId) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/remove-field-values/").path(databaseId).path(path).path(skull(topicId)).path(parentViewId).path(fieldId);
        return builder.build().toString();
    }

    public static String moveFieldValuesToIndex(URI baseUri, String databaseId, String topicId, String parentViewId, String fieldId) {
        PrestoContext parentContext = null;
        PrestoField parentField = null;
        return moveFieldValuesToIndex(baseUri, databaseId, parentContext, parentField, topicId, parentViewId, fieldId);
    }

    public static String moveFieldValuesToIndex(URI baseUri, String databaseId, PrestoContext parentContext, PrestoField parentField, String topicId, String parentViewId, String fieldId) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/move-field-values-to-index/").path(databaseId).path(path).path(skull(topicId)).path(parentViewId).path(fieldId).queryParam("index", "{index}");
        return builder.build().toString();
    }

    public static String pagingLink(URI baseUri, String databaseId, String topicId, String parentViewId, String fieldId) {
        PrestoContext parentContext = null;
        PrestoField parentField = null;
        return pagingLink(baseUri, databaseId, parentContext, parentField, topicId, parentViewId, fieldId);
    }

    public static String pagingLink(URI baseUri, String databaseId, PrestoContext parentContext, PrestoField parentField, String topicId, String parentViewId, String fieldId) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/paging-field/").path(databaseId).path(path).path(skull(topicId)).path(parentViewId).path(fieldId).path("{start}").path("{limit}");
        return builder.build().toString();
    }

    public static String availableFieldValues(URI baseUri, String databaseId, String topicId, String parentViewId, String fieldId, boolean query) {
        PrestoContext parentContext = null;
        PrestoField parentField = null;
        return availableFieldValues(baseUri, databaseId, parentContext, parentField, topicId, parentViewId, fieldId, query);
    }

    public static String availableFieldValues(URI baseUri, String databaseId, PrestoContext parentContext, PrestoField parentField, String topicId, String parentViewId, String fieldId, boolean query) {
        String path = PathParser.getInlineTopicPath(parentContext, parentField);
        UriBuilder builder = UriBuilder.fromUri(baseUri);
        builder = builder.path("editor/available-field-values/").path(databaseId).path(path).path(skull(topicId)).path(parentViewId).path(fieldId);
        if (query) {
            builder = builder.queryParam("query", "{query}");
        }
        return builder.build().toString();
    }

    @Deprecated
    public static String availableTypesTreeLazy(URI baseUri, String databaseId, String typeId) {
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/available-types-tree-lazy/").path(databaseId).path(typeId);
        return builder.build().toString();
    }

    @Deprecated
    public static String getAvailableTypesTree(URI baseUri, String databaseId) {
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/available-types-tree/").path(databaseId);
        return builder.build().toString();
    }
    
}
