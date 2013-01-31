package net.ontopia.presto.jaxrs;

import java.net.URI;

import javax.ws.rs.core.UriBuilder;

import net.ontopia.presto.jaxb.Link;

public class Links {

    private Links() {
    }
    
    public static Link createLabel(String name) {
        Link link = new Link();
        link.setName(name);
        link.setRel("label");
        return link;
    }
    
    public static String getEditTopicById(URI baseUri, String databaseId) {
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/topic/").path(databaseId).path("/{topicId}");
        return builder.build().toString();
    }

    public static String getTopicEditLink(URI baseUri, String databaseId, String topicId, String viewId) {
        return getTopicEditLink(baseUri, databaseId, topicId, viewId, false);
    }
    
    public static String getTopicEditLink(URI baseUri, String databaseId, String topicId, String viewId, boolean readOnly) {
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/topic/").path(databaseId).path(topicId).path(viewId);
        if (readOnly) {
            builder = builder.queryParam("readOnly", "true");
        }
        return builder.build().toString();
    }
    
    public static String getTopicViewEditLink(URI baseUri, String databaseId, String topicId, String viewId) {
        return getTopicViewEditLink(baseUri, databaseId, topicId, viewId, false);
    }
    
    public static String getTopicViewEditLink(URI baseUri, String databaseId, String topicId, String viewId, boolean readOnly) {
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/topic-view/").path(databaseId).path(topicId).path(viewId);
        if (readOnly) {
            builder = builder.queryParam("readOnly", "true");
        }
        return builder.build().toString();
    }
    
    public static String createNewTopicViewLink(URI baseUri, String databaseId, String typeId, String viewId) {
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/topic-view/").path(databaseId).path("_" + typeId).path(viewId);
        return builder.build().toString();
    }

    public static String createInstanceLink(URI baseUri, String databaseId, String typeId) {
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/create-instance/").path(databaseId).path(typeId);
        return builder.build().toString();
    }

    public static String createFieldInstanceLink(URI baseUri, String databaseId, String topicId, String fieldId, String typeId) {
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/create-field-instance/").path(databaseId).path(topicId).path(fieldId).path(typeId);
        return builder.build().toString();
    }

    public static String addFieldValuesLink(URI baseUri, String databaseId, String topicId, String parentViewId, String fieldId) {
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/add-field-values/").path(databaseId).path(topicId).path(parentViewId).path(fieldId);
        return builder.build().toString();
    }

    public static String addFieldValuesAtIndexLink(URI baseUri, String databaseId, String topicId, String parentViewId, String fieldId) {
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/add-field-values-at-index/").path(databaseId).path(topicId).path(parentViewId).path(fieldId).path("{index}");
        return builder.build().toString();
    }

    public static String removeFieldValuesLink(URI baseUri, String databaseId, String topicId, String parentViewId, String fieldId) {
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/remove-field-values/").path(databaseId).path(topicId).path(parentViewId).path(fieldId);
        return builder.build().toString();
    }

    public static String moveFieldValuesToIndex(URI baseUri, String databaseId, String topicId, String parentViewId, String fieldId) {
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/move-field-values-to-index/").path(databaseId).path(topicId).path(parentViewId).path(fieldId).path("{index}");
        return builder.build().toString();
    }

    public static String pagingLink(URI baseUri, String databaseId, String topicId, String parentViewId, String fieldId) {
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/paging-field/").path(databaseId).path(topicId).path(parentViewId).path(fieldId).path("{start}").path("{limit}");
        return builder.build().toString();
    }

    public static String availableFieldValues(URI baseUri, String databaseId, String topicId, String parentViewId, String fieldId) {
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/available-field-values/").path(databaseId).path(topicId).path(parentViewId).path(fieldId);
        return builder.build().toString();
    }

    public static String availableTypesTreeLazy(URI baseUri, String databaseId, String typeId) {
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/available-types-tree-lazy/").path(databaseId).path(typeId);
        return builder.build().toString();
    }

    public static String getAvailableTypesTree(URI baseUri, String databaseId) {
        UriBuilder builder = UriBuilder.fromUri(baseUri).path("editor/available-types-tree/").path(databaseId);
        return builder.build().toString();
    }
    
}
