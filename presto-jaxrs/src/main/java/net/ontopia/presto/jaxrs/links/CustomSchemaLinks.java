package net.ontopia.presto.jaxrs.links;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import net.ontopia.presto.jaxb.Link;
import net.ontopia.presto.jaxrs.PathParser;
import net.ontopia.presto.jaxrs.Presto;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoView;
import net.ontopia.presto.spi.utils.ExtraUtils;
import net.ontopia.presto.spi.utils.PrestoContext;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;

// NOTE: this class is EXPERIMENTAL.

public class CustomSchemaLinks implements Links {

    private final Links delegate;
    private final String databaseId;
    private final String baseUri;

    public CustomSchemaLinks(URI baseUri, String databaseId, Links delegate) {
        this.baseUri = baseUri.toASCIIString();
        this.databaseId = databaseId;
        this.delegate = delegate;
    }

    protected String getDatabaseId() {
        return databaseId;
    }

    protected String getBaseUri() {
        return baseUri;
    }
    
    protected String getInlineTopicPath(PrestoContext parentContext, PrestoField parentField) {
        return PathParser.getInlineTopicPath(parentContext, parentField);
    }
    
    protected String replaceUriPattern(String template, Map<String, String> values) {
        return PathParser.replaceUriPattern(template, values);
    }
    
    protected String getHref(String[] rels, PrestoType type) {
        for (String rel : rels) {
            String href = getHrefExtra(rel, ExtraUtils.getTypeExtraNode(type));
            if (href == null) {
                href = getHrefExtra(rel, ExtraUtils.getSchemaExtraNode(type.getSchemaProvider()));
            }
            if (href != null) {
                return href;
            }
        }
        return null;
    }

    protected String getHref(String[] rels, PrestoType type, PrestoView view) {
        for (String rel : rels) {
            String href = getHrefExtra(rel, ExtraUtils.getViewExtraNode(view));
            if (href == null) {
                href = getHrefExtra(rel, ExtraUtils.getTypeExtraNode(type));
                if (href == null) {
                    href = getHrefExtra(rel, ExtraUtils.getSchemaExtraNode(type.getSchemaProvider()));
                }
            }
            if (href != null) {
                return href;
            }
        }
        return null;
    }

    protected String getHref(String[] rels, PrestoType type, PrestoView view, PrestoField field) {
        for (String rel : rels) {
            String href = getHrefExtra(rel, ExtraUtils.getFieldExtraNode(field));
            if (href == null) {
                href = getHrefExtra(rel, ExtraUtils.getViewExtraNode(view));
                if (href == null) {
                    href = getHrefExtra(rel, ExtraUtils.getTypeExtraNode(type));
                    if (href == null) {
                        href = getHrefExtra(rel, ExtraUtils.getSchemaExtraNode(type.getSchemaProvider()));
                    }
                }
            }
            if (href != null) {
                return href;
            }
        }
        return null;
    }
    
    protected String getHrefExtra(String rel, ObjectNode extra) {
        if (extra != null) {
            JsonNode rels = extra.path("rels");
            if (rels.isObject()) {
                JsonNode relNode = rels.path(rel);
                if (relNode.isTextual()) {
                    return relNode.getTextValue();
                }
            }
        }
        return null;
    }
    
    @Override
    public Link topicEditLink(String topicId, PrestoType type, PrestoView view, boolean readOnly) {
        String rel = Presto.Rel.REL_TOPIC_EDIT.getRel();
        String href = getHref(new String[] { "topicEditLink", ":topic" }, type, view);
        if (href != null) {
            Map<String,String> params = new HashMap<String,String>();
            params.put("baseUri", getBaseUri());
            params.put("databaseId", getDatabaseId());
            params.put("topicId", PathParser.skull(topicId));
            params.put("typeId", type.getId());
            params.put("viewId", view.getId());
            params.put("readOnly", Boolean.toString(readOnly));
            return new Link(rel, replaceUriPattern(href, params));
        }
        return delegate.topicEditLink(topicId, type, view, readOnly);
    }

    @Override
    public Link topicEditInlineLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view, boolean readOnly) {
        String rel = Presto.Rel.REL_TOPIC_EDIT.getRel();
        String href = getHref(new String[] { "topicEditInlineLink", ":topicInline" }, type, view);
        if (href != null) {
            Map<String,String> params = new HashMap<String,String>();
            params.put("baseUri", getBaseUri());
            params.put("databaseId", getDatabaseId());
            params.put("topicId", PathParser.skull(topicId));
            params.put("typeId", type.getId());
            params.put("viewId", view.getId());
            params.put("readOnly", Boolean.toString(readOnly));
            String path = getInlineTopicPath(parentContext, parentField);
            params.put("path", path);
            return new Link(rel, replaceUriPattern(href, params));
        }
        return delegate.topicEditInlineLink(parentContext, parentField, topicId, type, view, readOnly);
    }

    @Override
    public String topicViewHref(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view, boolean readOnly) {
        String href = getHref(new String[] { "topicViewHref", ":topicView" }, type, view);
        if (href != null) {
            Map<String,String> params = new HashMap<String,String>();
            params.put("baseUri", getBaseUri());
            params.put("databaseId", getDatabaseId());
            params.put("topicId", PathParser.skull(topicId));
            params.put("typeId", type.getId());
            params.put("viewId", view.getId());
            params.put("readOnly", Boolean.toString(readOnly));
            String path = getInlineTopicPath(parentContext, parentField);
            params.put("path", path);
            return replaceUriPattern(href, params);
        }
        return delegate.topicViewHref(parentContext, parentField, topicId, type, view, readOnly);
    }

    @Override
    public Link topicViewUpdateLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view) {
        String rel = Presto.Rel.REL_TOPIC_UPDATE.getRel();
        String href = getHref(new String[] { "topicViewUpdateLink", ":topicView" }, type, view);
        if (href != null) {
            Map<String,String> params = new HashMap<String,String>();
            params.put("baseUri", getBaseUri());
            params.put("databaseId", getDatabaseId());
            params.put("topicId", PathParser.skull(topicId));
            params.put("typeId", type.getId());
            params.put("viewId", view.getId());
            String path = getInlineTopicPath(parentContext, parentField);
            params.put("path", path);
            return new Link(rel, replaceUriPattern(href, params));
        }
        return delegate.topicViewUpdateLink(parentContext, parentField, topicId, type, view);
    }

    @Override
    public Link topicViewDeleteLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view) {
        String rel = Presto.Rel.REL_TOPIC_DELETE.getRel();
        String href = getHref(new String[] { "topicViewDeleteLink", ":topicView" }, type, view);
        if (href != null) {
            Map<String,String> params = new HashMap<String,String>();
            params.put("baseUri", getBaseUri());
            params.put("databaseId", getDatabaseId());
            params.put("topicId", PathParser.skull(topicId));
            params.put("typeId", type.getId());
            params.put("viewId", view.getId());
            String path = getInlineTopicPath(parentContext, parentField);
            params.put("path", path);
            return new Link(rel, replaceUriPattern(href, params));
        }
        return delegate.topicViewDeleteLink(parentContext, parentField, topicId, type, view);
    }

    @Override
    public String topicViewExternalHref(PrestoContext context) {
        // TODO: ???
        return delegate.topicViewExternalHref(context);
    }

    @Override
    public Link fieldOnChangeLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view, PrestoField field) {
        String rel = Presto.Rel.REL_ONCHANGE.getRel();
        String href = getHref(new String[] { "fieldOnChangeLink" }, type, view, field);
        if (href != null) {
            Map<String,String> params = new HashMap<String,String>();
            params.put("baseUri", getBaseUri());
            params.put("databaseId", getDatabaseId());
            params.put("topicId", PathParser.skull(topicId));
            params.put("typeId", type.getId());
            params.put("viewId", view.getId());
            params.put("fieldId", field.getId());
            String path = getInlineTopicPath(parentContext, parentField);
            params.put("path", path);
            return new Link(rel, replaceUriPattern(href, params));
        }
        return delegate.fieldOnChangeLink(parentContext, parentField, topicId, type, view, field);
    }

    @Override
    public Link topicViewCreateLink(PrestoType type, PrestoView view) {
        String rel = Presto.Rel.REL_TOPIC_CREATE.getRel();
        String href = getHref(new String[] { "topicViewCreateLink", ":topicView" }, type, view);
        if (href != null) {
            Map<String,String> params = new HashMap<String,String>();
            params.put("baseUri", getBaseUri());
            params.put("databaseId", getDatabaseId());
            params.put("typeId", type.getId());
            params.put("topicId", "_" + type.getId()); // NOTE: not used by default
            params.put("viewId", view.getId());
            params.put("path", "_");
            return new Link(rel, replaceUriPattern(href, params));
        }
        return delegate.topicViewCreateLink(type, view);
    }

    @Override
    public Link topicViewCreateInlineLink(PrestoContext parentContext, PrestoField parentField, PrestoType type, PrestoView view) {
        String rel = Presto.Rel.REL_TOPIC_CREATE.getRel();
        String href = getHref(new String[] { "topicViewCreateInlineLink", ":topicViewInline" }, type, view);
        if (href != null) {
            Map<String,String> params = new HashMap<String,String>();
            params.put("baseUri", getBaseUri());
            params.put("databaseId", getDatabaseId());
            params.put("typeId", type.getId());
            params.put("topicId", "_" + type.getId()); // NOTE: not used by default
            params.put("viewId", view.getId());
            String path = getInlineTopicPath(parentContext, parentField);
            params.put("path", path);
            return new Link(rel, replaceUriPattern(href, params));
        }
        return delegate.topicViewCreateInlineLink(parentContext, parentField, type, view);
    }

    @Override
    public Link topicTemplateLink(PrestoType type) {
        String rel = Presto.Rel.REL_TOPIC_TEMPLATE.getRel();
        String href = getHref(new String[] { "topicTemplateLink", ":topicTemplate" }, type);
        if (href != null) {
            Map<String,String> params = new HashMap<String,String>();
            params.put("baseUri", getBaseUri());
            params.put("databaseId", getDatabaseId());
            params.put("typeId", type.getId());
            return new Link(rel, replaceUriPattern(href, params));
        }
        return delegate.topicTemplateLink(type);
    }

    @Override
    public String topicTemplateHref(PrestoType type) {
        String href = getHref(new String[] { "topicTemplateHref", ":topicTemplate" }, type);
        if (href != null) {
            Map<String,String> params = new HashMap<String,String>();
            params.put("baseUri", getBaseUri());
            params.put("databaseId", getDatabaseId());
            params.put("typeId", type.getId());
            return replaceUriPattern(href, params);
        }
        return delegate.topicTemplateHref(type);
    }

    @Override
    public Link topicTemplateFieldLink(PrestoContext parentContext, PrestoField parentField, PrestoType type) {
        String rel = Presto.Rel.REL_TOPIC_TEMPLATE_FIELD.getRel();
        String href = getHref(new String[] { "topicTemplateFieldLink", ":topicTemplateField" }, type);
        if (href != null) {
            Map<String,String> params = new HashMap<String,String>();
            params.put("baseUri", getBaseUri());
            params.put("databaseId", getDatabaseId());
            params.put("typeId", type.getId());
            String path = getInlineTopicPath(parentContext, parentField);
            params.put("path", path);
            return new Link(rel, replaceUriPattern(href, params));
        }
        return delegate.topicTemplateFieldLink(parentContext, parentField, type);
    }

    @Override
    public String topicTemplateFieldHref(PrestoContext parentContext, PrestoField parentField, PrestoType type) {
        String href = getHref(new String[] { "topicTemplateFieldHref", ":topicTemplateField" }, type);
        if (href != null) {
            Map<String,String> params = new HashMap<String,String>();
            params.put("baseUri", getBaseUri());
            params.put("databaseId", getDatabaseId());
            params.put("typeId", type.getId());
            String path = getInlineTopicPath(parentContext, parentField);
            params.put("path", path);
            return replaceUriPattern(href, params);
        }
        return delegate.topicTemplateFieldHref(parentContext, parentField, type);
    }

    @Override
    public Link fieldAddValuesLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view, PrestoField field) {
        String rel = Presto.Rel.REL_ADD_FIELD_VALUES.getRel();
        String href = getHref(new String[] { "fieldAddValuesLink" }, type, view, field);
        if (href != null) {
            Map<String,String> params = new HashMap<String,String>();
            params.put("baseUri", getBaseUri());
            params.put("databaseId", getDatabaseId());
            params.put("topicId", topicId);
            params.put("typeId", type.getId());
            params.put("viewId", view.getId());
            params.put("fieldId", field.getId());
            String path = getInlineTopicPath(parentContext, parentField);
            params.put("path", path);
            return new Link(rel, replaceUriPattern(href, params));
        }
        return delegate.fieldAddValuesLink(parentContext, parentField, topicId, type, view, field);
    }

    @Override
    public Link fieldAddValuesAtIndexLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view, PrestoField field) {
        String rel = Presto.Rel.REL_ADD_FIELD_VALUES_AT_INDEX.getRel();
        String href = getHref(new String[] { "fieldAddValuesAtIndexLink" }, type, view, field);
        if (href != null) {
            Map<String,String> params = new HashMap<String,String>();
            params.put("baseUri", getBaseUri());
            params.put("databaseId", getDatabaseId());
            params.put("topicId", topicId);
            params.put("typeId", type.getId());
            params.put("viewId", view.getId());
            params.put("fieldId", field.getId());
            String path = getInlineTopicPath(parentContext, parentField);
            params.put("path", path);
            return new Link(rel, replaceUriPattern(href, params));
        }
        return delegate.fieldAddValuesAtIndexLink(parentContext, parentField, topicId, type, view, field);
    }

    @Override
    public Link fieldRemoveValuesLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view, PrestoField field) {
        String rel = Presto.Rel.REL_REMOVE_FIELD_VALUES.getRel();
        String href = getHref(new String[] { "fieldRemoveValuesLink" }, type, view, field);
        if (href != null) {
            Map<String,String> params = new HashMap<String,String>();
            params.put("baseUri", getBaseUri());
            params.put("databaseId", getDatabaseId());
            params.put("topicId", topicId);
            params.put("typeId", type.getId());
            params.put("viewId", view.getId());
            params.put("topicId", topicId);
            params.put("fieldId", field.getId());
            String path = getInlineTopicPath(parentContext, parentField);
            params.put("path", path);
            return new Link(rel, replaceUriPattern(href, params));
        }
        return delegate.fieldRemoveValuesLink(parentContext, parentField, topicId, type, view, field);
    }

    @Override
    public Link fieldMoveValuesToIndexLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view, PrestoField field) {
        String rel = Presto.Rel.REL_MOVE_FIELD_VALUES_TO_INDEX.getRel();
        String href = getHref(new String[] { "fieldMoveValuesToIndexLink" }, type, view, field);
        if (href != null) {
            Map<String,String> params = new HashMap<String,String>();
            params.put("baseUri", getBaseUri());
            params.put("databaseId", getDatabaseId());
            params.put("topicId", topicId);
            params.put("typeId", type.getId());
            params.put("viewId", view.getId());
            params.put("fieldId", field.getId());
            String path = getInlineTopicPath(parentContext, parentField);
            params.put("path", path);
            return new Link(rel, replaceUriPattern(href, params));
        }
        return delegate.fieldMoveValuesToIndexLink(parentContext, parentField, topicId, type, view, field);
    }

    @Override
    public Link fieldPagingLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view, PrestoField field) {
        String rel = Presto.Rel.REL_FIELD_PAGING.getRel();
        String href = getHref(new String[] { "fieldPagingLink" }, type, view, field);
        if (href != null) {
            Map<String,String> params = new HashMap<String,String>();
            params.put("baseUri", getBaseUri());
            params.put("databaseId", getDatabaseId());
            params.put("topicId", topicId);
            params.put("typeId", type.getId());
            params.put("viewId", view.getId());
            params.put("fieldId", field.getId());
            String path = getInlineTopicPath(parentContext, parentField);
            params.put("path", path);
            return new Link(rel, replaceUriPattern(href, params));
        }
        return delegate.fieldPagingLink(parentContext, parentField, topicId, type, view, field);
    }

    @Override
    public Link fieldAvailableValuesLink(PrestoContext parentContext, PrestoField parentField, String topicId, PrestoType type, PrestoView view, PrestoField field, boolean query) {
        String rel = Presto.Rel.REL_FIELD_PAGING.getRel();
        String href = getHref(new String[] { "fieldAvailableValuesLink" }, type, view, field);
        if (href != null) {
            Map<String,String> params = new HashMap<String,String>();
            params.put("baseUri", getBaseUri());
            params.put("databaseId", getDatabaseId());
            params.put("topicId", topicId);
            params.put("typeId", type.getId());
            params.put("viewId", view.getId());
            params.put("fieldId", field.getId());
            String path = getInlineTopicPath(parentContext, parentField);
            params.put("path", path);
            return new Link(rel, replaceUriPattern(href, params));
        }
        return delegate.fieldAvailableValuesLink(parentContext, parentField, topicId, type, view, field, query);
    }

    @Override
    public Link topicEditByIdLink() {
        return delegate.topicEditByIdLink();
    }
    
}
