package net.ontopia.presto.jaxrs;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.UriBuilder;

import net.ontopia.presto.jaxb.AvailableFieldTypes;
import net.ontopia.presto.jaxb.AvailableFieldValues;
import net.ontopia.presto.jaxb.AvailableTopicTypes;
import net.ontopia.presto.jaxb.Database;
import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Link;
import net.ontopia.presto.jaxb.Origin;
import net.ontopia.presto.jaxb.Topic;
import net.ontopia.presto.jaxb.TopicType;
import net.ontopia.presto.jaxb.TopicTypeTree;
import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoDataProvider.ChangeSetHandler;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoUpdate;
import net.ontopia.presto.spi.PrestoView;
import net.ontopia.presto.spi.utils.Utils;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Presto {

    private static Logger log = LoggerFactory.getLogger(Presto.class.getName());

    private static final int DEFAULT_LIMIT = 100;

    private final String databaseId;
    private final String databaseName;

    private final PrestoSchemaProvider schemaProvider;
    private final PrestoDataProvider dataProvider;

    public Presto(String databaseId, String databaseName, PrestoSchemaProvider schemaProvider, PrestoDataProvider dataProvider) {
        this.databaseId = databaseId;
        this.databaseName = databaseName;
        this.schemaProvider = schemaProvider;
        this.dataProvider = dataProvider;
    }

    private String getDatabaseId() {
        return databaseId;
    }

    private String getDatabaseName() {
        return databaseName;
    }

    protected ChangeSetHandler getChangeSetHandler() {
        return null;
    }

    public PrestoDataProvider getDataProvider() {
        return dataProvider;
    }

    public PrestoSchemaProvider getSchemaProvider() {
        return schemaProvider;
    }

    public Map<String,Object> getTopicAsMap(PrestoTopic topic, PrestoType type) {
        Map<String,Object> result = new LinkedHashMap<String,Object>();

        result.put("_id", topic.getId());
        result.put(":name", topic.getName());
        result.put(":type", type.getId());

        for (PrestoField field : type.getFields()) {
            List<Object> values = getValueData(field, topic.getValues(field));
            if (!values.isEmpty()) {
                result.put(field.getId(), values);
            }
        }
        return result;
    }

    protected List<Object> getValueData(PrestoField field, Collection<? extends Object> fieldValues) {
        List<Object> result = new ArrayList<Object>(fieldValues.size());
        for (Object fieldValue : fieldValues) {
            if (fieldValue instanceof PrestoTopic) {
                PrestoTopic valueTopic = (PrestoTopic)fieldValue;
                result.add(valueTopic.getId());
            } else {
                result.add(fieldValue);
            }
        }
        return result;
    }

    public Topic getTopicInfo(PrestoTopic topic, PrestoType type, PrestoView view, boolean readOnlyMode) {
        Topic result = new Topic();

        result.setId(topic.getId());
        result.setName(topic.getName());

        TopicType typeInfo = getTypeInfo(type);    

        boolean isTypeReadOnly = readOnlyMode || type.isReadOnly(); // ISSUE: do we really need this?
        typeInfo.setReadOnly(isTypeReadOnly);

        List<Link> typeLinks = new ArrayList<Link>();
        if (!isTypeReadOnly && type.isCreatable()) {
            UriBuilder builder = UriBuilder.fromUri(getBaseUri()).path("editor/create-instance/").path(getDatabaseId()).path(type.getId());
            typeLinks.add(new Link("create-instance", builder.build().toString()));
        }
        typeInfo.setLinks(typeLinks);

        result.setType(typeInfo);
        result.setView(view.getId());

        List<FieldData> fields = new ArrayList<FieldData>(); 

        boolean allowUpdates = !isTypeReadOnly;
        for (PrestoFieldUsage field : type.getFields(view)) {
            if (!field.isHidden()) {
                fields.add(getFieldInfo(topic, field, readOnlyMode));
            }
            if (!readOnlyMode && !field.isReadOnly()) {
                allowUpdates = true;
            }
        }
        result.setFields(fields);

        List<Link> topicLinks = new ArrayList<Link>();
        UriBuilder builder = UriBuilder.fromUri(getBaseUri()).path("editor/topic/").path(getDatabaseId()).path(topic.getId()).path(view.getId());
        topicLinks.add(new Link("edit", builder.build().toString()));

        if (allowUpdates) {
            builder = UriBuilder.fromUri(getBaseUri()).path("editor/topic/").path(getDatabaseId()).path(topic.getId()).path(view.getId());
            topicLinks.add(new Link("update", builder.build().toString()));
        }

        if (!readOnlyMode && type.isRemovable()) {
            builder = UriBuilder.fromUri(getBaseUri()).path("editor/topic/").path(getDatabaseId()).path(topic.getId());
            topicLinks.add(new Link("delete", builder.build().toString()));
        }
        topicLinks.addAll(getViewLinks(topic, type, view, readOnlyMode));
        result.setLinks(topicLinks);

        result = postProcessTopic(result, topic, type, view);

        return result;
    }

    protected abstract URI getBaseUri();

    public Topic getNewTopicInfo(PrestoType type, PrestoView view) {
        return getNewTopicInfo(type, view, null, null);
    }

    public Topic getNewTopicInfo(PrestoType type, PrestoView view, String parentId, String parentFieldId) {
        Topic result = new Topic();

        final boolean readOnlyMode = false;
        if (parentId != null) {
            result.setOrigin(new Origin(parentId, parentFieldId));
        }
        result.setType(getTypeInfo(type));

        result.setView(view.getId());

        List<Link> topicLinks = new ArrayList<Link>();
        UriBuilder builder = UriBuilder.fromUri(getBaseUri()).path("editor/topic/").path(getDatabaseId()).path("_" + type.getId()).path(view.getId());
        topicLinks.add(new Link("create", builder.build().toString()));
        result.setLinks(topicLinks);

        List<FieldData> fields = new ArrayList<FieldData>(); 

        PrestoTopic topic = null;
        for (PrestoFieldUsage field : type.getFields(view)) {
            if (!field.isHidden()) {
                fields.add(getFieldInfo(topic, field, readOnlyMode));
            }
        }
        result.setFields(fields);

        result = postProcessTopic(result, null, type, view);

        return result;
    }

    private FieldData getFieldInfo(PrestoTopic topic, PrestoFieldUsage field, boolean readOnlyMode) {
        return getFieldInfo(topic, field, readOnlyMode, 0, -1);
    }

    public FieldData getFieldInfo(PrestoTopic topic, final PrestoFieldUsage field, boolean readOnlyMode, int offset, int limit) {

        PrestoType type = field.getType();
        PrestoView parentView = field.getView();

        boolean isNewTopic = topic == null;

        String databaseId = getDatabaseId();
        String topicId = isNewTopic ? "_" + type.getId() : topic.getId();
        String parentViewId = parentView.getId();
        String fieldId = field.getId();

        FieldData fieldData = new FieldData();
        fieldData.setId(fieldId);
        fieldData.setName(field.getName());

        int minCard = field.getMinCardinality();
        if (minCard > 0) {
            fieldData.setMinCardinality(minCard);
        }

        int maxCard = field.getMaxCardinality();
        if (maxCard > 0) {
            fieldData.setMaxCardinality(maxCard);
        }

        String validationType = field.getValidationType();
        if (validationType != null) {
            fieldData.setValidation(validationType);
        }

        String interfaceControl = field.getInterfaceControl(); // ISSUE: should we default the interface control?
        if (interfaceControl != null) {
            fieldData.setInterfaceControl(interfaceControl);          
        }

        if (field.isEmbedded()) {
            fieldData.setEmbeddable(true);
        }

        if (field.isPageable()) {
            fieldData.setPageable(true);
        }

        boolean isReadOnly = readOnlyMode || field.isReadOnly();
        if (isReadOnly) {
            fieldData.setReadOnly(Boolean.TRUE);
        }

        List<Link> fieldLinks = new ArrayList<Link>();      
        if (field.isReferenceField()) {
            fieldData.setDatatype("reference");

            if (!isReadOnly) {
                boolean allowCreate = field.isCreatable();
                boolean allowAdd = field.isAddable();
                boolean allowRemove = field.isRemovable();
                boolean allowMove = !field.isSorted();

                if (allowCreate) {
                    if (!getAvailableFieldCreateTypes(topic, field).isEmpty()) {
                        UriBuilder builder = UriBuilder.fromUri(getBaseUri()).path("editor/available-field-types/").path(databaseId).path(topicId).path(parentViewId).path(fieldId);
                        fieldLinks.add(new Link("available-field-types", builder.build().toString()));
                    }
                }
                if (allowAdd || allowCreate) {
                    if (!isNewTopic) {
                        UriBuilder builder = UriBuilder.fromUri(getBaseUri()).path("editor/add-field-values/").path(databaseId).path(topicId).path(parentViewId).path(fieldId);
                        fieldLinks.add(new Link("add-field-values", builder.build().toString()));
                        if (!field.isSorted()) {
                            builder = UriBuilder.fromUri(getBaseUri()).path("editor/add-field-values-at-index/").path(databaseId).path(topicId).path(parentViewId).path(fieldId);
                            fieldLinks.add(new Link("add-field-values-at-index", builder.build().toString() + "/{index}"));
                        }
                    }
                }
                if (allowRemove && !isNewTopic) {
                    UriBuilder builder = UriBuilder.fromUri(getBaseUri()).path("editor/remove-field-values/").path(databaseId).path(topicId).path(parentViewId).path(fieldId);
                    fieldLinks.add(new Link("remove-field-values", builder.build().toString()));
                }      

                if (allowMove && !isNewTopic) {
                    UriBuilder builder = UriBuilder.fromUri(getBaseUri()).path("editor/move-field-values-to-index/").path(databaseId).path(topicId).path(parentViewId).path(fieldId);
                    fieldLinks.add(new Link("move-field-values-to-index", builder.build().toString() + "/{index}"));
                }
            }
        } else {
            String dataType = field.getDataType();
            if (dataType != null) {
                fieldData.setDatatype(dataType);
            }
            if (!isReadOnly) {
                if (!isNewTopic) {
                    UriBuilder builder = UriBuilder.fromUri(getBaseUri()).path("editor/add-field-values/").path(databaseId).path(topicId).path(parentViewId).path(fieldId);
                    fieldLinks.add(new Link("add-field-values", builder.build().toString()));
                    builder = UriBuilder.fromUri(getBaseUri()).path("editor/remove-field-values/").path(databaseId).path(topicId).path(parentViewId).path(fieldId);
                    fieldLinks.add(new Link("remove-field-values", builder.build().toString()));
                    if (!field.isSorted()) {
                        builder = UriBuilder.fromUri(getBaseUri()).path("editor/add-field-values-at-index/").path(databaseId).path(topicId).path(parentViewId).path(fieldId);
                        fieldLinks.add(new Link("add-field-values-at-index", builder.build().toString() + "/{index}"));
                        builder = UriBuilder.fromUri(getBaseUri()).path("editor/move-field-values-to-index/").path(databaseId).path(topicId).path(parentViewId).path(fieldId);
                        fieldLinks.add(new Link("move-field-values-to-index", builder.build().toString() + "/{index}"));
                    }
                }
            }
        }
        if (!isReadOnly && field.isAddable()) {
            // ISSUE: should add-values and remove-values be links on list result instead?
            if (!field.isReferenceField() ||!getAvailableFieldValueTypes(topic, field).isEmpty()) {
                UriBuilder builder = UriBuilder.fromUri(getBaseUri()).path("editor/available-field-values/").path(databaseId).path(topicId).path(parentViewId).path(fieldId);
                fieldLinks.add(new Link("available-field-values", builder.build().toString()));
            }
        }

        if (field.isPageable()) {
            UriBuilder builder = UriBuilder.fromUri(getBaseUri()).path("editor/paging-field/").path(databaseId).path(topicId).path(parentViewId).path(fieldId);
            fieldLinks.add(new Link("paging", builder.build().toString() + "/{start}/{limit}"));    
        }

        if (!fieldLinks.isEmpty()) {
            fieldData.setLinks(fieldLinks);
        }

        Collection<PrestoType> availableFieldValueTypes = getAvailableFieldValueTypes(topic, field);
        if (!availableFieldValueTypes.isEmpty()) {
            List<TopicType> valueTypes = new ArrayList<TopicType>(availableFieldValueTypes.size());
            for (PrestoType valueType : availableFieldValueTypes) {
                valueTypes.add(getTypeInfo(valueType));
            }
            fieldData.setValueTypes(valueTypes);
        }

        Collection<PrestoType> availableFieldCreateTypes = getAvailableFieldCreateTypes(topic, field);
        if (!availableFieldCreateTypes.isEmpty()) {
            List<TopicType> createTypes = new ArrayList<TopicType>(availableFieldCreateTypes.size());
            for (PrestoType createType : availableFieldCreateTypes) {
                createTypes.add(getCreateFieldInstance(topic, field, createType));
            }
            fieldData.setCreateTypes(createTypes);
        }

        List<? extends Object> fieldValues;
        if (isNewTopic) {
            fieldValues = Collections.emptyList(); // TODO: support initial values
        } else {
            // server-side paging (only if not sorting)
            if (field.isPageable() && !field.isSorted()) {
                int actualOffset = offset >= 0 ? offset : 0;
                int actualLimit = limit > 0 ? limit : DEFAULT_LIMIT;
                fieldData.setPageable(true);
                PrestoTopic.PagedValues pagedValues = topic.getValues(field, actualOffset, actualLimit);
                fieldData.setValuesOffset(pagedValues.getPaging().getOffset());
                fieldData.setValuesLimit(pagedValues.getPaging().getLimit());
                fieldData.setValuesTotal(pagedValues.getTotal());
                fieldValues = pagedValues.getValues();
            } else {
                fieldValues = topic.getValues(field);
            }
        }

        int size = fieldValues.size();
        int start = 0;
        int end = size;

        // sort the result
        if (field.isSorted()) {
            Collections.sort(fieldValues, new Comparator<Object>() {
                public int compare(Object o1, Object o2) {
                    String n1 = (o1 instanceof PrestoTopic) ? ((PrestoTopic)o1).getName(field) : (o1 == null ? null : o1.toString());
                    String n2 = (o2 instanceof PrestoTopic) ? ((PrestoTopic)o2).getName(field) : (o2 == null ? null : o2.toString());
                    return compareComparables(n1, n2);
                }
            });
        }

        // get values (truncated if neccessary)
        List<Value> values = new ArrayList<Value>(fieldValues.size());
        for (int i=start; i < end; i++) {
            Object value = fieldValues.get(i);
            if (value != null) {
                values.add(getExistingFieldValue(field, value, readOnlyMode));
            } else {
                size--;
            }
        }
        fieldData.setValues(values);

        // figure out how to truncate result (offset/limit)
        if (field.isPageable() && field.isSorted()) {
            int _limit = limit > 0 ? limit : DEFAULT_LIMIT;
            start = Math.min(Math.max(0, offset), size);
            end = Math.min(start+_limit, size);
            fieldData.setValuesOffset(start);
            fieldData.setValuesLimit(_limit);
            fieldData.setValuesTotal(size);
        }

        fieldData = postProcessFieldData(fieldData, topic, field);

        return fieldData;
    }

    protected Value getExistingFieldValue(PrestoFieldUsage field, Object fieldValue, boolean readOnlyMode) {
        if (fieldValue instanceof PrestoTopic) {
            PrestoTopic topicValue = (PrestoTopic)fieldValue;
            return getExistingTopicFieldValue(field, topicValue, readOnlyMode);
        } else {
            String stringValue = fieldValue.toString();
            return getExistingStringFieldValue(field, stringValue, readOnlyMode);
        }
    }

    protected Value getExistingStringFieldValue(PrestoFieldUsage field, String fieldValue, boolean readOnlyMode) {
        Value result = new Value();
        result.setValue(fieldValue);
        if (!readOnlyMode && !field.isReadOnly()) {
            result.setRemovable(Boolean.TRUE);
        }
        return result;
    }

    protected Value getExistingTopicFieldValue(PrestoFieldUsage field, PrestoTopic value, boolean readOnlyMode) {

        Value result = new Value();
        result.setValue(value.getId());
        result.setName(value.getName(field));

        if (field.isEmbedded()) {
            PrestoType valueType = getSchemaProvider().getTypeById(value.getTypeId());
            result.setEmbedded(getTopicInfo(value, valueType, field.getValueView(), readOnlyMode));
        }

        if (!readOnlyMode && !field.isReadOnly()) {
            result.setRemovable(Boolean.TRUE);
        }

        List<Link> links = new ArrayList<Link>();
        if (field.isTraversable()) {
            PrestoView fieldsView = field.getValueView();
            UriBuilder builder = UriBuilder.fromUri(getBaseUri()).path("editor/topic/").path(getDatabaseId()).path(value.getId()).path(fieldsView.getId());
            if (readOnlyMode) {
                builder = builder.queryParam("readOnly", "true");
            }
            links.add(new Link("edit", builder.build().toString()));
        }
        result.setLinks(links);

        return result;
    }

    public AvailableFieldValues getAvailableFieldValuesInfo(PrestoTopic topic, PrestoFieldUsage field) {

        AvailableFieldValues result = new AvailableFieldValues();
        result.setId(field.getId());
        result.setName(field.getName());
        result.setValues(getAllowedFieldValues(topic, field));

        return result;
    }
    
    protected Collection<? extends Object> getAvailableFieldValues(PrestoTopic topic, PrestoFieldUsage field) {
        Collection<? extends Object> result = getCustomAvailableValues(topic, field);
        if (result != null) {
            return result;
        }
        return dataProvider.getAvailableFieldValues(topic, field);
    }

    private Collection<? extends Object> getCustomAvailableValues(PrestoTopic topic, PrestoFieldUsage field) {
        ObjectNode extra = getFieldExtraNode(field);
        if (extra != null) {
            JsonNode availableValuesNode = extra.path("availableValues");
            if (availableValuesNode.isArray()) {
                List<Object> result = new ArrayList<Object>();
                final boolean isReferenceField = field.isReferenceField();
                for (JsonNode availableValueNode : availableValuesNode) {
                    String availableValue = availableValueNode.getTextValue();
                    if (isReferenceField) {
                        PrestoTopic topicValue = dataProvider.getTopicById(availableValue);
                        if (topicValue != null) {
                            result.add(topicValue);
                        }
                    } else {
                        result.add(availableValue);
                    }
                }
                return result;
            } else if (availableValuesNode.isObject()) {
                JsonNode classNode = availableValuesNode.path("class");
                if (classNode.isTextual()) {
                    String className = classNode.getTextValue();
                    AvailableFieldValuesResolver processor = Utils.newInstanceOf(className, AvailableFieldValuesResolver.class);
                    if (processor != null) {
                        processor.setSchemaProvider(schemaProvider);
                        processor.setDataProvider(dataProvider);
                        return processor.getAvailableFieldValues(topic, field);
                    }
                } else {
                    log.warn("Field " + field.getId() + " extra.availableValues.class missing: " + extra);                    
                }
            } else if (!availableValuesNode.isMissingNode()) {
                log.warn("Field " + field.getId() + " extra.availableValues is not an array: " + extra);
            }
        }
        return null;
    }

    protected List<Value> getAllowedFieldValues(PrestoTopic topic, PrestoFieldUsage field) {
        Collection<? extends Object> availableFieldValues = getAvailableFieldValues(topic, field);
        
        List<Value> result = new ArrayList<Value>(availableFieldValues.size());
        for (Object value : availableFieldValues) {
            result.add(getAllowedFieldValue(topic, field, value));
        }
        return result;
    }
    
    protected Value getAllowedFieldValue(PrestoTopic topic, PrestoFieldUsage field, Object fieldValue) {
        if (fieldValue instanceof PrestoTopic) {
            PrestoTopic topicValue = (PrestoTopic)fieldValue;
            return getAllowedTopicFieldValue(topic, field, topicValue);
        } else {
            String stringValue = fieldValue.toString();
            return getAllowedStringFieldValue(topic, field, stringValue);
        }
    }

    protected Value getAllowedStringFieldValue(PrestoTopic topic, PrestoFieldUsage field, String fieldValue) {
        Value result = new Value();
        result.setValue(fieldValue);
        result.setName(fieldValue);
        return result;
    }

    protected Value getAllowedTopicFieldValue(PrestoTopic topic, PrestoFieldUsage field, PrestoTopic value) {

        Value result = new Value();
        result.setValue(value.getId());
        result.setName(value.getName(field));

        List<Link> links = new ArrayList<Link>();
        if (field.isTraversable()) {
            PrestoView fieldsView = field.getValueView();
            UriBuilder builder = UriBuilder.fromUri(getBaseUri()).path("editor/topic/").path(getDatabaseId()).path(value.getId()).path(fieldsView.getId());
            links.add(new Link("edit", builder.build().toString()));
        }
        result.setLinks(links);

        return result;
    }

    protected int compareComparables(String o1, String o2) {
        if (o1 == null) {
            return (o2 == null ? 0 : -1);
        } else if (o2 == null){ 
            return 1;
        } else {
            return o1.compareTo(o2);
        }
    }

    protected List<Link> getViewLinks(PrestoTopic topic, PrestoType type, PrestoView view, boolean readOnlyMode) {
        Collection<PrestoView> otherViews = type.getViews(view);

        List<Link> views = new ArrayList<Link>(otherViews.size()); 
        for (PrestoView otherView : otherViews) {
            views.add(getViewLink(topic, type, otherView, readOnlyMode));
        }
        return views;
    }

    protected Link getViewLink(PrestoTopic topic, PrestoType type, PrestoView view, boolean readOnlyMode) {
        UriBuilder builder = UriBuilder.fromUri(getBaseUri()).path("editor/topic/").path(getDatabaseId()).path(topic.getId()).path(view.getId());
        String href = builder.build().toString();
        Link result = new Link("edit-in-view", href);
        result.setId(view.getId());
        result.setName(view.getName());

        result = postProcessViewLink(result, view);

        return result;
    }

    protected Link postProcessViewLink(Link link, PrestoView view) {
        ObjectNode extra = getViewExtraNode(view);
        if (extra != null) {
            Map<String, Object> params = getExtraParamsMap(extra);
            if (params != null) {
                link.setParams(params);
            }
        }
        return link;
    }

    protected ObjectNode getTypeExtraNode(PrestoType type) {
        Object e = type.getExtra();
        if (e != null && e instanceof ObjectNode) {
            return (ObjectNode)e;
        }
        return null;
    }

    protected ObjectNode getViewExtraNode(PrestoView view) {
        Object e = view.getExtra();
        if (e != null && e instanceof ObjectNode) {
            return (ObjectNode)e;
        }
        return null;
    }

    protected ObjectNode getFieldExtraNode(PrestoField field) {
        Object e = field.getExtra();
        if (e != null && e instanceof ObjectNode) {
            return (ObjectNode)e;
        }
        return null;
    }
    
    protected FieldData postProcessFieldData(FieldData fieldData, PrestoTopic topic, PrestoFieldUsage field) {
        ObjectNode extra = getFieldExtraNode(field);
        if (extra != null) {
            Map<String, Object> params = getExtraParamsMap(extra);
            if (params != null) {
                fieldData.setParams(params);
            }
            JsonNode postProcessorNode = extra.path("postProcessor");
            if (postProcessorNode.isTextual()) {
                String className = postProcessorNode.getTextValue();
                FieldDataPostProcessor processor = Utils.newInstanceOf(className, FieldDataPostProcessor.class);
                if (processor != null) {
                    processor.setSchemaProvider(schemaProvider);
                    processor.setDataProvider(dataProvider);
                    fieldData = processor.postProcess(fieldData, topic, field);
                }
            }
            JsonNode messagesNode = extra.path("messages");
            if (messagesNode.isArray()) {
                List<FieldData.Message> messages = new ArrayList<FieldData.Message>();
                for (JsonNode messageNode : messagesNode) {
                    String type = messageNode.get("type").getTextValue();
                    String message = messageNode.get("message").getTextValue();
                    messages.add(new FieldData.Message(type, message));
                }
                if (fieldData.getMessages() != null) {
                    fieldData.getMessages().addAll(messages);
                } else {
                    fieldData.setMessages(messages);
                }
            }

        }
        return fieldData;
    }

    protected Topic postProcessTopic(Topic topicData, PrestoTopic topic, PrestoType type, PrestoView view) {
        ObjectNode extra = getTypeExtraNode(type);
        if (extra != null) {
            Map<String, Object> params = getExtraParamsMap(extra);
            if (params != null) {
                topicData.setParams(params);
            }
            JsonNode postProcessorNode = extra.path("postProcessor");
            if (postProcessorNode.isTextual()) {
                String className = postProcessorNode.getTextValue();
                TopicPostProcessor processor = Utils.newInstanceOf(className, TopicPostProcessor.class);
                if (processor != null) {
                    processor.setSchemaProvider(schemaProvider);
                    processor.setDataProvider(dataProvider);
                    topicData = processor.postProcess(topicData, topic, type, view);
                }
            }
        }    
        return topicData;
    }

    private Map<String,Object> getExtraParamsMap(ObjectNode extra) {
        ObjectNode extraNode = (ObjectNode)extra;
        JsonNode params = extraNode.path("params");
        if (params.isObject()) {
            Map<String,Object> result = new LinkedHashMap<String,Object>();
            Iterator<String> pnIter = params.getFieldNames();
            while (pnIter.hasNext()) {
                String pn = pnIter.next();
                result.put(pn, params.get(pn));
            }
            return result;
        }
        return null;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected <T> int compareStatic(Comparable o1, Comparable o2) {
        if (o1 == null)
            return (o2 == null ? 0 : -1);
        else if (o2 == null)
            return 1;
        else
            return o1.compareTo(o2);
    }

    protected TopicType getTypeInfo(PrestoType type) {
        return new TopicType(type.getId(), type.getName());
    }

    public TopicType getCreateFieldInstance(PrestoTopic topic, PrestoFieldUsage field, PrestoType createType) {
        TopicType result = getTypeInfo(createType);
        
        boolean isNewTopic = topic == null;
        PrestoType type = field.getType();
        String topicId = isNewTopic ? "_" + type.getId() : topic.getId();
        
        List<Link> links = new ArrayList<Link>();
        UriBuilder builder = UriBuilder.fromUri(getBaseUri()).path("editor/create-field-instance/").path(getDatabaseId()).path(topicId).path(field.getId()).path(createType.getId());
        links.add(new Link("create-field-instance", builder.build().toString()));
        result.setLinks(links);

        return result;
    }

    public FieldData addFieldValues(PrestoTopic topic, PrestoType type, PrestoFieldUsage field, 
            Integer index, FieldData fieldObject) {

        Collection<Object> addableValues = resolveValues(field, fieldObject.getValues(), true);

        PrestoDataProvider dataProvider = getDataProvider();
        PrestoChangeSet changeSet = dataProvider.newChangeSet(getChangeSetHandler());
        PrestoUpdate update = changeSet.updateTopic(topic, type);        

        if (index == null) {
            update.addValues(field, addableValues);
        } else {
            update.addValues(field, addableValues, index);        
        }

        changeSet.save();

        return getFieldInfo(update.getTopicAfterSave(), field, false);
    }

    public FieldData removeFieldValues(PrestoTopic topic, PrestoType type, PrestoFieldUsage field, FieldData fieldObject) {

        Collection<Object> removeableValues = resolveValues(field, fieldObject.getValues(), false);

        PrestoDataProvider dataProvider = getDataProvider();
        PrestoChangeSet changeSet = dataProvider.newChangeSet(getChangeSetHandler());
        PrestoUpdate update = changeSet.updateTopic(topic, type);        

        update.removeValues(field, removeableValues);

        changeSet.save();

        return getFieldInfo(update.getTopicAfterSave(), field, false);
    }

    public Topic updateTopic(PrestoTopic topic, PrestoType type, PrestoView view, Topic data) {
        PrestoTopic result = updatePrestoTopic(topic, type, view, data);
        return postProcessTopic(getTopicInfo(result, type, view, false), topic, type, view);
    }

    protected PrestoTopic updatePrestoTopic(PrestoTopic topic, PrestoType type, PrestoView view, Topic data) {

        PrestoDataProvider dataProvider = getDataProvider();
        PrestoChangeSet changeSet = dataProvider.newChangeSet(getChangeSetHandler());

        PrestoUpdate update;
        if (topic == null) {
            update = changeSet.createTopic(type);
        } else {
            update = changeSet.updateTopic(topic, type);
        }

        for (FieldData fd : data.getFields()) {

            String fieldId = fd.getId();
            PrestoFieldUsage field = type.getFieldById(fieldId, view);

            // ignore read-only or pageable fields 
            if (!field.isReadOnly() && !field.isPageable()) {
                Collection<Value> values = fd.getValues();
                update.setValues(field, resolveValues(field, values, true));
            }
        }

        changeSet.save();

        return update.getTopicAfterSave();
    }

    private Collection<Object> resolveValues(PrestoFieldUsage field, Collection<Value> values, boolean resolveEmbedded) {
        Collection<Object> result = new ArrayList<Object>(values.size());

        if (!values.isEmpty()) {

            if (field.isReferenceField()) {
                List<String> valueIds = new ArrayList<String>(values.size());
                for (Value value : values) {                
                    Topic embeddedReferenceValue = getEmbeddedReference(value);
                    if (resolveEmbedded && embeddedReferenceValue != null) {
                        PrestoView valueView = field.getValueView();
                        result.add(updateEmbeddedReference(valueView, embeddedReferenceValue));
                    } else {
                        String valueId = getReferenceValue(value);
                        if (valueId != null) {
                            valueIds.add(getReferenceValue(value));
                        }
                    }
                }
                result.addAll(getDataProvider().getTopicsByIds(valueIds));
            } else {
                for (Value value : values) {
                    result.add(getPrimitiveValue(value));
                }
            }
        }
        return result;
    }

    private PrestoTopic updateEmbeddedReference(PrestoView view, Topic embeddedTopic) {

        PrestoDataProvider dataProvider = getDataProvider();
        PrestoSchemaProvider schemaProvider = getSchemaProvider();

        String topicId = embeddedTopic.getId();

        PrestoTopic topic = null;
        PrestoType type;
        if (topicId == null) {
            TopicType topicType = embeddedTopic.getType();
            String typeId = topicType.getId();
            type = schemaProvider.getTypeById(typeId);
        } else {
            topic = dataProvider.getTopicById(topicId);
            type = schemaProvider.getTypeById(topic.getTypeId());
        }

        return updatePrestoTopic(topic, type, view, embeddedTopic);
    }

    private Topic getEmbeddedReference(Value value) {
        return value.getEmbedded();
    }

    private String getPrimitiveValue(Value value) {
        return value.getValue();
    }

    private String getReferenceValue(Value value) {
        return value.getValue();
    }

    public void deleteTopic(PrestoTopic topic, PrestoType type) {
        log.warn("Removing topic " + topic.getId() + " from database " + getDatabaseId());
        PrestoChangeSet changeSet = getDataProvider().newChangeSet(getChangeSetHandler());
        changeSet.deleteTopic(topic, type);
        changeSet.save();
    }
    
    public AvailableFieldTypes getAvailableFieldTypesInfo(PrestoTopic topic, PrestoFieldUsage field) {

        AvailableFieldTypes result = new AvailableFieldTypes();
        result.setId(field.getId());
        result.setName(field.getName());

        if (field.isCreatable()) {
            Collection<PrestoType> availableFieldCreateTypes = getAvailableFieldCreateTypes(topic, field);
            List<TopicType> types = new ArrayList<TopicType>(availableFieldCreateTypes.size());
            for (PrestoType createType : availableFieldCreateTypes) {
                types.add(getCreateFieldInstance(topic, field, createType));
            }                
            result.setTypes(types);
        } else {
            result.setTypes(new ArrayList<TopicType>());
        }
        return result;
    }
    
    public AvailableTopicTypes getAvailableTypesInfo(boolean tree) {
        AvailableTopicTypes result = new AvailableTopicTypes();
        result.setTypes(getAvailableTypes(tree));
        return result;
    }
    
    protected Collection<TopicTypeTree> getAvailableTypes(boolean tree) {
        Collection<PrestoType> rootTypes = schemaProvider.getRootTypes();
        return getAvailableTypes(rootTypes, tree);
    }
    
    protected Collection<TopicTypeTree> getAvailableTypes(Collection<PrestoType> types, boolean tree) {
        Collection<TopicTypeTree> result = new ArrayList<TopicTypeTree>(); 
        for (PrestoType rootType : types) {
            result.addAll(getAvailableTypes(rootType, tree));
        }
        return result;
    }

    protected Collection<TopicTypeTree> getAvailableTypes(PrestoType type, boolean tree) {
        if (type.isHidden()) {
            return getAvailableTypes(type.getDirectSubTypes(), true);   
        } else {
            TopicTypeTree typeMap = new TopicTypeTree();
            typeMap.setId(type.getId());
            typeMap.setName(type.getName());

            List<Link> links = new ArrayList<Link>();
            if (type.isCreatable()) {
                UriBuilder builder = UriBuilder.fromUri(getBaseUri()).path("editor/create-instance/").path(getDatabaseId()).path(type.getId());
                links.add(new Link("create-instance", builder.build().toString()));
            }

            if (tree) {
                Collection<TopicTypeTree> typesList = getAvailableTypes(type.getDirectSubTypes(), true);
                if (!typesList.isEmpty()) {
                    typeMap.setTypes(typesList);
                }
            } else {
                if (!type.getDirectSubTypes().isEmpty()) {
                    UriBuilder builder = UriBuilder.fromUri(getBaseUri()).path("editor/available-types-tree-lazy/").path(getDatabaseId()).path(type.getId());
                    links.add(new Link("available-types-tree-lazy", builder.build().toString()));
                }
            }
            typeMap.setLinks(links);
            return Collections.singleton(typeMap);
        }
    }

    public void commit() {
    }

    public void abort() {
    }

    public void close() {
    }

    public Database getDatabaseInfo() {
        Database result = new Database();

        result.setId(getDatabaseId());
        result.setName(getDatabaseName());

        List<Link> links = new ArrayList<Link>();
        links.add(new Link("available-types-tree", getBaseUri() + "editor/available-types-tree/" + getDatabaseId()));
        links.add(new Link("edit-topic-by-id", getBaseUri() + "editor/topic/" + getDatabaseId() + "/{topicId}"));
        result.setLinks(links);      
        
        return result;
    }
    
    protected Collection<PrestoType> getAvailableFieldValueTypes(PrestoTopic topic, PrestoFieldUsage field) {
        return field.getAvailableFieldValueTypes();
    }
    
    protected Collection<PrestoType> getAvailableFieldCreateTypes(PrestoTopic topic, PrestoFieldUsage field) {
        Collection<PrestoType> result = getCustomAvailableFieldCreateTypes(topic, field);
        if (result != null) {
            return result;
        }
        return field.getAvailableFieldCreateTypes();
    }

    private Collection<PrestoType> getCustomAvailableFieldCreateTypes(PrestoTopic topic, PrestoFieldUsage field) {
        ObjectNode extra = getFieldExtraNode(field);
        if (extra != null) {
            JsonNode createTypesNode = extra.path("createTypes");
            if (createTypesNode.isObject()) {
                JsonNode classNode = createTypesNode.path("class");
                if (classNode.isTextual()) {
                    String className = classNode.getTextValue();
                    AvailableFieldCreateTypesResolver processor = Utils.newInstanceOf(className, AvailableFieldCreateTypesResolver.class);
                    if (processor != null) {
                        processor.setSchemaProvider(schemaProvider);
                        processor.setDataProvider(dataProvider);
                        return processor.getAvailableFieldCreateTypes(topic, field);
                    }
                } else {
                    log.warn("Field " + field.getId() + " extra.createTypes.class missing: " + extra);                    
                }
            } else if (!createTypesNode.isMissingNode()) {
                log.warn("Field " + field.getId() + " extra.createTypes is not an object: " + extra);
            }
        }
        return null;
    }

}
