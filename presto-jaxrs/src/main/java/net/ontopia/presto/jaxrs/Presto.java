package net.ontopia.presto.jaxrs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import net.ontopia.presto.jaxb.AvailableFieldValues;
import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Link;
import net.ontopia.presto.jaxb.Origin;
import net.ontopia.presto.jaxb.Topic;
import net.ontopia.presto.jaxb.TopicType;
import net.ontopia.presto.jaxb.TopicTypeTree;
import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.jaxb.View;
import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoSession;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoUpdate;
import net.ontopia.presto.spi.PrestoView;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Presto {

    private static Logger log = LoggerFactory.getLogger(Presto.class.getName());

    private static final int DEFAULT_LIMIT = 100;

    private final PrestoSession session;
    private final UriInfo uriInfo;

    public Presto(PrestoSession session, UriInfo uriInfo) {
        this.session = session;
        this.uriInfo = uriInfo;
    }

    public Map<String,Object> getTopicData(PrestoTopic topic, PrestoType type) {
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
        if (readOnlyMode) {
            result.setReadOnlyMode(readOnlyMode);
        }

        TopicType typeInfo = getTypeInfo(type);    

        boolean isTypeReadOnly = readOnlyMode || type.isReadOnly(); // ISSUE: do we really need this?
        typeInfo.setReadOnly(isTypeReadOnly);

        List<Link> typeLinks = new ArrayList<Link>();
        if (!isTypeReadOnly && type.isCreatable()) {
            UriBuilder builder = UriBuilder.fromUri(uriInfo.getBaseUri()).path("editor/create-instance/").path(type.getSchemaProvider().getDatabaseId()).path(type.getId());
            typeLinks.add(new Link("create-instance", builder.build().toString()));
        }
        typeInfo.setLinks(typeLinks);

        result.setType(typeInfo);

        result.setView(view.getId());

        List<Link> topicLinks = new ArrayList<Link>();
        UriBuilder builder = UriBuilder.fromUri(uriInfo.getBaseUri()).path("editor/topic/").path(type.getSchemaProvider().getDatabaseId()).path(topic.getId()).path(view.getId());
        topicLinks.add(new Link("edit", builder.build().toString()));
        builder = UriBuilder.fromUri(uriInfo.getBaseUri()).path("editor/topic/").path(type.getSchemaProvider().getDatabaseId()).path(topic.getId()).path(view.getId());
        topicLinks.add(new Link("update", builder.build().toString()));
        if (type.isRemovable()) {
            builder = UriBuilder.fromUri(uriInfo.getBaseUri()).path("editor/topic/").path(type.getSchemaProvider().getDatabaseId()).path(topic.getId());
            topicLinks.add(new Link("delete", builder.build().toString()));
        }
        result.setLinks(topicLinks);

        List<FieldData> fields = new ArrayList<FieldData>(); 

        for (PrestoFieldUsage field : type.getFields(view)) {
            if (!field.isHidden()) {
                fields.add(getFieldInfo(topic, field, readOnlyMode));
            }
        }
        result.setFields(fields);
        result.setViews(getViews(topic, type, view, readOnlyMode));
        return result;
    }

    public Topic getNewTopicInfo(PrestoType type, PrestoView view) {
        return getNewTopicInfo(type, view, null, null);
    }

    public Topic getNewTopicInfo(PrestoType type, PrestoView view, String parentId, String parentFieldId) {
        Topic result = new Topic();

        final boolean readOnlyMode = false;
        if (parentId != null) {
            result.setOrigin(new Origin(parentId, parentFieldId));
        }
        result.setType(new TopicType(type.getId(), type.getName()));

        result.setView(view.getId());

        List<Link> topicLinks = new ArrayList<Link>();
        UriBuilder builder = UriBuilder.fromUri(uriInfo.getBaseUri()).path("editor/topic/").path(type.getSchemaProvider().getDatabaseId()).path("_" + type.getId()).path(view.getId());
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
        result.setViews(Collections.singleton(getView(null, view, readOnlyMode)));
        return result;
    }

    private FieldData getFieldInfo(PrestoTopic topic, PrestoFieldUsage field, boolean readOnlyMode) {
        return getFieldInfo(topic, field, readOnlyMode, 0, -1);
    }
    
    public FieldData getFieldInfo(PrestoTopic topic, PrestoFieldUsage field, boolean readOnlyMode, int offset, int limit) {

        PrestoType type = field.getType();
        PrestoView parentView = field.getView();

        boolean isNewTopic = topic == null;

        String databaseId = field.getSchemaProvider().getDatabaseId();
        String topicId = isNewTopic ? "_" + type.getId() : topic.getId();
        String parentViewId = parentView.getId();
        String fieldId = field.getId();

        FieldData fieldData = new FieldData();
        fieldData.setId(fieldId);
        fieldData.setName(field.getName());

        fieldData.setExtra(field.getExtra());

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
                    if (!field.getAvailableFieldCreateTypes().isEmpty()) {
                        UriBuilder builder = UriBuilder.fromUri(uriInfo.getBaseUri()).path("editor/available-field-types/").path(databaseId).path(topicId).path(parentViewId).path(fieldId);
                        fieldLinks.add(new Link("available-field-types", builder.build().toString()));
                    }
                }
                if (allowAdd) {
                    // ISSUE: should add-values and remove-values be links on list result instead?
                    if (!field.getAvailableFieldValueTypes().isEmpty()) {
                        UriBuilder builder = UriBuilder.fromUri(uriInfo.getBaseUri()).path("editor/available-field-values/").path(databaseId).path(topicId).path(parentViewId).path(fieldId);
                        fieldLinks.add(new Link("available-field-values", builder.build().toString()));
                    }
                }
                if (allowAdd || allowCreate) {
                    if (!isNewTopic) {
                        UriBuilder builder = UriBuilder.fromUri(uriInfo.getBaseUri()).path("editor/add-field-values/").path(databaseId).path(topicId).path(parentViewId).path(fieldId);
                        fieldLinks.add(new Link("add-field-values", builder.build().toString()));
                        if (!field.isSorted()) {
                            builder = UriBuilder.fromUri(uriInfo.getBaseUri()).path("editor/add-field-values-at-index/").path(databaseId).path(topicId).path(parentViewId).path(fieldId);
                            fieldLinks.add(new Link("add-field-values-at-index", builder.build().toString() + "/{index}"));
                        }
                    }
                }
                if (allowRemove && !isNewTopic) {
                    UriBuilder builder = UriBuilder.fromUri(uriInfo.getBaseUri()).path("editor/remove-field-values/").path(databaseId).path(topicId).path(parentViewId).path(fieldId);
                    fieldLinks.add(new Link("remove-field-values", builder.build().toString()));
                }      

                if (allowMove && !isNewTopic) {
                    UriBuilder builder = UriBuilder.fromUri(uriInfo.getBaseUri()).path("editor/move-field-values-to-index/").path(databaseId).path(topicId).path(parentViewId).path(fieldId);
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
                    UriBuilder builder = UriBuilder.fromUri(uriInfo.getBaseUri()).path("editor/add-field-values/").path(databaseId).path(topicId).path(parentViewId).path(fieldId);
                    fieldLinks.add(new Link("add-field-values", builder.build().toString()));
                    builder = UriBuilder.fromUri(uriInfo.getBaseUri()).path("editor/remove-field-values/").path(databaseId).path(topicId).path(parentViewId).path(fieldId);
                    fieldLinks.add(new Link("remove-field-values", builder.build().toString()));
                    if (!field.isSorted()) {
                        builder = UriBuilder.fromUri(uriInfo.getBaseUri()).path("editor/add-field-values-at-index/").path(databaseId).path(topicId).path(parentViewId).path(fieldId);
                        fieldLinks.add(new Link("add-field-values-at-index", builder.build().toString() + "/{index}"));
                        builder = UriBuilder.fromUri(uriInfo.getBaseUri()).path("editor/move-field-values-to-index/").path(databaseId).path(topicId).path(parentViewId).path(fieldId);
                        fieldLinks.add(new Link("move-field-values-to-index", builder.build().toString() + "/{index}"));
                    }
                }
            }
        }

        if (field.isPageable()) {
            UriBuilder builder = UriBuilder.fromUri(uriInfo.getBaseUri()).path("editor/paging-field/").path(databaseId).path(topicId).path(parentViewId).path(fieldId);
            fieldLinks.add(new Link("paging", builder.build().toString() + "/{start}/{limit}"));    
        }

        if (!fieldLinks.isEmpty()) {
            fieldData.setLinks(fieldLinks);
        }

        Collection<PrestoType> availableFieldValueTypes = field.getAvailableFieldValueTypes();
        if (!availableFieldValueTypes.isEmpty()) {
            List<TopicType> valueTypes = new ArrayList<TopicType>(availableFieldValueTypes.size());
            for (PrestoType valueType : availableFieldValueTypes) {
                valueTypes.add(getTypeInfo(valueType));
            }
            fieldData.setValueTypes(valueTypes);
        }

        Collection<PrestoType> availableFieldCreateTypes = field.getAvailableFieldCreateTypes();
        if (!availableFieldCreateTypes.isEmpty()) {
            List<TopicType> createTypes = new ArrayList<TopicType>(availableFieldCreateTypes.size());
            for (PrestoType createType : availableFieldCreateTypes) {
                createTypes.add(getCreateFieldInstance(topic, type, field, createType));
            }
            fieldData.setCreateTypes(createTypes);
        }

        List<? extends Object> fieldValues;
        if (isNewTopic) {
            String valuesAssignmentType = field.getValuesAssignmentType();
            if (valuesAssignmentType.equals("initial")) {
                fieldValues = getDefaultValues(topic, type, field);                    
            } else {
                fieldValues = Collections.emptyList();
            }
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
                    String n1 = (o1 instanceof PrestoTopic) ? ((PrestoTopic)o1).getName() : (o1 == null ? null : o1.toString());
                    String n2 = (o2 instanceof PrestoTopic) ? ((PrestoTopic)o2).getName() : (o2 == null ? null : o2.toString());
                    return compareComparables(n1, n2);
                }
            });
        }

        // get values (truncated if neccessary)
        List<Value> values = new ArrayList<Value>(fieldValues.size());
        for (int i=start; i < end; i++) {
            Object value = fieldValues.get(i);
            if (value != null) {
                values.add(getValue(field, value, readOnlyMode));
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

        return fieldData;
    }

    protected Value getValue(PrestoFieldUsage field, Object fieldValue, boolean readOnlyMode) {
        if (fieldValue instanceof PrestoTopic) {
            PrestoTopic valueTopic = (PrestoTopic)fieldValue;
            return getExistingTopicFieldValue(field, valueTopic, readOnlyMode);
        } else {
            Value result = new Value();
            result.setValue(fieldValue.toString());
            boolean removable = !field.isReadOnly();
            if (!readOnlyMode && removable) {
                result.setRemovable(Boolean.TRUE);
            }
            return result;
        }
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

    public List<View> getViews(PrestoTopic topic, PrestoType type, PrestoView view, boolean readOnlyMode) {

        Collection<PrestoView> otherViews = type.getViews(view);

        List<View> views = new ArrayList<View>(otherViews.size()); 
        for (PrestoView otherView : otherViews) {
            views.add(getView(topic, otherView, readOnlyMode));
        }
        return views;
    }

    public View getView(PrestoTopic topic, PrestoView view, boolean readOnlyMode) {
        View result = new View();
        result.setId(view.getId());
        result.setName(view.getName());

        List<Link> links = new ArrayList<Link>();
        if (topic != null) {
            UriBuilder builder = UriBuilder.fromUri(uriInfo.getBaseUri()).path("editor/topic/").path(view.getSchemaProvider().getDatabaseId()).path(topic.getId()).path(view.getId());
            if (readOnlyMode) {
                builder = builder.queryParam("readOnly", "true");
            }
            links.add(new Link("edit-in-view", builder.build().toString()));
        }
        result.setLinks(links);
        return result;
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

    public Value getExistingTopicFieldValue(PrestoFieldUsage field, PrestoTopic value, boolean readOnlyMode) {

        Value result = new Value();
        result.setValue(value.getId());
        result.setName(value.getName());
        if (field.isEmbedded()) {
            PrestoType valueType = field.getSchemaProvider().getTypeById(value.getTypeId());
            result.setEmbedded(getTopicInfo(value, valueType, field.getValueView(), readOnlyMode));
        }

        if (!readOnlyMode && !field.isReadOnly()) {
            result.setRemovable(Boolean.TRUE);
        }

        List<Link> links = new ArrayList<Link>();
        if (field.isTraversable()) {
            PrestoView fieldsView = field.getValueView();
            UriBuilder builder = UriBuilder.fromUri(uriInfo.getBaseUri()).path("editor/topic/").path(fieldsView.getSchemaProvider().getDatabaseId()).path(value.getId()).path(fieldsView.getId());
            if (readOnlyMode) {
                builder = builder.queryParam("readOnly", "true");
            }
            links.add(new Link("edit", builder.build().toString()));
        }
        result.setLinks(links);

        return result;
    }

    public AvailableFieldValues createFieldInfoAllowed(PrestoFieldUsage field, Collection<PrestoTopic> availableFieldValues) {

        AvailableFieldValues result = new AvailableFieldValues();
        result.setId(field.getId());
        result.setName(field.getName());

        List<Value> values = new ArrayList<Value>(availableFieldValues.size());
        for (PrestoTopic value : availableFieldValues) {
            values.add(getAllowedTopicFieldValue(field, value));
        }
        result.setValues(values);

        return result;
    }

    public Value getAllowedTopicFieldValue(PrestoFieldUsage field, PrestoTopic value) {

        Value result = new Value();
        result.setValue(value.getId());
        result.setName(value.getName());

        List<Link> links = new ArrayList<Link>();
        if (field.isTraversable()) {
            PrestoView fieldsView = field.getValueView();
            UriBuilder builder = UriBuilder.fromUri(uriInfo.getBaseUri()).path("editor/topic/").path(fieldsView.getSchemaProvider().getDatabaseId()).path(value.getId()).path(fieldsView.getId());
            links.add(new Link("edit", builder.build().toString()));
        }
        result.setLinks(links);

        return result;
    }

    protected TopicType getTypeInfo(PrestoType type) {
        return new TopicType(type.getId(), type.getName());
    }

    public TopicType getCreateFieldInstance(PrestoTopic topic, PrestoType type, PrestoFieldUsage field, PrestoType createType) {
        TopicType result = getTypeInfo(createType);

        boolean isNewTopic = topic == null;
        String topicId = isNewTopic ? "_" + type.getId() : topic.getId();

        List<Link> links = new ArrayList<Link>();
        UriBuilder builder = UriBuilder.fromUri(uriInfo.getBaseUri()).path("editor/create-field-instance/").path(field.getSchemaProvider().getDatabaseId()).path(topicId).path(field.getId()).path(createType.getId());
        links.add(new Link("create-field-instance", builder.build().toString()));
        result.setLinks(links);

        return result;
    }

    public FieldData addFieldValues(PrestoTopic topic, PrestoType type, PrestoFieldUsage field, 
            Integer index, FieldData fieldObject) {

        Collection<Object> addableValues = resolveValues(field, fieldObject.getValues(), true);

        PrestoDataProvider dataProvider = session.getDataProvider();
        PrestoChangeSet changeSet = dataProvider.newChangeSet();
        PrestoUpdate update = changeSet.updateTopic(topic, type);        

        if (index == null) {
            update.addValues(field, addableValues);
        } else {
            update.addValues(field, addableValues, index);        
        }
        changeSet.save();
        topic = update.getTopicAfterUpdate();
        return getFieldInfo(topic, field, false);
    }

    public FieldData removeFieldValues(PrestoTopic topic, PrestoType type, PrestoFieldUsage field, FieldData fieldObject) {

        Collection<Object> removeableValues = resolveValues(field, fieldObject.getValues(), false);

        PrestoDataProvider dataProvider = session.getDataProvider();
        PrestoChangeSet changeSet = dataProvider.newChangeSet();
        PrestoUpdate update = changeSet.updateTopic(topic, type);        

        update.removeValues(field, removeableValues);

        changeSet.save();
        topic = update.getTopicAfterUpdate();

        return getFieldInfo(topic, field, false);
    }

    public PrestoTopic updateTopic(PrestoTopic topic, PrestoType type, PrestoView view, Topic data) {

        PrestoDataProvider dataProvider = session.getDataProvider();
        PrestoChangeSet changeSet = dataProvider.newChangeSet();

        PrestoUpdate update;
        if (topic == null) {
            update = changeSet.createTopic(type);
        } else {
            update = changeSet.updateTopic(topic, type);
        }

        Map<String, PrestoFieldUsage> fields = getFieldInstanceMap(topic, type, view);

        for (FieldData jsonField : data.getFields()) {
            String fieldId = jsonField.getId();

            PrestoFieldUsage field = fields.get(fieldId);

            // ignore read-only or pageable fields 
            if (!field.isReadOnly() && !field.isPageable()) {
                if  (fields.containsKey(fieldId)) {
                    Collection<Value> values = jsonField.getValues();
                    update.setValues(field, resolveValues(field, values, true));
                }
            }
        }
        
        assignDefaultValues(topic, type, update);
        
        changeSet.save();
        return update.getTopicAfterUpdate();
    }

    protected void assignDefaultValues(PrestoTopic topic, PrestoType type, PrestoUpdate update) {
        boolean isNewTopic = (topic == null);
        // assign [initial] values
        for (PrestoField field : type.getFields()) {
            String valuesAssignmentType = field.getValuesAssignmentType();
            if (valuesAssignmentType.equals("initial")) {
                if (isNewTopic) {
                    update.setValues(field, getDefaultValues(topic, type, field));                    
                }
            } else if (valuesAssignmentType.equals("always")) {
                update.setValues(field, getDefaultValues(topic, type, field));
            }
        }        
    }
    
    protected List<Object> getDefaultValues(PrestoTopic topic, PrestoType type, PrestoField field) {
        List<Object> result = new ArrayList<Object>();
        for (String value : field.getValues()) {
            // TODO: get values from: topic field, session properties
            
            if (value != null) {
                if (value.charAt(0) == '$') {
                    String variable = value.substring(1);
                    for (String varValue : getVariableValues(variable)) {
                        if (varValue != null) {
                            result.add(varValue);
                        }
                    }
                } else {
                    result.add(value);
                }
            }
        }
        return result;
    }
    
    protected Collection<String> getVariableValues(String variable) {
        return Collections.emptyList(); // should be overridden
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
                result.addAll(session.getDataProvider().getTopicsByIds(valueIds));
            } else {
                for (Value value : values) {
                    result.add(getPrimitiveValue(value));
                }
            }
        }
        return result;
    }

    private PrestoTopic updateEmbeddedReference(PrestoView view, Topic embeddedTopic) {

        PrestoDataProvider dataProvider = session.getDataProvider();
        PrestoSchemaProvider schemaProvider = session.getSchemaProvider();

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

        return updateTopic(topic, type, view, embeddedTopic);
    }

    private Map<String, PrestoFieldUsage> getFieldInstanceMap(PrestoTopic topic,
            PrestoType type, PrestoView view) {
        Map<String, PrestoFieldUsage> fields = new HashMap<String, PrestoFieldUsage>();
        for (PrestoFieldUsage field : type.getFields(view)) {
            fields.put(field.getId(), field);
        }
        return fields;
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

    public boolean deleteTopic(PrestoTopic topic, PrestoType type) {
        log.warn("Removing topic " + topic.getId() + " from database " + session.getSchemaProvider().getDatabaseId());
        PrestoChangeSet changeSet = session.getDataProvider().newChangeSet();
        changeSet.deleteTopic(topic, type);
        changeSet.save();
        return true;
    }

    public Collection<TopicTypeTree> getAvailableTypes(Collection<PrestoType> types, boolean tree) {
        Collection<TopicTypeTree> result = new ArrayList<TopicTypeTree>(); 
        for (PrestoType type : types) {
            result.addAll(getAvailableTypes(type, tree));
        }
        return result;
    }

    private Collection<TopicTypeTree> getAvailableTypes(PrestoType type, boolean tree) {
        if (type.isHidden()) {
            return getAvailableTypes(type.getDirectSubTypes(), true);   
        } else {
            TopicTypeTree typeMap = new TopicTypeTree();
            typeMap.setId(type.getId());
            typeMap.setName(type.getName());

            List<Link> links = new ArrayList<Link>();
            if (type.isCreatable()) {
                UriBuilder builder = UriBuilder.fromUri(uriInfo.getBaseUri()).path("editor/create-instance/").path(type.getSchemaProvider().getDatabaseId()).path(type.getId());
                links.add(new Link("create-instance", builder.build().toString()));
            }

            if (tree) {
                Collection<TopicTypeTree> typesList = getAvailableTypes(type.getDirectSubTypes(), true);
                if (!typesList.isEmpty()) {
                    typeMap.setTypes(typesList);
                }
            } else {
                if (!type.getDirectSubTypes().isEmpty()) {
                    UriBuilder builder = UriBuilder.fromUri(uriInfo.getBaseUri()).path("editor/available-types-tree-lazy/").path(type.getSchemaProvider().getDatabaseId()).path(type.getId());
                    links.add(new Link("available-types-tree-lazy", builder.build().toString()));
                }
            }
            typeMap.setLinks(links);
            return Collections.singleton(typeMap);
        }
    }
    
}
