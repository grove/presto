package net.ontopia.presto.jaxrs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.UriInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import net.ontopia.presto.spi.PrestoView;

public class Presto {

    private Logger log = LoggerFactory.getLogger(Presto.class.getName());
    
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

        boolean readOnly = readOnlyMode || type.isReadOnly(); // ISSUE: do we really need this?
        typeInfo.setReadOnly(readOnly);

        List<Link> typeLinks = new ArrayList<Link>();
        if (!readOnlyMode && type.isCreatable()) {
            typeLinks.add(new Link("create-instance", uriInfo.getBaseUri() + "editor/create-instance/" + type.getSchemaProvider().getDatabaseId() + "/" + type.getId()));
        }
        typeInfo.setLinks(typeLinks);

        result.setType(typeInfo);

        result.setView(view.getId());

        List<Link> topicLinks = new ArrayList<Link>();
        topicLinks.add(new Link("edit", uriInfo.getBaseUri() + "editor/topic/" + view.getSchemaProvider().getDatabaseId() + "/" + topic.getId() + "/" + view.getId()));
        if (type.isRemovable()) {
            topicLinks.add(new Link("delete", uriInfo.getBaseUri() + "editor/topic/" + type.getSchemaProvider().getDatabaseId() + "/" + topic.getId()));
        }
        result.setLinks(topicLinks);

        List<FieldData> fields = new ArrayList<FieldData>(); 

        for (PrestoFieldUsage field : type.getFields(view)) {
            fields.add(getFieldInfo(topic, field, topic.getValues(field), readOnlyMode));
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
        topicLinks.add(new Link("create", uriInfo.getBaseUri() + "editor/topic/" + type.getSchemaProvider().getDatabaseId() + "/_" + type.getId() + "/" + view.getId()));    
        result.setLinks(topicLinks);

        List<FieldData> fields = new ArrayList<FieldData>(); 

        PrestoTopic topic = null;
        for (PrestoFieldUsage field : type.getFields(view)) {
            fields.add(getFieldInfo(topic, field, Collections.emptyList(), readOnlyMode));
        }
        result.setFields(fields);
        result.setViews(Collections.singleton(getView(null, view, readOnlyMode)));
        return result;
    }

    private FieldData getFieldInfo(
            PrestoTopic topic, PrestoFieldUsage field, Collection<? extends Object> fieldValues, boolean readOnlyMode) {

        PrestoType type = field.getType();
        PrestoView parentView = field.getView();

        boolean isNewTopic = topic == null;

        String databaseId = field.getSchemaProvider().getDatabaseId();
        String topicId = isNewTopic ? "_" + type.getId() : topic.getId();
        String parentViewId = parentView.getId();
        String fieldId = field.getId();

        String fieldReference = databaseId + "/" + topicId + "/" + parentViewId + "/" + fieldId;

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

        boolean isReadOnly = readOnlyMode || field.isReadOnly();
        if (isReadOnly) {
            fieldData.setReadOnly(Boolean.TRUE);
        }

        if (field.isPrimitiveField()) {
            String dataType = field.getDataType();
            if (dataType != null) {
                fieldData.setDatatype(dataType);
            }
            if (!isReadOnly) {
                List<Link> fieldLinks = new ArrayList<Link>();
                if (!isNewTopic) {
                    fieldLinks.add(new Link("add-field-values", uriInfo.getBaseUri() + "editor/add-field-values/" + fieldReference));
                    fieldLinks.add(new Link("remove-field-values", uriInfo.getBaseUri() + "editor/remove-field-values/" + fieldReference));
                    if (!field.isSorted()) {
                        fieldLinks.add(new Link("add-field-values-at-index", uriInfo.getBaseUri() + "editor/add-field-values-at-index/" + fieldReference + "/{index}"));
                        fieldLinks.add(new Link("move-field-values-to-index", uriInfo.getBaseUri() + "editor/move-field-values-to-index/" + fieldReference + "/{index}"));
                    }
                }
                fieldData.setLinks(fieldLinks);
            }

        } else if (field.isReferenceField()) {
            fieldData.setDatatype("reference");

            boolean allowAddRemove = !isReadOnly && !field.isNewValuesOnly();
            boolean allowCreate = !isReadOnly && !field.isExistingValuesOnly();

            List<Link> fieldLinks = new ArrayList<Link>();      
            if (allowCreate) {
                if (!field.getAvailableFieldCreateTypes().isEmpty()) {
                    fieldLinks.add(new Link("available-field-types", uriInfo.getBaseUri() + "editor/available-field-types/" + fieldReference));
                }
            }
            if (allowAddRemove) {
                // ISSUE: should add-values and remove-values be links on list result instead?
                if (!field.getAvailableFieldValueTypes().isEmpty()) {
                    fieldLinks.add(new Link("available-field-values", uriInfo.getBaseUri() + "editor/available-field-values/" + fieldReference));
                }
                if (!isNewTopic) {
                    fieldLinks.add(new Link("add-field-values", uriInfo.getBaseUri() + "editor/add-field-values/" + fieldReference));
                    fieldLinks.add(new Link("remove-field-values", uriInfo.getBaseUri() + "editor/remove-field-values/" + fieldReference));
                    if (!field.isSorted()) {
                        fieldLinks.add(new Link("add-field-values-at-index", uriInfo.getBaseUri() + "editor/add-field-values-at-index/" + fieldReference + "/{index}"));
                        fieldLinks.add(new Link("move-field-values-to-index", uriInfo.getBaseUri() + "editor/move-field-values-to-index/" + fieldReference + "/{index}"));
                    }
                }
            }      
            fieldData.setLinks(fieldLinks);

        } else {
            // used by query fields, which can have both primitive and reference values
            fieldData.setDatatype("query");
        }

        Collection<PrestoType> availableFieldValueTypes = field.getAvailableFieldValueTypes();
        if (!availableFieldValueTypes.isEmpty()) {
            List<TopicType> valueTypes = new ArrayList<TopicType>(availableFieldValueTypes.size());
            for (PrestoType valueType : availableFieldValueTypes) {
                valueTypes.add(getTypeInfo(valueType));
            }
            fieldData.setValueTypes(valueTypes);
        }

        fieldData.setValues(getValues(field, fieldValues, readOnlyMode));
        return fieldData;
    }

    public List<View> getViews(
            PrestoTopic topic, PrestoType type, PrestoView view, boolean readOnlyMode) {

        Collection<PrestoView> otherViews = type.getViews(view);

        List<View> views = new ArrayList<View>(otherViews.size()); 
        for (PrestoView otherView : otherViews) {
            views.add(getView(topic, otherView, readOnlyMode));
        }
        return views;
    }

    public View getView(PrestoTopic topic,
            PrestoView view, boolean readOnlyMode) {
        View result = new View();
        result.setId(view.getId());
        result.setName(view.getName());

        List<Link> links = new ArrayList<Link>();
        if (topic != null) {
            links.add(new Link("edit-in-view", uriInfo.getBaseUri() + "editor/topic/" + view.getSchemaProvider().getDatabaseId() + "/" + topic.getId() + "/" + view.getId() + (readOnlyMode ? "?readOnly=true" : "")));
        }
        result.setLinks(links);
        return result;
    }

    protected Collection<Value> getValues(PrestoFieldUsage field, Collection<? extends Object> fieldValues, boolean readOnlyMode) {
        List<Value> result = new ArrayList<Value>(fieldValues.size());
        for (Object value : fieldValues) {
            result.add(getValue(field, value, readOnlyMode));
        }
        if (field.isSorted()) {
            Collections.sort(result, new Comparator<Value>() {
                public int compare(Value v1, Value v2) {
                    String vx1 = v1.getName();
                    if (vx1 == null) {
                        vx1 = v1.getValue();
                    }
                    String vx2 = v2.getName();
                    if (vx2 == null) {
                        vx2 = v2.getValue();
                    }
                    return compareStatic(vx1, vx2);
                }
            });
        }
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

    public Value getExistingTopicFieldValue(
            PrestoFieldUsage field, PrestoTopic value, boolean readOnlyMode) {

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
            links.add(new Link("edit", uriInfo.getBaseUri() + "editor/topic/" + fieldsView.getSchemaProvider().getDatabaseId() + "/" + value.getId() + "/" + fieldsView.getId() + (readOnlyMode ? "?readOnly=true" : "")));
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
            values.add(new Presto(session, uriInfo).getAllowedTopicFieldValue(field, value));
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
            links.add(new Link("edit", uriInfo.getBaseUri() + "editor/topic/" + fieldsView.getSchemaProvider().getDatabaseId() + "/" + value.getId() + "/" + fieldsView.getId()));
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
        links.add(new Link("create-field-instance", uriInfo.getBaseUri() + "editor/create-field-instance/" + field.getSchemaProvider().getDatabaseId() + "/" + topicId + "/" + field.getId() + "/" + createType.getId()));
        result.setLinks(links);

        return result;
    }

    public FieldData addFieldValues(PrestoTopic topic, PrestoFieldUsage field, 
            Integer index, FieldData fieldObject) {
        PrestoDataProvider dataProvider = session.getDataProvider();

        if  (field != null) {
            PrestoChangeSet changeSet = dataProvider.updateTopic(topic);        
            boolean isReferenceField = field.isReferenceField();        
            Collection<Object> addableValues = new HashSet<Object>();

            for (Value value : fieldObject.getValues()) {

                if (isReferenceField) {
                    String valueId = getReferenceValue(value);
                    PrestoTopic valueTopic = dataProvider.getTopicById(valueId);
                    addableValues.add(valueTopic);
                } else {
                    addableValues.add(getPrimitiveValue(value));
                }
            }
            if (index == null) {
                changeSet.addValues(field, addableValues);
            } else {
                changeSet.addValues(field, addableValues, index);        
            }
            changeSet.save();
        }
        return getFieldInfo(topic, field, topic.getValues(field), false);
    }

    public FieldData removeFieldValues(PrestoTopic topic, PrestoFieldUsage field, FieldData fieldObject) {
        PrestoDataProvider dataProvider = session.getDataProvider();

        if  (field != null) {

            PrestoChangeSet changeSet = dataProvider.updateTopic(topic);
            boolean isReferenceField = field.isReferenceField();
            Collection<Object> removeableValues = new HashSet<Object>();

            for (Value value : fieldObject.getValues()) {

                if (isReferenceField) {
                    String valueId = getReferenceValue(value);
                    PrestoTopic valueTopic = dataProvider.getTopicById(valueId);
                    removeableValues.add(valueTopic);
                } else {
                    removeableValues.add(getPrimitiveValue(value));
                }
            }
            changeSet.removeValues(field, removeableValues);
            changeSet.save();
        }
        return getFieldInfo(topic, field, topic.getValues(field), false);
    }

    public PrestoTopic updateTopic(PrestoTopic topic, PrestoType type, PrestoView view, Topic data) {

        PrestoDataProvider dataProvider = session.getDataProvider();

        PrestoChangeSet changeSet;
        if (topic == null) {
            changeSet = dataProvider.createTopic(type);
        } else {
            changeSet = dataProvider.updateTopic(topic);
        }

        Map<String, PrestoFieldUsage> fields = getFieldInstanceMap(topic, type, view);

        for (FieldData jsonField : data.getFields()) {
            String fieldId = jsonField.getId();

            PrestoFieldUsage field = fields.get(fieldId);

            boolean isReferenceField = field.isReferenceField();
            boolean isReadOnly = field.isReadOnly(); // ignore readOnly-fields 
            if (!isReadOnly) {
                if  (fields.containsKey(fieldId)) {
                    Collection<Value> values = jsonField.getValues();
                    Collection<Object> newValues = new ArrayList<Object>(values.size());

                    if (!values.isEmpty()) {
                        if (isReferenceField) {
                            if (field.isEmbedded()) {
                                for (Value value : values) {
                                    Topic embeddedReferenceValue = getEmbeddedReference(value);
                                    if (embeddedReferenceValue != null) {
                                        PrestoView valueView = field.getValueView();
                                        newValues.add(updateEmbeddedReference(valueView, embeddedReferenceValue));
                                    }
                                }                
                            } else {
                                if (values.size() == 1) {
                                    Value value = values.iterator().next();
                                    newValues.add(dataProvider.getTopicById(getReferenceValue(value)));
                                } else {
                                    List<String> valueIds = new ArrayList<String>(values.size());
                                    for (Value value : values) {                
                                        valueIds.add(getReferenceValue(value));
                                    }
                                    newValues.add(dataProvider.getTopicsByIds(valueIds));
                                }
                            }
                        } else {
                            for (Value value : values) {
                                newValues.add(getPrimitiveValue(value));
                            }
                        }
                    }
                    changeSet.setValues(field, newValues);
                }
            }
        }
        topic = changeSet.save();
        return topic;
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
        return session.getDataProvider().removeTopic(topic, type);
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
                links.add(new Link("create-instance", uriInfo.getBaseUri() + "editor/create-instance/" + type.getSchemaProvider().getDatabaseId() + "/" + type.getId()));
            }

            if (tree) {
                Collection<TopicTypeTree> typesList = getAvailableTypes(type.getDirectSubTypes(), true);
                if (!typesList.isEmpty()) {
                    typeMap.setTypes(typesList);
                }
            } else {
                if (!type.getDirectSubTypes().isEmpty()) {
                    links.add(new Link("available-types-tree-lazy", uriInfo.getBaseUri() + "editor/available-types-tree-lazy/" + type.getSchemaProvider().getDatabaseId() + "/" + type.getId()));
                }
            }
            typeMap.setLinks(links);
            return Collections.singleton(typeMap);
        }
    }

}
