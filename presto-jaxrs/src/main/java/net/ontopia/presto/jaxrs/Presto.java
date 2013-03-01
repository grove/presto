package net.ontopia.presto.jaxrs;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import net.ontopia.presto.jaxb.AvailableFieldTypes;
import net.ontopia.presto.jaxb.AvailableFieldValues;
import net.ontopia.presto.jaxb.AvailableTopicTypes;
import net.ontopia.presto.jaxb.Database;
import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Link;
import net.ontopia.presto.jaxb.Topic;
import net.ontopia.presto.jaxb.TopicType;
import net.ontopia.presto.jaxb.TopicTypeTree;
import net.ontopia.presto.jaxb.TopicView;
import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.jaxrs.PrestoProcessor.Status;
import net.ontopia.presto.jaxrs.resolve.AvailableFieldCreateTypesResolver;
import net.ontopia.presto.jaxrs.resolve.AvailableFieldValuesResolver;
import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoDataProvider.ChangeSetHandler;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoFieldUsage;
import net.ontopia.presto.spi.PrestoInlineTopicBuilder;
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

    private static Logger log = LoggerFactory.getLogger(Presto.class);

    public static final int DEFAULT_LIMIT = 100;

    private final String databaseId;
    private final String databaseName;

    private final PrestoSchemaProvider schemaProvider;
    private final PrestoDataProvider dataProvider;

    private final PrestoProcessor processor;

    public Presto(String databaseId, String databaseName, PrestoSchemaProvider schemaProvider, PrestoDataProvider dataProvider) {
        this.databaseId = databaseId;
        this.databaseName = databaseName;
        this.schemaProvider = schemaProvider;
        this.dataProvider = dataProvider;
        this.processor = new PrestoProcessor(this);
    }

    public String getDatabaseId() {
        return databaseId;
    }

    public String getDatabaseName() {
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

    public PrestoProcessor getProcessor() {
        return processor;
    }
    
//    public Map<String,Object> getTopicAsMap(PrestoTopic topic, PrestoType type) {
//        Map<String,Object> result = new LinkedHashMap<String,Object>();
//
//        result.put("_id", topic.getId());
//        result.put(":name", topic.getName());
//        result.put(":type", type.getId());
//
//        for (PrestoField field : type.getFields()) {
//            List<Object> values = getValueData(field, topic.getValues(field));
//            if (!values.isEmpty()) {
//                result.put(field.getId(), values);
//            }
//        }
//        return result;
//    }
//
//    protected List<Object> getValueData(PrestoField field, Collection<? extends Object> fieldValues) {
//        List<Object> result = new ArrayList<Object>(fieldValues.size());
//        for (Object fieldValue : fieldValues) {
//            if (fieldValue instanceof PrestoTopic) {
//                PrestoTopic valueTopic = (PrestoTopic)fieldValue;
//                result.add(valueTopic.getId());
//            } else {
//                result.add(fieldValue);
//            }
//        }
//        return result;
//    }

    public Topic getTopicAndProcess(PrestoTopic topic, PrestoType type, PrestoView view, boolean readOnlyMode) {
        
        Topic result = new Topic();
        result.setId(topic.getId());
        result.setName(topic.getName());

//        result.setType(getTopicTypeWithNoLinks(type));
        String viewId = view.getId();
        result.setView(viewId);

//        String href = Links.getTopicEditLink(getBaseUri(), getDatabaseId(), topic.getId(), view.getId(), readOnlyMode);
//        result.setHref(href);
        
        // create topic-views
        Collection<PrestoView> views = type.getViews(view);
        List<TopicView> topicViews = new ArrayList<TopicView>(views.size()); 
        for (PrestoView v : views) {
            if (viewId.equals(v.getId())) {
                topicViews.add(getTopicViewAndProcess(topic, type, v, readOnlyMode));
            } else {
                topicViews.add(getTopicViewRemote(topic, type, v));
            }
        }
        result.setViews(topicViews);
        
        return result;
    }
    
    public TopicView getTopicViewRemote(PrestoTopic topic, PrestoType type, PrestoView view) {
        TopicView result = TopicView.remoteView();
        
        result.setId(view.getId());
        result.setName(view.getName());
        result.setTopicId(topic.getId());

        String href = Links.getTopicViewEditLink(getBaseUri(), getDatabaseId(), topic.getId(), view.getId());
        result.setHref(href);
        return result;
    }
    
    public TopicView getTopicViewAndProcess(PrestoTopic topic, PrestoType type, PrestoView view, boolean readOnlyMode) {
        TopicView result = getTopicView(topic, type, view, readOnlyMode);
        
//        Status status = new Status();
//        result = processor.preProcessTopic(result, topic, type, view, status);
        result = processor.postProcessTopicView(result, topic, type, view, null);
        
        return result;
    }

    public TopicView getTopicView(PrestoTopic topic, PrestoType type, PrestoView view, boolean readOnlyMode) {
        TopicView result = TopicView.view();
        result.setId(view.getId());
        result.setName(view.getName());

        result.setTopicId(topic.getId());
        result.setTopicTypeId(type.getId());

        String href = Links.getTopicViewEditLink(getBaseUri(), getDatabaseId(), topic.getId(), view.getId());
        result.setHref(href);

        boolean isTypeReadOnly = readOnlyMode || type.isReadOnly(); // ISSUE: do we really need this?
//        TopicType typeInfo = getTopicTypeWithCreateInstanceLink(type, isTypeReadOnly);
//        result.setType(typeInfo);
        
        List<FieldData> fields = new ArrayList<FieldData>(); 
        boolean allowUpdates = !isTypeReadOnly;
        for (PrestoFieldUsage field : type.getFields(view)) {
            if (!field.isHidden()) {
                fields.add(getFieldData(topic, field, readOnlyMode));
            }
            if (!readOnlyMode && !field.isReadOnly()) {
                allowUpdates = true;
            }
        }
        result.setFields(fields);

        List<Link> links = new ArrayList<Link>();
        links.add(Links.createLabel(type.getName()));

//        topicLinks.add(new Link("edit", href));

        if (allowUpdates) {
            links.add(new Link("update", href));
        }

        if (!readOnlyMode && type.isRemovable()) {
            links.add(new Link("delete", href));
        }
        if (!isTypeReadOnly && type.isCreatable()) {
            links.add(new Link("create-instance", Links.createInstanceLink(getBaseUri(), getDatabaseId(), type.getId())));
        }
        
        result.setLinks(links);

        return result;
    }

    public abstract URI getBaseUri();
    
    public TopicView getNewTopicView(PrestoType type, PrestoView view) {
        return getNewTopicView(type, view, null, null);
    }

    public TopicView getNewTopicView(PrestoType type, PrestoView view, String parentId, String parentFieldId) {
        TopicView result = TopicView.view();
        result.setId(view.getId());
//        result.setName(view.getName());
        result.setName("*" + type.getName() + "*");

//        result.setType(getTopicTypeWithNoLinks(type));
        result.setTopicTypeId(type.getId());
        
        String href;
        if (parentId != null) {
//            result.setOrigin(new Origin(parentId, parentFieldId));
            href = Links.createFieldInstanceLink(getBaseUri(), getDatabaseId(), parentId, parentFieldId, type.getId());
        } else {
            href = Links.createInstanceLink(getBaseUri(), getDatabaseId(), type.getId());
        }
        result.setHref(href);

        final boolean readOnlyMode = false;

        List<FieldData> fields = new ArrayList<FieldData>(); 
        PrestoTopic topic = null;
        for (PrestoFieldUsage field : type.getFields(view)) {
            if (!field.isHidden()) {
                fields.add(getFieldData(topic, field, readOnlyMode));
            }
        }
        result.setFields(fields);
        
        List<Link> links = new ArrayList<Link>();
        links.add(Links.createLabel(type.getName()));
        links.add(new Link("create", Links.createNewTopicViewLink(getBaseUri(), getDatabaseId(), type.getId(), view.getId())));
        result.setLinks(links);

//        Status status = new Status();
//        result = processor.preProcessTopicView(result, null, type, view, status);
        result = processor.postProcessTopicView(result, null, type, view, null);
        return result;
    }

    private FieldData getFieldData(PrestoTopic topic, PrestoFieldUsage field, boolean readOnlyMode) {
        return getFieldData(topic, field, readOnlyMode, 0, -1, true);
    }
    
    public FieldData getFieldDataNoValues(PrestoTopic topic, PrestoFieldUsage field, boolean readOnlyMode) {
        return getFieldData(topic, field, readOnlyMode, 0, -1, false);
    }

    public FieldData getFieldData(PrestoTopic topic, final PrestoFieldUsage field, boolean readOnlyMode, 
            int offset, int limit, boolean includeValues) {

        PrestoType type = field.getType();
        PrestoView parentView = field.getView();

        String databaseId = getDatabaseId();

        boolean isNewTopic = topic == null;
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
            setParam(fieldData, "validationType", validationType);
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

        Collection<Link> fieldLinks = new LinkedHashSet<Link>();      
        if (field.isReferenceField()) {
            fieldData.setDatatype("reference");

            if (!isReadOnly) {
                boolean allowCreate = field.isCreatable();
                boolean allowAdd = field.isAddable();
                boolean allowRemove = field.isRemovable();
                boolean allowMove = !field.isSorted();

                if (allowAdd || allowCreate) {
                    if (!isNewTopic) {
                        fieldLinks.add(new Link("add-field-values", Links.addFieldValuesLink(getBaseUri(), databaseId, topicId, parentViewId, fieldId)));
                        if (!field.isSorted()) {
                            fieldLinks.add(new Link("add-field-values-at-index", Links.addFieldValuesAtIndexLink(getBaseUri(), databaseId, topicId, parentViewId, fieldId)));
                        }
                    }
                }
                if (allowRemove && !isNewTopic) {
                    fieldLinks.add(new Link("remove-field-values", Links.removeFieldValuesLink(getBaseUri(), databaseId, topicId, parentViewId, fieldId)));
                }      

                if (allowMove && !isNewTopic) {
                    fieldLinks.add(new Link("move-field-values-to-index", Links.moveFieldValuesToIndex(getBaseUri(), databaseId, topicId, parentViewId, fieldId)));
                }
            }
        } else {
            String dataType = field.getDataType();
            if (dataType != null) {
                fieldData.setDatatype(dataType);
            }
            if (!isReadOnly) {
                if (!isNewTopic) {
                    fieldLinks.add(new Link("add-field-values", Links.addFieldValuesLink(getBaseUri(), databaseId, topicId, parentViewId, fieldId)));
                    fieldLinks.add(new Link("remove-field-values", Links.removeFieldValuesLink(getBaseUri(), databaseId, topicId, parentViewId, fieldId)));
                    if (!field.isSorted()) {
                        fieldLinks.add(new Link("add-field-values-at-index", Links.addFieldValuesAtIndexLink(getBaseUri(), databaseId, topicId, parentViewId, fieldId)));
                        fieldLinks.add(new Link("move-field-values-to-index", Links.moveFieldValuesToIndex(getBaseUri(), databaseId, topicId, parentViewId, fieldId)));
                    }
                }
            }
        }
        if (!isReadOnly && field.isAddable()) {
            // ISSUE: should add-values and remove-values be links on list result instead?
            if (!field.isReferenceField() || !getAvailableFieldValueTypes(topic, field).isEmpty()) {
                fieldLinks.add(new Link("available-field-values", Links.availableFieldValues(getBaseUri(), databaseId, topicId, parentViewId, fieldId)));
            }
        }

        if (field.isPageable()) {
            fieldLinks.add(new Link("paging", Links.pagingLink(getBaseUri(), databaseId, topicId, parentViewId, fieldId)));    
        }

        fieldLinks.addAll(getCreateFieldInstanceLinks(topic, field));

        if (!fieldLinks.isEmpty()) {
            fieldData.setLinks(fieldLinks);
        }

        // get values (truncated if neccessary)
        if (includeValues) {
            setFieldDataValues(isNewTopic, readOnlyMode, offset, limit, topic, field, fieldData);
        }
        
//        fieldData = processor.postProcessFieldData(fieldData, topic, field, null);

        return fieldData;
    }

    private void setParam(FieldData fieldData, String key, Object value) {
        Map<String, Object> params = fieldData.getParams();
        if (params == null) {
            params = new LinkedHashMap<String,Object>();
            fieldData.setParams(params);
        }
        params.put(key, value);
    }

    private Collection<? extends Link> getCreateFieldInstanceLinks(PrestoTopic topic, PrestoFieldUsage field) {
        Collection<PrestoType> availableFieldCreateTypes = getAvailableFieldCreateTypes(topic, field);
        if (!availableFieldCreateTypes.isEmpty()) {
            return Collections.emptyList();
        }
        Collection<Link> links = new ArrayList<Link>(availableFieldCreateTypes.size());
        for (PrestoType createType : availableFieldCreateTypes) {
            links.add(getCreateFieldInstanceLink(topic, field, createType));
        }
        if (links.size() > 1) {
            Link link = new Link();
            link.setRel("create-field-instance");
            link.setLinks(links);
            link.setName("Ny"); // FIXME: localize
            return Collections.singleton(link);
        } else {
            return links;
        }
    }

    public static final class FieldDataValues {
        private List<Object> inputValues; 
        private List<Value> outputValues;
        
        FieldDataValues(List<Object> inputValues, List<Value> outputValues) {
            this.inputValues = inputValues;
            this.outputValues = outputValues;
        }

        public int size() {
            return inputValues.size();
        }
        
        public Object getInputValue(int index) {
            return inputValues.get(index);
        }
        
        public Value getOutputValue(int index) {
            return outputValues.get(index);
        }
    }
    
    public FieldDataValues setFieldDataValues(boolean isNewTopic, boolean readOnlyMode, int offset, int limit, 
            PrestoTopic topic, final PrestoFieldUsage field, FieldData fieldData) {

        // TODO: refactor to return DTO instead of mutating FieldData here
        
        List<? extends Object> fieldValues;
        if (isNewTopic) {
            fieldValues = Collections.emptyList(); // TODO: support initial values
        } else {
            // server-side paging (only if not sorting)
            if (field.isPageable() && !field.isSorted()) {
                int actualOffset = offset >= 0 ? offset : 0;
                int actualLimit = limit > 0 ? limit : DEFAULT_LIMIT;
                PrestoTopic.PagedValues pagedValues = topic.getValues(field, actualOffset, actualLimit);
                if (fieldData != null) {
                    fieldData.setValuesOffset(pagedValues.getPaging().getOffset());
                    fieldData.setValuesLimit(pagedValues.getPaging().getLimit());
                    fieldData.setValuesTotal(pagedValues.getTotal());
                }
                fieldValues = pagedValues.getValues();
            } else {
                fieldValues = topic.getValues(field);
            }
        }

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

        int size = fieldValues.size();
        int start = 0;
        int end = size;

        List<Object> inputValues = new ArrayList<Object>(fieldValues.size());
        List<Value> outputValues = new ArrayList<Value>(fieldValues.size());
        for (int i=start; i < end; i++) {
            Object value = fieldValues.get(i);
            if (value != null) {
                Value efv = getExistingFieldValue(field, value, readOnlyMode);
                outputValues.add(efv);
                inputValues.add(value);
            } else {
                size--;
            }
        }

        if (fieldData != null) {
            fieldData.setValues(outputValues);
                    
            // figure out how to truncate result (offset/limit)
            if (field.isPageable() && field.isSorted()) {
                int _limit = limit > 0 ? limit : DEFAULT_LIMIT;
                start = Math.min(Math.max(0, offset), size);
                end = Math.min(start+_limit, size);
                fieldData.setValuesOffset(start);
                fieldData.setValuesLimit(_limit);
                fieldData.setValuesTotal(size);
            }
        }
        return new FieldDataValues(inputValues, outputValues);
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
            result.setEmbedded(getTopicView(value, valueType, field.getValueView(), readOnlyMode));
        }

        if (!readOnlyMode && !field.isReadOnly()) {
            result.setRemovable(Boolean.TRUE);
        }

        List<Link> links = new ArrayList<Link>();
        if (field.isTraversable()) {
            PrestoView fieldsView = field.getValueView();
            links.add(new Link("edit", Links.getTopicEditLink(getBaseUri(), getDatabaseId(), value.getId(), fieldsView.getId(), readOnlyMode)));
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
        // TODO: shouldn't this be a PrestoFunction
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
                    if (className != null) {
                        AvailableFieldValuesResolver processor = Utils.newInstanceOf(className, AvailableFieldValuesResolver.class);
                        if (processor != null) {
                            processor.setPresto(this);
                            return processor.getAvailableFieldValues(topic, field);
                        }
                    }
                }
                log.warn("Not able to extract extra.availableValues.class from field " + field.getId() + ": " + extra);                    
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
            links.add(new Link("edit", Links.getTopicEditLink(getBaseUri(), getDatabaseId(), value.getId(), fieldsView.getId())));
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

//    protected List<Link> getViewLinks(PrestoTopic topic, PrestoType type, PrestoView view, boolean readOnlyMode, ViewType viewType) {
//        Collection<PrestoView> otherViews = type.getViews(view);
//
//        List<Link> views = new ArrayList<Link>(otherViews.size()); 
//        for (PrestoView otherView : otherViews) {
//            if (otherView.getType().equals(viewType)) {
//                views.add(getViewLink(topic, type, otherView, readOnlyMode));
//            }
//        }
//        return views;
//    }
//
//    protected Link getViewLink(PrestoTopic topic, PrestoType type, PrestoView view, boolean readOnlyMode) {
//        String href = Links.getTopicEditLink(getBaseUri(), getDatabaseId(), topic.getId(), view.getId(), readOnlyMode);
//        Link result = new Link(view.getType().getLinkId(), href);
//        result.setId(view.getId());
//        result.setName(view.getName());
//
//        result = postProcessViewLink(result, view);
//
//        return result;
//    }
//
//    protected Link postProcessViewLink(Link link, PrestoView view) {
//        ObjectNode extra = getViewExtraNode(view);
//        if (extra != null) {
//            Map<String, Object> params = getExtraParamsMap(extra);
//            if (params != null) {
//                link.setParams(params);
//            }
//        }
//        return link;
//    }

    public ObjectNode getSchemaExtraNode(PrestoSchemaProvider schemaProvider) {
        Object e = schemaProvider.getExtra();
        if (e != null && e instanceof ObjectNode) {
            return (ObjectNode)e;
        }
        return null;
    }

    public ObjectNode getTypeExtraNode(PrestoType type) {
        Object e = type.getExtra();
        if (e != null && e instanceof ObjectNode) {
            return (ObjectNode)e;
        }
        return null;
    }

    public ObjectNode getViewExtraNode(PrestoView view) {
        Object e = view.getExtra();
        if (e != null && e instanceof ObjectNode) {
            return (ObjectNode)e;
        }
        return null;
    }

    public ObjectNode getFieldExtraNode(PrestoField field) {
        Object e = field.getExtra();
        if (e != null && e instanceof ObjectNode) {
            return (ObjectNode)e;
        }
        return null;
    }

    public Map<String,Object> getExtraParamsMap(ObjectNode extra) {
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

    protected TopicType getTopicTypeWithNoLinks(PrestoType type) {
        return new TopicType(type.getId(), type.getName());
    }

    protected TopicType getTopicTypeWithCreateFieldInstanceLink(PrestoTopic topic, PrestoFieldUsage field, PrestoType createType) {
        TopicType result = new TopicType(createType.getId(), createType.getName());
        List<Link> links = new ArrayList<Link>();
        links.add(getCreateFieldInstanceLink(topic, field, createType));
        result.setLinks(links);
        return result;
    }
    
    protected Link getCreateFieldInstanceLink(PrestoTopic topic, PrestoFieldUsage field, PrestoType createType) {
        PrestoType type = field.getType();
        boolean isNewTopic = topic == null;
        String topicId = isNewTopic ? "_" + type.getId() : topic.getId();
        
        Link result = new Link("create-field-instance", Links.createFieldInstanceLink(getBaseUri(), getDatabaseId(), topicId, field.getId(), createType.getId()));
        result.setName(createType.getName());
        return result;
    }

    public FieldData addFieldValues(PrestoTopic topic, PrestoType type, PrestoFieldUsage field, 
            Integer index, FieldData fieldData) {

        PrestoDataProvider dataProvider = getDataProvider();
        PrestoChangeSet changeSet = dataProvider.newChangeSet(getChangeSetHandler());
        PrestoUpdate update = changeSet.updateTopic(topic, type);        

        Collection<? extends Object> addableValues = updateAndExtractValuesFromFieldData(changeSet, field, fieldData, true);

        if (index == null) {
            update.addValues(field, addableValues);
        } else {
            update.addValues(field, addableValues, index);        
        }

        changeSet.save();

        return getFieldData(update.getTopicAfterSave(), field, false);
    }

    public FieldData removeFieldValues(PrestoTopic topic, PrestoType type, PrestoFieldUsage field, FieldData fieldData) {

        PrestoDataProvider dataProvider = getDataProvider();
        PrestoChangeSet changeSet = dataProvider.newChangeSet(getChangeSetHandler());
        PrestoUpdate update = changeSet.updateTopic(topic, type);        
        
        Collection<? extends Object> removeableValues = updateAndExtractValuesFromFieldData(changeSet, field, fieldData, false);

        update.removeValues(field, removeableValues);

        changeSet.save();

        return getFieldData(update.getTopicAfterSave(), field, false);
    }

    public TopicView validateTopic(PrestoTopic topic, PrestoType type, PrestoView view, TopicView topicView) {
        Status status = new Status();
        
        topicView = processor.preProcessTopicView(topicView, topic, type, view, status);

        return processor.postProcessTopicView(topicView, topic, type, view, null);
    }

    public TopicView updateTopic(PrestoTopic topic, PrestoType type, PrestoView view, TopicView topicView) {
        Status status = new Status();
        
        topicView = processor.preProcessTopicView(topicView, topic, type, view, status);

        if (status.isValid()) {
            PrestoTopic result = updatePrestoTopic(topic, type, view, topicView);
            
            return processor.postProcessTopicView(getTopicView(result, type, view, false), topic, type, view, null);
                
        } else {
            return processor.postProcessTopicView(topicView, topic, type, view, null);
        }
    }

    protected PrestoTopic updatePrestoTopic(PrestoTopic topic, PrestoType type, PrestoView view, TopicView topicView) {

        PrestoDataProvider dataProvider = getDataProvider();
        PrestoChangeSet changeSet = dataProvider.newChangeSet(getChangeSetHandler());

        PrestoUpdate update;
        if (topic == null) {
            // TODO: add support for assigning topic ids?
            update = changeSet.createTopic(type);
        } else {
            update = changeSet.updateTopic(topic, type);
        }

        for (FieldData fieldData : topicView.getFields()) {

            String fieldId = fieldData.getId();
            PrestoFieldUsage field = type.getFieldById(fieldId, view);

            // ignore read-only or pageable fields 
            if (!field.isReadOnly() && !field.isPageable()) {
                update.setValues(field, updateAndExtractValuesFromFieldData(changeSet, field, fieldData, true));
            }
        }

        changeSet.save();

        return update.getTopicAfterSave();
    }

    private Collection<? extends Object> updateAndExtractValuesFromFieldData(PrestoChangeSet changeSet, PrestoFieldUsage field, FieldData fieldData, boolean resolveEmbedded) {
        Collection<Value> values = fieldData.getValues();
        Collection<Object> result = new ArrayList<Object>(values.size());

        if (!values.isEmpty()) {

            if (field.isReferenceField()) {
                if (field.isInline()) {
                    for (Value value : values) {
                        TopicView embeddedTopic = getEmbeddedTopic(value);
                        result.add(getInlineTopic(changeSet, field, embeddedTopic));
                    }                    
                } else {
                    List<String> valueIds = new ArrayList<String>(values.size());
                    for (Value value : values) {                
                        TopicView embeddedTopic = getEmbeddedTopic(value);
                        if (resolveEmbedded && embeddedTopic != null) {
                            result.add(updateEmbeddedTopic(field, embeddedTopic));
                        } else {
                            String valueId = getReferenceValue(value);
                            if (valueId != null) {
                                valueIds.add(getReferenceValue(value));
                            }
                        }
                    }
                    result.addAll(getDataProvider().getTopicsByIds(valueIds));
                }
            } else {
                for (Value value : values) {
                    result.add(getPrimitiveValue(value));
                }
            }
        }
        return result;
    }

    private PrestoTopic getInlineTopic(PrestoChangeSet changeSet, PrestoFieldUsage inlineField, TopicView embeddedTopic) {

        PrestoSchemaProvider schemaProvider = getSchemaProvider();

        String topicTypeId = embeddedTopic.getTopicTypeId();
        PrestoType type = schemaProvider.getTypeById(topicTypeId);

        if (!type.isInline()) {
            throw new RuntimeException("Type " + type.getId() + " is not an inline type.");
        }
        PrestoView view = inlineField.getValueView();

        String topicId = embeddedTopic.getTopicId();
        PrestoInlineTopicBuilder builder = changeSet.createInlineTopic(type, topicId);

        for (FieldData fieldData : embeddedTopic.getFields()) {

            String fieldId = fieldData.getId();
            PrestoFieldUsage field = type.getFieldById(fieldId, view);

            builder.setValues(field, updateAndExtractValuesFromFieldData(changeSet, field, fieldData, true));
        }
        
        return builder.build();
    }

    private PrestoTopic updateEmbeddedTopic(PrestoFieldUsage field, TopicView embeddedTopic) {

        PrestoDataProvider dataProvider = getDataProvider();
        PrestoSchemaProvider schemaProvider = getSchemaProvider();

        String topicId = embeddedTopic.getTopicId();

        PrestoTopic topic = null;
        PrestoType type;
        if (topicId == null) {
            String topicTypeId = embeddedTopic.getTopicTypeId();
            type = schemaProvider.getTypeById(topicTypeId);
        } else {
            topic = dataProvider.getTopicById(topicId);
            type = schemaProvider.getTypeById(topic.getTypeId());
        }

        PrestoView view = field.getValueView();

        return updatePrestoTopic(topic, type, view, embeddedTopic);
    }

    TopicView getEmbeddedTopic(Value value) {
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
    
//    public LinkList getAvailableFieldTypesInfo(PrestoTopic topic, PrestoFieldUsage field) {
//        LinkList result = new LinkList();
//        if (field.isCreatable()) {
//            Collection<PrestoType> availableFieldCreateTypes = getAvailableFieldCreateTypes(topic, field);
//            List<Link> links = new ArrayList<Link>(availableFieldCreateTypes.size());
//            for (PrestoType createType : availableFieldCreateTypes) {
//                links.add(getTopicTypeWithCreateFieldInstanceLink(topic, field, createType));
//            }                
//            result.setLinks(links);
//        } else {
//            result.setLinks(new ArrayList<Link>());
//        }
//        return result;
//    }
    
    public AvailableFieldTypes getAvailableFieldTypesInfo(PrestoTopic topic, PrestoFieldUsage field) {

        AvailableFieldTypes result = new AvailableFieldTypes();
        result.setId(field.getId());
        result.setName(field.getName());

        if (field.isCreatable()) {
            Collection<PrestoType> availableFieldCreateTypes = getAvailableFieldCreateTypes(topic, field);
            List<TopicType> types = new ArrayList<TopicType>(availableFieldCreateTypes.size());
            for (PrestoType createType : availableFieldCreateTypes) {
                types.add(getTopicTypeWithCreateFieldInstanceLink(topic, field, createType));
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
                links.add(new Link("create-instance", Links.createInstanceLink(getBaseUri(), getDatabaseId(), type.getId())));
            }

            if (tree) {
                Collection<TopicTypeTree> typesList = getAvailableTypes(type.getDirectSubTypes(), true);
                if (!typesList.isEmpty()) {
                    typeMap.setTypes(typesList);
                }
            } else {
                if (!type.getDirectSubTypes().isEmpty()) {
                    links.add(new Link("available-types-tree-lazy", Links.availableTypesTreeLazy(getBaseUri(), getDatabaseId(), type.getId())));
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
        links.add(new Link("available-types-tree", Links.getAvailableTypesTree(getBaseUri(), getDatabaseId())));
        links.add(new Link("edit-topic-by-id", Links.getEditTopicById(getBaseUri(), getDatabaseId())));
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
                    if (className != null) {
                        AvailableFieldCreateTypesResolver processor = Utils.newInstanceOf(className, AvailableFieldCreateTypesResolver.class);
                        if (processor != null) {
                            processor.setPresto(this);
                            return processor.getAvailableFieldCreateTypes(topic, field);
                        }
                    }
                }
                log.warn("Not able to extract extra.createTypes.class from field " + field.getId() + ": " + extra);                    
            } else if (!createTypesNode.isMissingNode()) {
                log.warn("Field " + field.getId() + " extra.createTypes is not an object: " + extra);
            }
        }
        return null;
    }

}
