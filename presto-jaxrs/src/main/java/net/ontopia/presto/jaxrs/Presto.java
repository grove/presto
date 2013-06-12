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
import net.ontopia.presto.jaxrs.process.ValueProcessor;
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
import net.ontopia.presto.spi.PrestoView.ViewType;
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

    public Topic getTopicAndProcess(PrestoContext context) {
        PrestoTopic topic = context.getTopic();
        PrestoType type = context.getType();
        PrestoView view = context.getView();

        Topic result = new Topic();
        result.setId(topic.getId());
        result.setName(topic.getName());

        String viewId = view.getId();
        result.setView(viewId);
        
        // create topic-views
        Collection<PrestoView> views = type.getViews(view);
        List<TopicView> topicViews = new ArrayList<TopicView>(views.size()); 
        for (PrestoView v : views) {
            if (ViewType.EDIT_IN_VIEW.equals(v.getType())) {
                PrestoContext subcontext = PrestoContext.create(this, topic, type, v, context.isReadOnly());
                
                if (viewId.equals(v.getId())) {
                    topicViews.add(getTopicView(subcontext));
                } else {
                    topicViews.add(getTopicViewRemote(subcontext));
                }
            }
        }
        result.setViews(topicViews);

        result = processor.postProcessTopic(result, context, null);

        return result;
    }
    
    public TopicView getTopicViewRemoteAndProcess(PrestoContext context) {
        TopicView result = getTopicViewRemote(context);
        
        result = processor.postProcessTopicView(result, context, null);

        return result;
    }
    
    public TopicView getTopicViewAndProcess(PrestoContext context) {
        TopicView result = getTopicView(context);
        
//        Status status = new Status();
//        result = processor.preProcessTopicView(result, topic, type, view, status);
        result = processor.postProcessTopicView(result, context, null);
        
        return result;
    }
    
    public TopicView getTopicViewRemote(PrestoContext context) {
        PrestoTopic topic = context.getTopic();
        PrestoView view = context.getView();

        TopicView result = TopicView.remoteView();
        
        result.setId(view.getId());
        result.setName(view.getName());
        result.setTopicId(topic.getId());
        result.setHref(Links.getTopicViewHref(getBaseUri(), getDatabaseId(), topic.getId(), view.getId(), context.isReadOnly()));

        return result;
    }
    
    public TopicView getTopicView(PrestoContext context) {
        return getTopicView(context, null);
    }
    
    public TopicView getTopicView(PrestoContext context, TopicView oldTopicView) {
        PrestoTopic topic = context.getTopic();
        PrestoType type = context.getType();
        PrestoView view = context.getView();
        
        TopicView result = TopicView.view();
        result.setId(view.getId());
        result.setName(view.getName());

        result.setTopicId(topic.getId());
        result.setTopicTypeId(type.getId());

        String href = Links.getTopicViewHref(getBaseUri(), getDatabaseId(), topic.getId(), view.getId(), context.isReadOnly());
        result.setHref(href);

        List<FieldData> fields = new ArrayList<FieldData>(); 
        boolean allFieldsReadOnly = true;
        for (PrestoFieldUsage field : type.getFields(view)) {
            if (!field.isHidden()) {
                fields.add(getFieldData(context, field));

                if (!field.isReadOnly() && !context.isReadOnly()) {
                    allFieldsReadOnly = false;
                }
            }
        }
        result.setFields(fields);

        List<Link> links = new ArrayList<Link>();
        links.add(Links.createLabel(type.getName()));

        if (!context.isReadOnly()) {
            if (!allFieldsReadOnly) {
                links.add(new Link("update", href));
            }
            if (type.isRemovable()) {
                links.add(new Link("delete", href));
            }
            if (type.isCreatable() && !type.isInline()) {
                links.add(new Link("create-instance", Links.createInstanceLink(getBaseUri(), getDatabaseId(), type.getId())));
            }
            // get 'parent' link from old topic view
            if (type.isInline() && topic.getId() != null && oldTopicView != null) {
                boolean foundParent = false;
                Collection<Link> oldLinks = oldTopicView.getLinks();
                if (oldLinks != null) {
                    for (Link oldLink : oldLinks) {
                        if (oldLink.getRel().equals("parent")) {
                            links.add(new Link("update-parent", oldLink.getHref()));
                            foundParent = true;
                        }
                    }
                }
                if (!foundParent) {
                    throw new RuntimeException("Could not find parent link in oldTopicView.");
                }
            }
        }
        
        result.setLinks(links);

        return result;
    }

    public abstract URI getBaseUri();
    
    public TopicView getNewTopicView(PrestoContext context) {
        return getNewTopicView(context, null, null);
    }

    public TopicView getNewTopicView(PrestoContext context, PrestoContext parentContext, String parentFieldId) {
        PrestoType type = context.getType();
        PrestoView view = context.getView();

        TopicView result = TopicView.view();
        result.setId(view.getId());
        result.setName("*" + type.getName() + "*");

        result.setTopicTypeId(type.getId());

        List<FieldData> fields = new ArrayList<FieldData>(); 
        for (PrestoFieldUsage field : type.getFields(view)) {
            if (!field.isHidden()) {
                fields.add(getFieldData(context, field));
            }
        }
        result.setFields(fields);
        
        List<Link> links = new ArrayList<Link>();
        links.add(Links.createLabel(type.getName()));
        
        String href;
        if (parentContext != null) {
            
            String parentTopicId = parentContext.getTopic().getId();
            String parentViewId = parentContext.getView().getId();
            
            links.add(new Link("create", Links.createNewTopicViewLinkParent(getBaseUri(), getDatabaseId(), type.getId(), view.getId(), parentTopicId, parentViewId, parentFieldId)));

            if (type.isInline()) {
                links.add(new Link("parent", Links.getTopicEditLink(getBaseUri(), getDatabaseId(), parentTopicId, parentViewId)));
            }
            href = Links.createFieldInstanceLink(getBaseUri(), getDatabaseId(), parentTopicId, parentViewId, parentFieldId, type.getId());
        } else {
            
            links.add(new Link("create", Links.createNewTopicViewLink(getBaseUri(), getDatabaseId(), type.getId(), view.getId())));
            
            href = Links.createInstanceLink(getBaseUri(), getDatabaseId(), type.getId());
        }
        result.setHref(href);
        result.setLinks(links);
        
//        Status status = new Status();
//        result = processor.preProcessTopicView(result, context, status);
        result = processor.postProcessTopicView(result, context, null);
        return result;
    }

    public FieldData getFieldData(PrestoContext context, PrestoFieldUsage field) {
        return getFieldData(context, field, 0, -1, true);
    }
    
    public FieldData getFieldDataNoValues(PrestoContext context, PrestoFieldUsage field) {
        return getFieldData(context, field, 0, -1, false);
    }

    public FieldData getFieldData(PrestoContext context, PrestoFieldUsage field, 
            int offset, int limit, boolean includeValues) {

        PrestoTopic topic = context.getTopic();
        PrestoType type = context.getType();
        PrestoView view = context.getView();

        String databaseId = getDatabaseId();

        boolean isNewTopic = context.isNewTopic();

        String topicId;
        if (isNewTopic) {
            topicId = "_" + type.getId();
        } else {
            topicId = topic.getId();
        }
        
        String parentViewId = view.getId();
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

        boolean isReadOnly = context.isReadOnly() || field.isReadOnly();
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
            if (!field.isReferenceField() || !getAvailableFieldValueTypes(context, field).isEmpty()) {
                boolean query = isCustomAvailableValuesQuery(context, field);
                fieldLinks.add(new Link("available-field-values", Links.availableFieldValues(getBaseUri(), databaseId, topicId, parentViewId, fieldId, query)));
            }
        }

        if (field.isPageable()) {
            fieldLinks.add(new Link("paging", Links.pagingLink(getBaseUri(), databaseId, topicId, parentViewId, fieldId)));    
        }

        fieldLinks.addAll(getCreateFieldInstanceLinks(context, field));

        if (!fieldLinks.isEmpty()) {
            fieldData.setLinks(fieldLinks);
        }

        // get values (truncated if neccessary)
        if (includeValues) {
            setFieldDataValues(offset, limit, context, field, fieldData);
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

    private Collection<? extends Link> getCreateFieldInstanceLinks(PrestoContext context, PrestoFieldUsage field) {
        Collection<PrestoType> availableFieldCreateTypes = getAvailableFieldCreateTypes(context, field);
        
        if (availableFieldCreateTypes.isEmpty()) {
            return Collections.emptyList();
        
        } else if (availableFieldCreateTypes.size() == 1) {
            PrestoType createType = availableFieldCreateTypes.iterator().next();
            Link link = getCreateFieldInstanceLink(context, field, createType);
            link.setName("Ny"); // FIXME: localize
            return Collections.singleton(link);
        } else {
            Collection<Link> links = new ArrayList<Link>(availableFieldCreateTypes.size());
            for (PrestoType createType : availableFieldCreateTypes) {
                links.add(getCreateFieldInstanceLink(context, field, createType));
            }
            Link link = new Link();
            link.setRel("create-field-instance");
            link.setName("Ny"); // FIXME: localize
            link.setLinks(links);
            return Collections.singleton(link);
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
    
    public FieldDataValues setFieldDataValues(int offset, int limit, 
            PrestoContext context, final PrestoFieldUsage field, FieldData fieldData) {

        // TODO: refactor to return DTO instead of mutating FieldData here
    
        PrestoTopic topic = context.getTopic();
        
        List<? extends Object> fieldValues;
        if (context.isNewTopic()) {
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
        
        ValueProcessor valueProcessor = createValueProcessor(context, field);

        int size = fieldValues.size();
        int start = 0;
        int end = size;

        List<Object> inputValues = new ArrayList<Object>(fieldValues.size());
        List<Value> outputValues = new ArrayList<Value>(fieldValues.size());
        for (int i=start; i < end; i++) {
            Object value = fieldValues.get(i);
            if (value != null) {
                Value efv = getExistingFieldValue(valueProcessor, context, field, value);
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
    
    private ValueProcessor createValueProcessor(PrestoContext context, PrestoFieldUsage field) {
        ObjectNode extra = getFieldExtraNode(field);
        if (extra != null) {
            JsonNode processorsNode = extra.path("valueProcessors");
            if (!processorsNode.isMissingNode()) {
                return processor.getProcessor(ValueProcessor.class, processorsNode);
            }
        }
        return null;
    }
    

    protected Value getExistingFieldValue(ValueProcessor valueProcessor, PrestoContext context, PrestoFieldUsage field, Object fieldValue) {
        if (fieldValue instanceof PrestoTopic) {
            PrestoTopic topicValue = (PrestoTopic)fieldValue;
            return getExistingTopicFieldValue(valueProcessor, context, field, topicValue);
        } else {
            String stringValue = fieldValue.toString();
            return getExistingStringFieldValue(valueProcessor, context, field, stringValue);
        }
    }

    protected Value getExistingStringFieldValue(ValueProcessor valueProcessor, PrestoContext context, PrestoFieldUsage field, String fieldValue) {
        Value result = new Value();
        result.setValue(fieldValue);
 
        if (valueProcessor != null) {
            String name = valueProcessor.getName(context, field, fieldValue);
            if (name != null) {
                result.setName(name);
            }
        }
        if (!context.isReadOnly() && !field.isReadOnly()) {
            result.setRemovable(Boolean.TRUE);
        }
        return result;
    }

    protected Value getExistingTopicFieldValue(ValueProcessor valueProcessor, PrestoContext context, PrestoFieldUsage field, PrestoTopic value) {
        Value result = new Value();
        result.setValue(value.getId());
        
        if (valueProcessor != null) {
            String name = valueProcessor.getName(context, field, value);
            if (name != null) {
                result.setName(name);
            }
        } else {
            String name = value.getName(field);
            if (name != null) {
                result.setName(name);
            }
        }

        result.setType(value.getTypeId());

        if (field.isEmbedded()) {
            PrestoType valueType = getSchemaProvider().getTypeById(value.getTypeId());
            
            PrestoContext subcontext = PrestoContext.createSubContext(this, context, value, valueType, field.getValueView(), context.isReadOnly());
            result.setEmbedded(getTopicView(subcontext));
        }

        if (!context.isReadOnly() && !field.isReadOnly()) {
            result.setRemovable(Boolean.TRUE);
        }

        List<Link> links = new ArrayList<Link>();
        if (field.isTraversable()) {
            PrestoView fieldsView = field.getValueView();
//            System.out.println("PC: " + field + " -> " + context.getParentContext());
            links.add(new Link("edit", Links.getTopicEditLink(getBaseUri(), getDatabaseId(), value.getId(), fieldsView.getId(), context.isReadOnly())));
        }
        result.setLinks(links);

        return result;
    }

    FieldData createFieldDataForParent(PrestoContext parentContext, String parentFieldId,
            Presto session, boolean readOnly, PrestoContext context, TopicView topicView) {
        PrestoType parentType = parentContext.getType();
        PrestoView parentView = parentContext.getView();
        PrestoFieldUsage parentField = parentType.getFieldById(parentFieldId, parentView);
        FieldData fieldData = session.getFieldData(context, parentField);
        Value value = new Value();
        value.setValue(topicView.getTopicId());
        value.setType(topicView.getTopicTypeId());
        value.setName(topicView.getName());
        value.setEmbedded(topicView);
        fieldData.setValues(Collections.singleton(value));
        return fieldData;
    }

    public AvailableFieldValues getAvailableFieldValuesInfo(PrestoContext context, PrestoFieldUsage field, String query) {

        AvailableFieldValues result = new AvailableFieldValues();
        result.setId(field.getId());
        result.setName(field.getName());
        result.setValues(getAllowedFieldValues(context, field, query));

        return result;
    }
    
    protected Collection<? extends Object> getAvailableFieldValues(PrestoContext context, PrestoFieldUsage field, String query) {
        Collection<? extends Object> result = getCustomAvailableValues(context, field, query);
        if (result != null) {
            return result;
        }
        return dataProvider.getAvailableFieldValues(context.getTopic(), field, query);
    }

    private boolean isCustomAvailableValuesQuery(PrestoContext context, PrestoFieldUsage field) {
        ObjectNode extra = getFieldExtraNode(field);
        if (extra != null) {
            JsonNode availableValuesNode = extra.path("availableValues");
            if (availableValuesNode.isObject()) {
                return availableValuesNode.path("query").getBooleanValue();
            }
        }
        return false;
    }
    
    private Collection<? extends Object> getCustomAvailableValues(PrestoContext context, PrestoFieldUsage field, String query) {
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
                            processor.setConfig((ObjectNode)availableValuesNode);
                            return processor.getAvailableFieldValues(context, field, query);
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

    protected Collection<Value> getAllowedFieldValues(PrestoContext context, PrestoFieldUsage field, String query) {
        Collection<? extends Object> availableFieldValues = getAvailableFieldValues(context, field, query);
        
        ValueProcessor valueProcessor = createValueProcessor(context, field);

        Collection<Value> result = new ArrayList<Value>(availableFieldValues.size());
        for (Object value : availableFieldValues) {
            result.add(getAllowedFieldValue(valueProcessor, context, field, value));
        }
//        result = processor.postProcessValues(result, context, field, null);

        return result;
    }
    
    protected Value getAllowedFieldValue(ValueProcessor valueProcessor, PrestoContext context, PrestoFieldUsage field, Object fieldValue) {
        if (fieldValue instanceof PrestoTopic) {
            PrestoTopic topicValue = (PrestoTopic)fieldValue;
            return getAllowedTopicFieldValue(valueProcessor, context, field, topicValue);
        } else {
            String stringValue = fieldValue.toString();
            return getAllowedStringFieldValue(valueProcessor, context, field, stringValue);
        }
    }

    protected Value getAllowedStringFieldValue(ValueProcessor valueProcessor, PrestoContext context, PrestoFieldUsage field, String fieldValue) {
        Value result = new Value();
        result.setValue(fieldValue);
        
        if (valueProcessor != null) {
            String name = valueProcessor.getName(context, field, fieldValue);
            if (name != null) {
                result.setName(name);
            }
        }
        return result;
    }

    protected Value getAllowedTopicFieldValue(ValueProcessor valueProcessor, PrestoContext context, PrestoFieldUsage field, PrestoTopic value) {
        Value result = new Value();
        result.setValue(value.getId());

        if (valueProcessor != null) {
            String name = valueProcessor.getName(context, field, value);
            if (name != null) {
                result.setName(name);
            }
        } else {
            String name = value.getName(field);
            if (name != null) {
                result.setName(name);
            }
        }

        result.setType(value.getTypeId());

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

    protected TopicType getTopicTypeWithCreateFieldInstanceLink(PrestoContext context, PrestoFieldUsage field, PrestoType createType) {
        TopicType result = new TopicType(createType.getId(), createType.getName());
        List<Link> links = new ArrayList<Link>();
        links.add(getCreateFieldInstanceLink(context, field, createType));
        result.setLinks(links);
        return result;
    }
    
    protected Link getCreateFieldInstanceLink(PrestoContext context, PrestoFieldUsage field, PrestoType createType) {
        PrestoType type = context.getType();
        String topicId;
        if (context.isNewTopic()) {
            topicId = "_" + type.getId();
        } else {
            topicId = context.getTopic().getId();
        }
        
        Link result = new Link("create-field-instance", Links.createFieldInstanceLink(getBaseUri(), getDatabaseId(), topicId, field.getView().getId(), field.getId(), createType.getId()));
        result.setName(createType.getName());
        return result;
    }

    public FieldData addFieldValues(PrestoContext context, PrestoFieldUsage field, 
            Integer index, FieldData fieldData) {

        PrestoTopic topic = context.getTopic();
        PrestoType type = context.getType();
        PrestoView view = context.getView();

        PrestoDataProvider dataProvider = getDataProvider();
        PrestoChangeSet changeSet = dataProvider.newChangeSet(getChangeSetHandler());
        PrestoUpdate update = changeSet.updateTopic(topic, type);        

        List<? extends Object> addableValues = updateAndExtractValuesFromFieldData(context, field, fieldData, true);

        if (index == null) {
            update.addValues(field, addableValues);
        } else {
            update.addValues(field, addableValues, index);        
        }

        changeSet.save();

        PrestoTopic topicAfterSave = update.getTopicAfterSave();

        PrestoContext subcontext = PrestoContext.create(this, topicAfterSave, type, view, context.isReadOnly());

        FieldData result = getFieldData(subcontext, field);
        return processor.postProcessFieldData(result, context, field, null);

    }

    public FieldData removeFieldValues(PrestoContext context, PrestoFieldUsage field, FieldData fieldData) {

        PrestoTopic topic = context.getTopic();
        PrestoType type = context.getType();
        PrestoView view = context.getView();

        PrestoDataProvider dataProvider = getDataProvider();
        PrestoChangeSet changeSet = dataProvider.newChangeSet(getChangeSetHandler());
        PrestoUpdate update = changeSet.updateTopic(topic, type);        
        
        List<? extends Object> removeableValues = updateAndExtractValuesFromFieldData(context, field, fieldData, false);

        update.removeValues(field, removeableValues);

        changeSet.save();

        PrestoTopic topicAfterSave = update.getTopicAfterSave();

        PrestoContext subcontext = PrestoContext.create(this, topicAfterSave, type, view, context.isReadOnly());
        
        FieldData result = getFieldData(subcontext, field);
        return processor.postProcessFieldData(result, context, field, null);
    }

    public TopicView validateTopic(PrestoContext context, TopicView topicView) {
        Status status = new Status();
        
        topicView = processor.preProcessTopicView(topicView, context, status);

        return processor.postProcessTopicView(topicView, context, null);
    }

    public TopicView updateTopic(PrestoContext context, TopicView topicView) {
        Status status = new Status();
        
        topicView = processor.preProcessTopicView(topicView, context, status);

        if (status.isValid()) {
            PrestoTopic result = updatePrestoTopic(context, topicView);
            PrestoContext newContext = PrestoContext.create(this, result, context.getType(), context.getView(), context.isReadOnly());
            TopicView newTopicView = getTopicView(newContext, topicView);
            return processor.postProcessTopicView(newTopicView, context, null);
                
        } else {
            return processor.postProcessTopicView(topicView, context, null);
        }
    }

    protected PrestoTopic updatePrestoTopic(PrestoContext context, TopicView topicView) {

        PrestoDataProvider dataProvider = getDataProvider();
        PrestoChangeSet changeSet = dataProvider.newChangeSet(getChangeSetHandler());

        PrestoType type = context.getType();
        PrestoView view = context.getView();

        if (type.isInline()) {
            PrestoTopic inlineTopic = buildInlineTopic(context, topicView);
            return inlineTopic;
        } else {
            PrestoUpdate update;
            if (context.isNewTopic()) {
                // TODO: add support for assigning topic ids?
                update = changeSet.createTopic(type);
            } else {
                update = changeSet.updateTopic(context.getTopic(), type);
            }
    
            for (FieldData fieldData : topicView.getFields()) {
    
                String fieldId = fieldData.getId();
                PrestoFieldUsage field = type.getFieldById(fieldId, view);
    
                // ignore read-only or pageable fields 
                if (!field.isReadOnly() && !field.isPageable()) {
                    update.setValues(field, updateAndExtractValuesFromFieldData(context, field, fieldData, true));
                }
            }
    
            changeSet.save();
    
            return update.getTopicAfterSave();
        }
    }

    private List<? extends Object> updateAndExtractValuesFromFieldData(PrestoContext context, PrestoFieldUsage field, FieldData fieldData, boolean resolveEmbedded) {
        Collection<Value> values = fieldData.getValues();
        List<Object> result = new ArrayList<Object>(values.size());

        if (!values.isEmpty()) {

            if (field.isReferenceField()) {
                if (field.isInline()) {
                    // build inline topics from field data
                    List<Object> newValues = new ArrayList<Object>();
                    for (Value value : values) {
                        TopicView embeddedTopic = getEmbeddedTopic(value);
                        if (embeddedTopic != null) {
                            newValues.add(buildInlineTopic(context, embeddedTopic));
                        } else {
                            String typeId = value.getType();
                            PrestoType type = schemaProvider.getTypeById(typeId);
                            newValues.add(buildInlineTopic(context, type, value.getValue()));                            
                        }
                    }
                    // merge new inline topics with existing ones
                    PrestoTopic topic = context.getTopic();
                    List<? extends Object> existingValues = topic.getValues(field);
                    result.addAll(mergeInlineTopics(newValues, existingValues));
                } else {
                    List<String> valueIds = new ArrayList<String>(values.size());
                    for (Value value : values) {                
                        TopicView embeddedTopic = getEmbeddedTopic(value);
                        if (resolveEmbedded && embeddedTopic != null) {
                            result.add(updateEmbeddedTopic(context, field, embeddedTopic));
                        } else {
                            String valueId = getReferenceValue(value);
                            if (valueId != null) {
                                valueIds.add(valueId);
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

    protected PrestoTopic buildInlineTopic(PrestoContext context, PrestoType type, String topicId) {
        PrestoInlineTopicBuilder builder = dataProvider.createInlineTopic(type, topicId);
        return builder.build();
    }
    
    protected PrestoTopic buildInlineTopic(PrestoContext context, TopicView embeddedTopic) {

        PrestoSchemaProvider schemaProvider = getSchemaProvider();

        String topicTypeId = embeddedTopic.getTopicTypeId();
        PrestoType type = schemaProvider.getTypeById(topicTypeId);

        if (!type.isInline()) {
            throw new RuntimeException("Type " + type.getId() + " is not an inline type.");
        }

        PrestoDataProvider dataProvider = getDataProvider();
        
        String topicId = embeddedTopic.getTopicId();
        
        PrestoInlineTopicBuilder builder = dataProvider.createInlineTopic(type, topicId);

        String viewId = embeddedTopic.getId();
        PrestoView view = type.getViewById(viewId);
        for (FieldData fieldData : embeddedTopic.getFields()) {

            String fieldId = fieldData.getId();
            PrestoFieldUsage field = type.getFieldById(fieldId, view);

            builder.setValues(field, updateAndExtractValuesFromFieldData(context, field, fieldData, true));
        }
        
        return builder.build();
    }

    protected PrestoTopic mergeInlineTopics(PrestoTopic t1, PrestoTopic t2) {
        String topicId = t1.getId();
        if (Utils.different(topicId, t2.getId())) {
            throw new IllegalArgumentException("Cannot merge topics with different ids: '" + topicId + "' and '" + t2.getId() + "'");
        }
        PrestoSchemaProvider schemaProvider = getSchemaProvider();
        PrestoType type = schemaProvider.getTypeById(t1.getTypeId());
        
        PrestoInlineTopicBuilder builder = dataProvider.createInlineTopic(type, topicId);
        
        //    n{ "a" : 1, "b" : 2, "c" : {"_" : 11, "x" : 1}          }  
        //    e{ "a" : 3,          "c" : {"_" : 11, "x" : 2, "y" : 3} } 
        // =>  { "a" : 1, "b" : 2, "c" : {"_" : 11, "x" : 1, "y" : 3} }

        for (PrestoField field : type.getFields()) {
            boolean hasValue1 = t1.hasValue(field);
            boolean hasValue2 = t2.hasValue(field);
            
            if (hasValue1 && hasValue2) {
                if (field.isInline()) {
                    List<? extends Object> merged = mergeInlineTopics(t1.getValues(field), t2.getValues(field));
                    builder.setValues(field, merged);
                } else {
                    builder.setValues(field, t1.getValues(field));
                }
            } else if (hasValue1) {
                builder.setValues(field, t1.getValues(field));
            } else if (hasValue2) {
                builder.setValues(field, t2.getValues(field));
            }
        }
        
        return builder.build();
    }
    
    protected List<? extends Object> mergeInlineTopics(List<? extends Object> v1, List<? extends Object> v2) {
        Map<String,Object> map1 = toMapTopics(v1);
        Map<String,Object> map2 = toMapTopics(v2);
        
        List<PrestoTopic> result = new ArrayList<PrestoTopic>(Math.max(v1.size(), v2.size())); 
        for (String topicId : map1.keySet()) {
            PrestoTopic t1 = (PrestoTopic)map1.get(topicId);
            PrestoTopic t2 = (PrestoTopic)map2.get(topicId);
            if (t1 != null && t2 != null) {
                result.add(mergeInlineTopics(t1, t2));
            } else if (t1 != null){
                result.add(t1);
            } else if (t2 != null) {
                result.add(t2);
            } else {
                throw new RuntimeException("Woot! Both t1 and t2 were null.");
            }
        }
        return result;
    }

    private Map<String,Object> toMapTopics(List<? extends Object> values) {
        Map<String,Object> result = new LinkedHashMap<String,Object>(values.size());
        for (Object value : values) {
            PrestoTopic topic = (PrestoTopic)value;
            result.put(topic.getId(), topic);
        }
        return result;
    }
    private PrestoTopic updateEmbeddedTopic(PrestoContext context, PrestoFieldUsage field, TopicView embeddedTopic) {

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

        PrestoContext subcontext = PrestoContext.create(this, topic, type, view, context.isReadOnly());

        return updatePrestoTopic(subcontext, embeddedTopic);
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
    
    public AvailableFieldTypes getAvailableFieldTypesInfo(PrestoContext context, PrestoFieldUsage field) {

        AvailableFieldTypes result = new AvailableFieldTypes();
        result.setId(field.getId());
        result.setName(field.getName());

        if (field.isCreatable()) {
            Collection<PrestoType> availableFieldCreateTypes = getAvailableFieldCreateTypes(context, field);
            List<TopicType> types = new ArrayList<TopicType>(availableFieldCreateTypes.size());
            for (PrestoType createType : availableFieldCreateTypes) {
                types.add(getTopicTypeWithCreateFieldInstanceLink(context, field, createType));
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
            if (type.isCreatable() && !type.isInline()) {
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
        links.add(new Link("edit-topic-by-id", Links.getTopicEditLinkById(getBaseUri(), getDatabaseId())));
        result.setLinks(links);      
        
        return result;
    }
    
    protected Collection<PrestoType> getAvailableFieldValueTypes(PrestoContext context, PrestoFieldUsage field) {
        return field.getAvailableFieldValueTypes();
    }
    
    protected Collection<PrestoType> getAvailableFieldCreateTypes(PrestoContext context, PrestoFieldUsage field) {
        Collection<PrestoType> result = getCustomAvailableFieldCreateTypes(context, field);
        if (result != null) {
            return result;
        }
        return field.getAvailableFieldCreateTypes();
    }

    private Collection<PrestoType> getCustomAvailableFieldCreateTypes(PrestoContext context, PrestoFieldUsage field) {
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
                            processor.setConfig((ObjectNode)createTypesNode);
                            return processor.getAvailableFieldCreateTypes(context, field);
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

    protected PrestoTopic findInlineTopicById(PrestoTopic topic, PrestoFieldUsage field, String topicId) {
        for (Object value : topic.getValues(field)) {
            if (value instanceof PrestoTopic) {
                PrestoTopic valueTopic = (PrestoTopic)value;
                if (topicId.equals(valueTopic.getId())) {
                    return valueTopic;
                }
            }
        }
        throw new RuntimeException("Could not find inline topic '" + topicId + "'");
    }

}
