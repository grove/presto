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
import net.ontopia.presto.jaxrs.process.ValueFactory;
import net.ontopia.presto.jaxrs.resolve.AvailableFieldCreateTypesResolver;
import net.ontopia.presto.jaxrs.resolve.AvailableFieldValuesResolver;
import net.ontopia.presto.jaxrs.sort.SortKeyGenerator;
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

    protected boolean isReadOnlyMode() {
        return false;
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
        PrestoContextRules rules = getPrestoContextRules(context);

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
                PrestoContext parentContext = context.getParentContext();
                PrestoFieldUsage parentField = context.getParentField();

                PrestoContext subcontext = PrestoContext.createSubContext(parentContext, parentField, topic, type, v);
                PrestoContextRules subrules = getPrestoContextRules(subcontext);

                if (viewId.equals(v.getId())) {
                    topicViews.add(getTopicView(subrules));
                } else {
                    if (isRemoteLoadView(v)) {
                        topicViews.add(getTopicViewRemote(subrules));
                    } else {
                        topicViews.add(getTopicView(subrules));
                    }
                }
            }
        }
        result.setViews(topicViews);

        result = processor.postProcessTopic(result, rules, null);

        return result;
    }

    private boolean isRemoteLoadView(PrestoView v) {
        ObjectNode viewExtra = (ObjectNode)v.getExtra();
        if (viewExtra != null) {
            JsonNode remoteView = viewExtra.path("remote-view");
            if (remoteView.isBoolean()) {
                return remoteView.getBooleanValue();
            }
        }
        return true;
    }

    public TopicView getTopicViewRemoteAndProcess(PrestoContextRules rules) {
        TopicView result = getTopicViewRemote(rules);

        result = processor.postProcessTopicView(result, rules, null);

        return result;
    }

    public TopicView getTopicViewAndProcess(PrestoContext context) {
        return getTopicViewAndProcess(getPrestoContextRules(context));
    }

    public TopicView getTopicViewAndProcess(PrestoContextRules rules) {
        TopicView result = getTopicView(rules);

        //        Status status = new Status();
        //        result = processor.preProcessTopicView(result, topic, type, view, status);
        result = processor.postProcessTopicView(result, rules, null);

        return result;
    }

    public TopicView getTopicViewRemote(PrestoContextRules rules) {
        PrestoContext context = rules.getContext();

        PrestoTopic topic = context.getTopic();
        PrestoType type = context.getType();
        PrestoView view = context.getView();

        TopicView result = TopicView.remoteView();

        result.setId(view.getId());
        result.setName(view.getName());
        result.setTopicId(topic.getId());

        String href;
        if (type.isInline()) {
            String topicId = context.getTopicId();
            PrestoContext parentContext = context.getParentContext();
            PrestoField parentField = context.getParentField();
            href = Links.getTopicViewHref(getBaseUri(), getDatabaseId(), parentContext, parentField, topicId, view.getId(), isReadOnlyMode());
        } else {
            href = Links.getTopicViewHref(getBaseUri(), getDatabaseId(), topic.getId(), view.getId(), isReadOnlyMode());
        }
        result.setHref(href);

        return result;
    }

    public PrestoContextRules getPrestoContextRules(PrestoContext context) {
        return new PrestoContextRules(this, context) {
            @Override
            public boolean isReadOnlyType() {
                return isReadOnlyMode() || super.isReadOnlyType();
            }
        };
    }

    public TopicView getTopicView(PrestoContextRules rules) {
        PrestoContext context = rules.getContext();

        PrestoTopic topic = context.getTopic();
        PrestoType type = context.getType();
        PrestoView view = context.getView();

        TopicView result = TopicView.view();
        result.setId(view.getId());
        result.setName(view.getName());

        result.setTopicId(topic.getId());
        result.setTopicTypeId(type.getId());

        String href;
        if (type.isInline()) {
            String topicId = context.getTopicId();
            PrestoContext parentContext = context.getParentContext();
            PrestoField parentField = context.getParentField();
            href = Links.getTopicViewHref(getBaseUri(), getDatabaseId(), parentContext, parentField, topicId, view.getId(), isReadOnlyMode());
        } else {
            href = Links.getTopicViewHref(getBaseUri(), getDatabaseId(), topic.getId(), view.getId(), isReadOnlyMode());
        }
        result.setHref(href);

        List<FieldData> fields = new ArrayList<FieldData>(); 
        boolean allFieldsReadOnly = true;
        for (PrestoFieldUsage field : type.getFields(view)) {
            if (!rules.isHiddenField(field)) {
                fields.add(getFieldData(rules, field));

                if (!rules.isReadOnlyField(field)) {
                    allFieldsReadOnly = false;
                }
            }
        }
        result.setFields(fields);

        List<Link> links = new ArrayList<Link>();
        links.add(Links.createLabel(type.getName()));

        if (!rules.isReadOnlyType()) {
            if (!allFieldsReadOnly && rules.isUpdatableType()) {
                links.add(new Link("update", href));
            }
            if (rules.isRemovableType()) {
                links.add(new Link("delete", href));
            }
            if (rules.isCreatableType() && !type.isInline()) {
                links.add(new Link("create-instance", Links.createInstanceLink(getBaseUri(), getDatabaseId(), type.getId())));
            }
        }
        result.setLinks(links);

        return result;
    }

    public abstract URI getBaseUri();

    public TopicView getNewTopicView(PrestoContext context) {

        PrestoContextRules rules = getPrestoContextRules(context);

        PrestoType type = context.getType();
        PrestoView view = context.getView();

        String typeId = type.getId();
        String viewId = view.getId();

        TopicView result = TopicView.view();
        result.setId(viewId);
        result.setName("*" + type.getName() + "*");

        result.setTopicTypeId(typeId);

        List<FieldData> fields = new ArrayList<FieldData>(); 
        for (PrestoFieldUsage field : type.getFields(view)) {
            if (!rules.isHiddenField(field)) {
                fields.add(getFieldData(rules, field));
            }
        }
        result.setFields(fields);

        List<Link> links = new ArrayList<Link>();
        links.add(Links.createLabel(type.getName()));

        links.add(new Link("create", Links.createNewTopicViewLink(getBaseUri(), getDatabaseId(), typeId, viewId)));

        result.setHref(Links.createInstanceLink(getBaseUri(), getDatabaseId(), typeId));
        result.setLinks(links);

        //        Status status = new Status();
        //        result = processor.preProcessTopicView(result, rules, status);
        result = processor.postProcessTopicView(result, rules, null);
        return result;
    }

    public TopicView getNewTopicView(PrestoContext parentContext, PrestoFieldUsage parentField, PrestoType type) {

        PrestoView view = parentField.getCreateView(type);

        String typeId = type.getId();
        String viewId = view.getId();

        String topicId = "_" + type.getId();

        PrestoContext subcontext = PrestoContext.createSubContext(this, parentContext, parentField, topicId, viewId);
        PrestoContextRules subrules = getPrestoContextRules(subcontext);

        TopicView result = TopicView.view();
        result.setId(viewId);
        result.setName("*" + type.getName() + "*");

        result.setTopicTypeId(typeId);

        List<FieldData> fields = new ArrayList<FieldData>(); 
        for (PrestoFieldUsage field : type.getFields(view)) {
            if (!subrules.isHiddenField(field)) {
                fields.add(getFieldData(subrules, field));
            }
        }
        result.setFields(fields);

        List<Link> links = new ArrayList<Link>();
        links.add(Links.createLabel(type.getName()));

        links.add(new Link("create", Links.createNewTopicViewLink(getBaseUri(), getDatabaseId(), parentContext, parentField, typeId, viewId)));

        result.setHref(Links.createFieldInstanceLink(getBaseUri(), getDatabaseId(), parentContext, parentField, type.getId()));
        result.setLinks(links);

        //        Status status = new Status();
        //        result = processor.preProcessTopicView(result, subrules, status);
        result = processor.postProcessTopicView(result, subrules, null);
        return result;
    }

    public FieldData getFieldData(PrestoContextRules rules, PrestoFieldUsage field) {
        return getFieldData(rules, field, 0, -1, true);
    }

    public FieldData getFieldDataNoValues(PrestoContextRules rules, PrestoFieldUsage field) {
        return getFieldData(rules, field, 0, -1, false);
    }

    public FieldData getFieldData(PrestoContextRules rules, PrestoFieldUsage field, 
            int offset, int limit, boolean includeValues) {

        PrestoContext context = rules.getContext();

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

        boolean isReadOnly = rules.isReadOnlyField(field);
        if (isReadOnly) {
            fieldData.setReadOnly(Boolean.TRUE);
        }

        PrestoContext parentContext = context.getParentContext();
        PrestoFieldUsage parentField = context.getParentField();

        boolean allowCreate = rules.isCreatableField(field);
        boolean allowAdd = rules.isAddableField(field);

        Collection<Link> fieldLinks = new LinkedHashSet<Link>();      
        if (field.isReferenceField()) {
            fieldData.setDatatype("reference");

            if (!isReadOnly) {
                boolean isSorted = rules.isSortedField(field);

                boolean allowRemove = rules.isRemovableField(field);
                boolean allowMove = !isSorted;

                if (allowAdd || allowCreate) {
                    if (!isNewTopic) {
                        fieldLinks.add(new Link("add-field-values", Links.addFieldValuesLink(getBaseUri(), databaseId, parentContext, parentField, topicId, parentViewId, fieldId, false)));
                        if (!isSorted) {
                            fieldLinks.add(new Link("add-field-values-at-index", Links.addFieldValuesLink(getBaseUri(), databaseId, parentContext, parentField, topicId, parentViewId, fieldId, true)));
                        }
                    }
                }
                if (allowRemove && !isNewTopic) {
                    fieldLinks.add(new Link("remove-field-values", Links.removeFieldValuesLink(getBaseUri(), databaseId, parentContext, parentField, topicId, parentViewId, fieldId)));
                }      

                if (allowMove && !isNewTopic) {
                    fieldLinks.add(new Link("move-field-values-to-index", Links.moveFieldValuesToIndex(getBaseUri(), databaseId, parentContext, parentField, topicId, parentViewId, fieldId)));
                }
            }
        } else {
            String dataType = field.getDataType();
            if (dataType != null) {
                fieldData.setDatatype(dataType);
            }
            if (!isReadOnly) {
                if (!isNewTopic) {
                    fieldLinks.add(new Link("add-field-values", Links.addFieldValuesLink(getBaseUri(), databaseId, parentContext, parentField, topicId, parentViewId, fieldId, false)));
                    fieldLinks.add(new Link("remove-field-values", Links.removeFieldValuesLink(getBaseUri(), databaseId, parentContext, parentField, topicId, parentViewId, fieldId)));
                    if (!rules.isSortedField(field)) {
                        fieldLinks.add(new Link("add-field-values-at-index", Links.addFieldValuesLink(getBaseUri(), databaseId, parentContext, parentField, topicId, parentViewId, fieldId, true)));
                        fieldLinks.add(new Link("move-field-values-to-index", Links.moveFieldValuesToIndex(getBaseUri(), databaseId, parentContext, parentField, topicId, parentViewId, fieldId)));
                    }
                }
            }
        }
        if (!isReadOnly && allowAdd) {
            // ISSUE: should add-values and remove-values be links on list result instead?
            if (!field.isReferenceField() || !getAvailableFieldValueTypes(context, field).isEmpty()) {
                boolean query = isCustomAvailableValuesQuery(context, field);
                fieldLinks.add(new Link("available-field-values", Links.availableFieldValues(getBaseUri(), databaseId, parentContext, parentField, topicId, parentViewId, fieldId, query)));
            }
        }

        if (!isReadOnly && allowCreate) {
            fieldLinks.addAll(getCreateFieldInstanceLinks(context, field));
        }

        if (rules.isPageableField(field)) {
            fieldLinks.add(new Link("paging", Links.pagingLink(getBaseUri(), databaseId, parentContext, parentField, topicId, parentViewId, fieldId)));    
        }

        if (!fieldLinks.isEmpty()) {
            fieldData.setLinks(fieldLinks);
        }

        // get values (truncated if neccessary)
        if (includeValues) {
            setFieldDataValues(offset, limit, rules, field, fieldData);
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
            PrestoContextRules rules, final PrestoFieldUsage field, FieldData fieldData) {

        // TODO: refactor to return DTO instead of mutating FieldData here

        PrestoContext context = rules.getContext();
        PrestoTopic topic = context.getTopic();

        List<? extends Object> fieldValues;
        if (context.isNewTopic()) {
            fieldValues = Collections.emptyList(); // TODO: support initial values
        } else {
            // server-side paging (only if not sorting)
            if (rules.isPageableField(field) && !rules.isSortedField(field)) {
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

        return setFieldDataValues(offset, limit, rules, field, fieldData, fieldValues);
    }

    public FieldDataValues setFieldDataValues(int offset, int limit,
            PrestoContextRules rules, final PrestoFieldUsage field, FieldData fieldData, List<? extends Object> fieldValues) {
        // sort the result
        if (rules.isSortedField(field)) {
            sortFieldValues(rules, field, fieldValues, rules.isSortedAscendingField(field));
        }

        ValueFactory valueFactory = createValueFactory(rules, field);

        int size = fieldValues.size();
        int start = 0;
        int end = size;

        List<Object> inputValues = new ArrayList<Object>(fieldValues.size());
        List<Value> outputValues = new ArrayList<Value>(fieldValues.size());
        for (int i=start; i < end; i++) {
            Object value = fieldValues.get(i);
            if (value != null) {
                Value efv = getExistingFieldValue(valueFactory, rules, field, value);
                outputValues.add(efv);
                inputValues.add(value);
            } else {
                size--;
            }
        }

        if (fieldData != null) {
            fieldData.setValues(outputValues);

            // figure out how to truncate result (offset/limit)
            if (rules.isPageableField(field) && rules.isSortedField(field)) {
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

    private void sortFieldValues(final PrestoContextRules rules, final PrestoFieldUsage field, List<? extends Object> fieldValues, final boolean ascending) {
        final SortKeyGenerator sortKeyGenerator = createSortKeyGenerator(field);
        if (sortKeyGenerator == null) {
            Collections.sort(fieldValues, new Comparator<Object>() {
                public int compare(Object o1, Object o2) {
                    String n1 = (o1 instanceof PrestoTopic) ? ((PrestoTopic)o1).getName(field) : (o1 == null ? null : o1.toString());
                    String n2 = (o2 instanceof PrestoTopic) ? ((PrestoTopic)o2).getName(field) : (o2 == null ? null : o2.toString());
                    return compareComparables(n1, n2);
                }
            });
        } else {
            Collections.sort(fieldValues, new Comparator<Object>() {
                public int compare(Object o1, Object o2) {
                    PrestoContext context = rules.getContext();
                    String n1 = sortKeyGenerator.getSortKey(context, field, ((PrestoTopic)o1));
                    String n2 = sortKeyGenerator.getSortKey(context, field, ((PrestoTopic)o2));
                    if (ascending) {
                        return compareComparables(n1, n2);
                    } else {
                        return compareComparables(n1, n2) * -1;
                    }
                }
            });

        }
    }

    private SortKeyGenerator createSortKeyGenerator(final PrestoFieldUsage field) {
        ObjectNode extra = getFieldExtraNode(field);
        if (extra != null) {
            JsonNode sortKeyNode = extra.path("sortKeyGenerator");
            if (sortKeyNode.isObject()) {
                JsonNode classNode = sortKeyNode.path("class");
                if (classNode.isTextual()) {
                    String className = classNode.getTextValue();
                    if (className != null) {
                        SortKeyGenerator skg = Utils.newInstanceOf(className, SortKeyGenerator.class);
                        if (skg != null) {
                            skg.setPresto(this);
                            skg.setConfig((ObjectNode)sortKeyNode);
                            return skg;
                        }
                    }
                }
                log.warn("Not able to extract extra.sortKeyGenerator.class from field " + field.getId() + ": " + extra);                    
            } else if (!sortKeyNode.isMissingNode()) {
                log.warn("Field " + field.getId() + " extra.sortKeyGenerator is not an object: " + extra);
            }
        }
        return null;
    }

    private ValueFactory createValueFactory(PrestoContextRules rules, PrestoFieldUsage field) {
        ObjectNode extra = getFieldExtraNode(field);
        if (extra != null) {
            JsonNode processorsNode = extra.path("valueFactory");
            if (!processorsNode.isMissingNode()) {
                return PrestoProcessor.getHandler(this, ValueFactory.class, processorsNode);
            }
        }
        return null;
    }

    protected Value getExistingFieldValue(ValueFactory valueFactory, PrestoContextRules rules, PrestoFieldUsage field, Object fieldValue) {
        if (fieldValue instanceof PrestoTopic) {
            PrestoTopic topicValue = (PrestoTopic)fieldValue;
            return getExistingFieldValueTopic(valueFactory, rules, field, topicValue);
        } else {
            String stringValue = fieldValue.toString();
            return getExistingFieldValueString(valueFactory, rules, field, stringValue);
        }
    }

    protected Value getExistingFieldValueString(ValueFactory valueFactory, PrestoContextRules rules, PrestoFieldUsage field, String value) {
        Value result;
        if (valueFactory != null) {
            result = valueFactory.createValue(rules, field, value);
        } else {
            result = new Value();
            result.setValue(value);
        }
        boolean isRemovableValue = !rules.isReadOnlyField(field) && rules.isRemovableFieldValue(field, value);
        if (isRemovableValue) {
            result.setRemovable(Boolean.TRUE);
        }
        return result;
    }

    protected Value getExistingFieldValueTopic(ValueFactory valueFactory, PrestoContextRules rules, PrestoFieldUsage field, PrestoTopic value) {
        Value result;
        if (valueFactory != null) {
            result = valueFactory.createValue(rules, field, value);
        } else {
            result = new Value();
            result.setValue(value.getId());
            result.setName(value.getName(field));
        }
        result.setType(value.getTypeId());

        PrestoContext context = rules.getContext();

        PrestoType valueType = getSchemaProvider().getTypeById(value.getTypeId());
        if (field.isEmbedded()) {

            PrestoView valueView = field.getValueView(valueType);
            PrestoContext subcontext = PrestoContext.createSubContext(context, field, value, valueType, valueView);
            PrestoContextRules subrules = getPrestoContextRules(subcontext);

            result.setEmbedded(getTopicView(subrules));
        }

        boolean isRemovableValue = !rules.isReadOnlyField(field) && rules.isRemovableFieldValue(field, value);
        if (isRemovableValue) {
            result.setRemovable(Boolean.TRUE);
        }

        List<Link> links = new ArrayList<Link>();
        if (rules.isTraversableField(field)) {
            PrestoView fieldsView = field.getEditView(valueType);
            if (valueType.isInline()) {
                links.add(new Link("edit", Links.getTopicEditLink(getBaseUri(), getDatabaseId(), context, field, value.getId(), fieldsView.getId(), isReadOnlyMode())));
            } else {
                links.add(new Link("edit", Links.getTopicEditLink(getBaseUri(), getDatabaseId(), value.getId(), fieldsView.getId(), isReadOnlyMode())));
            }
        }
        result.setLinks(links);

        return result;
    }

    public AvailableFieldValues getAvailableFieldValuesInfo(PrestoContextRules rules, PrestoFieldUsage field, String query) {

        AvailableFieldValues result = new AvailableFieldValues();
        result.setId(field.getId());
        result.setName(field.getName());
        result.setValues(getAllowedFieldValues(rules, field, query));

        return result;
    }

    protected Collection<? extends Object> getAvailableFieldValues(PrestoContextRules rules, PrestoFieldUsage field, String query) {
        Collection<? extends Object> result = getCustomAvailableValues(rules, field, query);
        if (result != null) {
            return result;
        }
        PrestoTopic topic = rules.getContext().getTopic();
        if (!rules.isReadOnlyField(field) && rules.isAddableField(field)) {
            return dataProvider.getAvailableFieldValues(topic, field, query);
        } else {
            return Collections.emptyList();
        }
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

    private Collection<? extends Object> getCustomAvailableValues(PrestoContextRules rules, PrestoFieldUsage field, String query) {
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
                            return processor.getAvailableFieldValues(rules.getContext(), field, query);
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

    protected Collection<Value> getAllowedFieldValues(PrestoContextRules rules, PrestoFieldUsage field, String query) {
        Collection<? extends Object> availableFieldValues = getAvailableFieldValues(rules, field, query);

        ValueFactory valueFactory = createValueFactory(rules, field);

        Collection<Value> result = new ArrayList<Value>(availableFieldValues.size());
        for (Object value : availableFieldValues) {
            result.add(getAllowedFieldValue(valueFactory, rules, field, value));
        }

        return result;
    }

    protected Value getAllowedFieldValue(ValueFactory valueFactory, PrestoContextRules rules, PrestoFieldUsage field, Object fieldValue) {
        if (fieldValue instanceof PrestoTopic) {
            PrestoTopic topicValue = (PrestoTopic)fieldValue;
            return getAllowedFieldValueTopic(valueFactory, rules, field, topicValue);
        } else {
            String stringValue = fieldValue.toString();
            return getAllowedFieldValueString(valueFactory, rules, field, stringValue);
        }
    }

    protected Value getAllowedFieldValueString(ValueFactory valueFactory, PrestoContextRules rules, PrestoFieldUsage field, String value) {
        Value result;
        if (valueFactory != null) {
            result = valueFactory.createValue(rules, field, value);
        } else {
            result = new Value();
            result.setValue(value);
        }
        return result;
    }

    protected Value getAllowedFieldValueTopic(ValueFactory valueFactory, PrestoContextRules rules, PrestoFieldUsage field, PrestoTopic value) {
        Value result;
        if (valueFactory != null) {
            result = valueFactory.createValue(rules, field, value);
        } else {
            result = new Value();
            result.setValue(value.getId());
            result.setName(value.getName(field));
        }
        result.setType(value.getTypeId());

        List<Link> links = new ArrayList<Link>();
        if (rules.isTraversableField(field)) {
            PrestoType valueType = getSchemaProvider().getTypeById(value.getTypeId());
            PrestoView fieldsView = field.getEditView(valueType);
            links.add(new Link("edit", Links.getTopicEditLink(getBaseUri(), getDatabaseId(), value.getId(), fieldsView.getId(), false)));
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
        Link result = new Link("create-field-instance", Links.createFieldInstanceLink(getBaseUri(), getDatabaseId(), context, field, createType.getId()));
        result.setName(createType.getName());
        return result;
    }

    public FieldData getFieldData(PrestoTopic topic, PrestoFieldUsage field) {
        PrestoType type = field.getType();
        PrestoView view = field.getView();

        PrestoContext context = PrestoContext.create(topic, type, view);
        PrestoContextRules rules = getPrestoContextRules(context);

        FieldData result = getFieldData(rules, field);

        return processor.postProcessFieldData(result, rules, field, null);
    }

    public FieldData addFieldValues(PrestoContextRules rules, PrestoFieldUsage field, Integer index, FieldData fieldData) {
        boolean resolveEmbedded = true;
        boolean includeExisting = false;
        List<? extends Object> addableValues = updateAndExtractValuesFromFieldData(rules, field, fieldData, resolveEmbedded, includeExisting);
        
        // TODO: filter out !rules.isAddableFieldValue(field, value)
        
        PrestoTopic topicAfterSave = addFieldValues(rules, field, addableValues, index);

        return getFieldData(topicAfterSave, field);
    }

    public FieldData removeFieldValues(PrestoContextRules rules, PrestoFieldUsage field, FieldData fieldData) {
        boolean resolveEmbedded = false;
        boolean includeExisting = false;
        List<? extends Object> removeableValues = updateAndExtractValuesFromFieldData(rules, field, fieldData, resolveEmbedded, includeExisting);
        
        // TODO: filter out !rules.isRemovableFieldValue(field, value)

        PrestoTopic topicAfterSave = removeFieldValues(rules, field, removeableValues);

        return getFieldData(topicAfterSave, field);
    }

    protected PrestoTopic updateFieldValues(PrestoContext context, PrestoFieldUsage field, List<? extends Object> updateableValues) {
        PrestoTopic topic = context.getTopic();
        PrestoType type = context.getType();

        PrestoDataProvider dataProvider = getDataProvider();
        PrestoChangeSet changeSet = dataProvider.newChangeSet(getChangeSetHandler());
        PrestoUpdate update = changeSet.updateTopic(topic, type);        

        List<? extends Object> existingValues = topic.getValues(field);
        update.setValues(field, mergeInlineTopics(updateableValues, existingValues, true)); // TODO: check if inline field first?
        changeSet.save();

        return update.getTopicAfterSave();
    }

    public PrestoTopic addFieldValues(PrestoContextRules rules, PrestoFieldUsage field, List<? extends Object> addableValues, Integer index) {
        PrestoContext context = rules.getContext();
        PrestoTopic topic = context.getTopic();
        PrestoType type = context.getType();

        PrestoDataProvider dataProvider = getDataProvider();
        PrestoChangeSet changeSet = dataProvider.newChangeSet(getChangeSetHandler());
        PrestoUpdate update = changeSet.updateTopic(topic, type);        

        if (index == null) {
            update.addValues(field, addableValues);
        } else {
            update.addValues(field, addableValues, index);        
        }
        changeSet.save();

        return update.getTopicAfterSave();
    }

    public PrestoTopic removeFieldValues(PrestoContextRules rules, PrestoFieldUsage field, List<? extends Object> removeableValues) {
        PrestoContext context = rules.getContext();
        PrestoTopic topic = context.getTopic();
        PrestoType type = context.getType();

        PrestoDataProvider dataProvider = getDataProvider();
        PrestoChangeSet changeSet = dataProvider.newChangeSet(getChangeSetHandler());
        PrestoUpdate update = changeSet.updateTopic(topic, type);        

        update.removeValues(field, removeableValues);
        changeSet.save();

        return update.getTopicAfterSave();
    }

    public TopicView validateTopic(PrestoContext context, TopicView topicView) {
        PrestoContextRules rules = getPrestoContextRules(context);
        Status status = new Status();

        topicView = processor.preProcessTopicView(topicView, rules, status);

        return processor.postProcessTopicView(topicView, rules, null);
    }

    public Object updateTopic(PrestoContext context, TopicView topicView, boolean returnParent) {
        PrestoContextRules rules = getPrestoContextRules(context);
        Status status = new Status();

        topicView = processor.preProcessTopicView(topicView, rules, status);

        if (status.isValid()) {

            PrestoTopic result = updatePrestoTopic(rules, topicView);

            // update inline topic in field of parent
            PrestoContext parentContext = context.getParentContext();
            PrestoFieldUsage parentField = context.getParentField();
            PrestoContext newContext;
            if (parentContext != null && returnParent) {
                PrestoTopic updatedParent = updateFieldValues(parentContext, parentField, Collections.singletonList(result));
                newContext = PrestoContext.newContext(parentContext, updatedParent);
            } else {            
                newContext = PrestoContext.createSubContext(parentContext, parentField, result, context.getType(), context.getView());
            }

            if (context.isNewTopic() && parentContext == null) {
                return getTopicAndProcess(newContext);
            } else {
                return getTopicViewAndProcess(newContext);
            }
        } else {
            return processor.postProcessTopicView(topicView, rules, null);
        }
    }

    protected PrestoTopic updatePrestoTopic(PrestoContextRules rules, TopicView topicView) {

        PrestoDataProvider dataProvider = getDataProvider();
        PrestoChangeSet changeSet = dataProvider.newChangeSet(getChangeSetHandler());

        PrestoContext context = rules.getContext();
        PrestoType type = context.getType();
        PrestoView view = context.getView();

        if (type.isInline()) {
            PrestoTopic inlineTopic = buildInlineTopic(context.getParentContext(), context.getParentField(), topicView);
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
                if (!rules.isReadOnlyField(field) && !rules.isPageableField(field)) {

                    boolean resolveEmbedded = true;
                    boolean includeExisting = false;
                    List<? extends Object> values = updateAndExtractValuesFromFieldData(rules, field, fieldData, resolveEmbedded, includeExisting);

                    // TODO: filter out !rules.isAddableFieldValue(field, value) OR updatable?
                    
                    update.setValues(field, values);
                }
            }

            changeSet.save();

            return update.getTopicAfterSave();
        }
    }

    private List<? extends Object> updateAndExtractValuesFromFieldData(PrestoContextRules rules, PrestoFieldUsage field, FieldData fieldData, 
            boolean resolveEmbedded, boolean includeExisting) {
        Collection<Value> values = fieldData.getValues();
        List<Object> result = new ArrayList<Object>(values.size());

        if (!values.isEmpty()) {

            if (field.isReferenceField()) {
                PrestoContext context = rules.getContext();
                if (field.isInline()) {
                    // build inline topics from field data
                    List<Object> newValues = new ArrayList<Object>();
                    for (Value value : values) {
                        TopicView embeddedTopic = getEmbeddedTopic(value);
                        if (embeddedTopic != null) {
                            newValues.add(buildInlineTopic(context, field, embeddedTopic));
                        } else {
                            String typeId = value.getType();
                            PrestoType type = schemaProvider.getTypeById(typeId);
                            newValues.add(buildInlineTopic(context, type, value.getValue()));                            
                        }
                    }
                    // merge new inline topics with existing ones
                    if (context.isNewTopic()) {
                        result.addAll(newValues);
                    } else {
                        PrestoTopic topic = context.getTopic();
                        List<? extends Object> existingValues = topic.getValues(field);
                        result.addAll(mergeInlineTopics(newValues, existingValues, includeExisting));
                    }
                } else {
                    List<String> valueIds = new ArrayList<String>(values.size());
                    for (Value value : values) {                
                        TopicView embeddedTopic = getEmbeddedTopic(value);
                        if (resolveEmbedded && embeddedTopic != null) {
                            result.add(updateEmbeddedTopic(rules, field, embeddedTopic));
                        } else {
                            String valueId = getReferenceValue(value);
                            if (valueId != null) {
                                valueIds.add(valueId);
                            }
                        }
                    }
                    if (!valueIds.isEmpty()) {
                        result.addAll(getDataProvider().getTopicsByIds(valueIds));
                    }
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

    protected PrestoTopic buildInlineTopic(PrestoContext parentContext, PrestoFieldUsage parentField, TopicView embeddedTopic) {

        PrestoSchemaProvider schemaProvider = getSchemaProvider();

        String topicId = embeddedTopic.getTopicId();
        String topicTypeId = embeddedTopic.getTopicTypeId();
        String viewId = embeddedTopic.getId();

        PrestoType type = schemaProvider.getTypeById(topicTypeId);

        if (!type.isInline()) {
            throw new RuntimeException("Type " + type.getId() + " is not an inline type.");
        }

        PrestoTopic topic;
        if (parentContext.isNewTopic() || topicId == null) {
            topic = null;
        } else {
            PrestoTopic parentTopic = parentContext.getTopic();
            topic = findInlineTopicById(parentTopic, parentField, topicId);
        }
        PrestoView view = type.getViewById(viewId);
        PrestoContext subcontext = PrestoContext.createSubContext(parentContext, parentField, topic, type, view);
        PrestoContextRules subrules = getPrestoContextRules(subcontext);

        PrestoDataProvider dataProvider = getDataProvider();

        PrestoInlineTopicBuilder builder = dataProvider.createInlineTopic(type, topicId);

        for (FieldData fieldData : embeddedTopic.getFields()) {

            String fieldId = fieldData.getId();
            PrestoFieldUsage field = type.getFieldById(fieldId, view);

            boolean resolveEmbedded = true;
            boolean includeExisting = false;
            List<? extends Object> values = updateAndExtractValuesFromFieldData(subrules, field, fieldData, resolveEmbedded, includeExisting);
            builder.setValues(field, values);
        }

        return builder.build();
    }

    protected PrestoTopic mergeInlineTopic(PrestoTopic t1, PrestoTopic t2) {
        String topicId = t1.getId();
        if (Utils.different(topicId, t2.getId())) {
            throw new IllegalArgumentException("Cannot merge topics with different ids: '" + topicId + "' and '" + t2.getId() + "'");
        }
        PrestoSchemaProvider schemaProvider = getSchemaProvider();
        PrestoType type = schemaProvider.getTypeById(t1.getTypeId());

        if (!type.isInline()) {
            log.warn("Attempted to merge non-inline topics: " + t1.getId() + " and " + t2.getId());
        }
        PrestoInlineTopicBuilder builder = dataProvider.createInlineTopic(type, topicId);

        //    n{ "a" : 1, "b" : 2, "c" : {"_" : 11, "x" : 1}          }  
        //    e{ "a" : 3,          "c" : {"_" : 11, "x" : 2, "y" : 3} } 
        // =>  { "a" : 1, "b" : 2, "c" : {"_" : 11, "x" : 1, "y" : 3} }

        for (PrestoField field : type.getFields()) {
            boolean hasValue1 = t1.hasValue(field);
            boolean hasValue2 = t2.hasValue(field);

            if (hasValue1 && hasValue2) {
                if (field.isInline()) {
                    boolean includeExisting = false;
                    Collection<? extends Object> merged = mergeInlineTopics(t1.getValues(field), t2.getValues(field), includeExisting);
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

    protected Collection<? extends Object> mergeInlineTopics(List<? extends Object> valuesNew, List<? extends Object> valuesExisting, 
            boolean includeExisting) {
        Map<String,Object> mapNew = toMapTopics(valuesNew);
        Map<String,Object> mapExisting = toMapTopics(valuesExisting);

        Map<String,PrestoTopic> result = new LinkedHashMap<String,PrestoTopic>(Math.max(valuesNew.size(), valuesExisting.size()));
        if (includeExisting) {
            // keep order of existing
            for (String topicId : mapExisting.keySet()) {
                PrestoTopic topicNew = (PrestoTopic)mapNew.get(topicId);
                PrestoTopic topicExisting = (PrestoTopic)mapExisting.get(topicId);
                if (topicNew != null) {
                    result.put(topicId, mergeInlineTopic(topicNew, topicExisting));
                } else {
                    result.put(topicId, topicExisting);
                }
            }
            // add remaining new at end
            for (Object value : valuesNew) {
                PrestoTopic topic = (PrestoTopic)value;
                String topicId = topic.getId();
                if (!result.containsKey(topicId)) {
                    result.put(topicId, topic);
                }
            }
        } else {
            // keep order of new. merge common only
            for (String topicId : mapNew.keySet()) {
                PrestoTopic topicNew = (PrestoTopic)mapNew.get(topicId);
                PrestoTopic topicExisting = (PrestoTopic)mapExisting.get(topicId);
                if (topicExisting != null) {
                    result.put(topicId, mergeInlineTopic(topicNew, topicExisting));
                } else {
                    result.put(topicId, topicNew);
                }
            }
        }
        return result.values();
    }

    private Map<String,Object> toMapTopics(List<? extends Object> values) {
        Map<String,Object> result = new LinkedHashMap<String,Object>(values.size());
        for (Object value : values) {
            PrestoTopic topic = (PrestoTopic)value;
            result.put(topic.getId(), topic);
        }
        return result;
    }

    private PrestoTopic updateEmbeddedTopic(PrestoContextRules rules, PrestoFieldUsage field, TopicView embeddedTopic) {

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

        PrestoView valueView = field.getValueView(type);

        PrestoContext subcontext = PrestoContext.create(topic, type, valueView);
        PrestoContextRules subrules = getPrestoContextRules(subcontext);

        return updatePrestoTopic(subrules, embeddedTopic);
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

    public AvailableFieldTypes getAvailableFieldTypesInfo(PrestoContextRules rules, PrestoFieldUsage field) {

        AvailableFieldTypes result = new AvailableFieldTypes();
        result.setId(field.getId());
        result.setName(field.getName());

        if (rules.isCreatableField(field)) {
            PrestoContext context = rules.getContext();
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

    protected PrestoTopic findInlineTopicById(PrestoTopic parentTopic, PrestoFieldUsage field, String topicId) {
        for (Object value : parentTopic.getValues(field)) {
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
