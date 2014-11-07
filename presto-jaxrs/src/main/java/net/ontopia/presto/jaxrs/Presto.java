package net.ontopia.presto.jaxrs;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.ontopia.presto.jaxb.AvailableFieldValues;
import net.ontopia.presto.jaxb.Database;
import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Layout;
import net.ontopia.presto.jaxb.Link;
import net.ontopia.presto.jaxb.Topic;
import net.ontopia.presto.jaxb.TopicView;
import net.ontopia.presto.jaxb.Value;
import net.ontopia.presto.jaxrs.PrestoProcessor.Status;
import net.ontopia.presto.jaxrs.action.FieldAction;
import net.ontopia.presto.jaxrs.constraints.InvalidValueTypeConstraintException;
import net.ontopia.presto.jaxrs.constraints.NotAddableValueConstraintException;
import net.ontopia.presto.jaxrs.constraints.NotInlineTypeConstraintException;
import net.ontopia.presto.jaxrs.constraints.NotMovableValueConstraintException;
import net.ontopia.presto.jaxrs.constraints.NotRemovableValueConstraintException;
import net.ontopia.presto.jaxrs.links.DefaultLinks;
import net.ontopia.presto.jaxrs.links.Links;
import net.ontopia.presto.jaxrs.process.ValueFactory;
import net.ontopia.presto.jaxrs.resolve.AvailableFieldCreateTypesResolver;
import net.ontopia.presto.jaxrs.resolve.AvailableFieldValuesResolver;
import net.ontopia.presto.jaxrs.sort.DefaultSortKeyGenerator;
import net.ontopia.presto.jaxrs.sort.SortKeyGenerator;
import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoDataProvider.ChangeSetHandler;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoInlineTopicBuilder;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoTopic.Projection;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoUpdate;
import net.ontopia.presto.spi.PrestoView;
import net.ontopia.presto.spi.PrestoView.ViewType;
import net.ontopia.presto.spi.rules.ContextPathExpressions;
import net.ontopia.presto.spi.utils.AbstractHandler;
import net.ontopia.presto.spi.utils.ExtraUtils;
import net.ontopia.presto.spi.utils.FieldValues;
import net.ontopia.presto.spi.utils.PrestoAttributes;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextField;
import net.ontopia.presto.spi.utils.PrestoContextRules;
import net.ontopia.presto.spi.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public abstract class Presto {

    private static Logger log = LoggerFactory.getLogger(Presto.class);

    public enum Rel {
        REL_TOPIC_EDIT_BY_ID("edit-topic-by-id"),
        REL_TOPIC_CREATE("create"),
        REL_TOPIC_EDIT("edit"),
        REL_TOPIC_UPDATE("update"),
        REL_TOPIC_DELETE("delete"),

        REL_TOPIC_TEMPLATE("topic-template"),
        REL_TOPIC_TEMPLATE_FIELD("topic-template-field"),

        REL_FIELD_PAGING("paging"),
        REL_AVAILABLE_FIELD_VALUES("available-field-values"),
        REL_ADD_FIELD_VALUES("add-field-values"),
        REL_ADD_FIELD_VALUES_AT_INDEX("add-field-values-at-index"),
        REL_MOVE_FIELD_VALUES_TO_INDEX("move-field-values-to-index"),
        REL_REMOVE_FIELD_VALUES("remove-field-values"),

        REL_ONCHANGE("onchange");

        private final String rel;

        Rel(String rel) {
            this.rel = rel;
        }
        
        public String getRel() {
            return rel;
        }
        
    }

    private final String databaseId;
    private final String databaseName;

    private final PrestoSchemaProvider schemaProvider;
    private final PrestoDataProvider dataProvider;
    private final PrestoAttributes attributes;
    
    private final PrestoProcessor processor;
    private final Links lx;

    public Presto(String databaseId, String databaseName, PrestoSchemaProvider schemaProvider, PrestoDataProvider dataProvider, PrestoAttributes attributes) {
        this.databaseId = databaseId;
        this.databaseName = databaseName;
        this.schemaProvider = schemaProvider;
        this.dataProvider = dataProvider;
        this.attributes = attributes;
        this.processor = new PrestoProcessor(this);
        this.lx = createLinks(getBaseUri(), getDatabaseId());
    }

    protected Links createLinks(URI baseUri, String databaseId) {
        return new DefaultLinks(baseUri, databaseId);
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

    public Links getLinks() {
        return lx;
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

    public PrestoAttributes getAttributes() {
        return attributes;
    }

    public PrestoProcessor getProcessor() {
        return processor;
    }

    public PrestoChangeSet newChangeSet() {
        PrestoDataProvider dataProvider = getDataProvider();
        return dataProvider.newChangeSet(getChangeSetHandler());
    }
    
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

        ObjectNode extra = ExtraUtils.getTypeExtraNode(type);
        Layout layout = getLayout(extra);
        if (layout != null) {
            result.setLayout(layout);
        }
        
        // create topic-views
        Collection<PrestoView> views = type.getViews(view);
        List<TopicView> topicViews = new ArrayList<TopicView>(views.size()); 
        for (PrestoView v : views) {
            if (!rules.isHiddenView(v)) {
                PrestoContext parentContext = context.getParentContext();
                PrestoField parentField = context.getParentField();

                PrestoContext subcontext = PrestoContext.createSubContext(parentContext, parentField, topic, type, v);
                PrestoContextRules subrules = getPrestoContextRules(subcontext);

                ViewType viewType = v.getType();

                if (ViewType.NORMAL_VIEW.equals(viewType)) {

                    if (viewId.equals(v.getId())) {
                        topicViews.add(getTopicView(subrules));
                    } else {
                        if (isRemoteLoadView(v)) {
                            topicViews.add(getTopicViewRemote(subrules, false));
                        } else {
                            topicViews.add(getTopicView(subrules));
                        }
                    }
                } else if (ViewType.EXTERNAL_VIEW.equals(viewType)) {
                    topicViews.add(getTopicViewRemote(subrules, true));
                }
            }
        }
        result.setViews(topicViews);

        result = processor.postProcessTopic(result, rules, null);

        return result;
    }

    private Layout getLayout(ObjectNode extra) {
        Layout layout = null;
        if (extra != null) {
            JsonNode layoutNode = extra.path("layout");
            if (layoutNode.isObject()) {
                ObjectMapper mapper = new ObjectMapper();
                try {
                    layout = mapper.convertValue(layoutNode, Layout.class);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return layout;
    }

    private boolean isRemoteLoadView(PrestoView v) {
        ObjectNode viewExtra = ExtraUtils.getViewExtraNode(v);
        if (viewExtra != null) {
            JsonNode remoteView = viewExtra.path("remote-view");
            if (remoteView.isBoolean()) {
                return remoteView.booleanValue();
            }
        }
        return true;
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

    public TopicView getTopicViewRemote(PrestoContextRules rules, boolean external) {
        PrestoContext context = rules.getContext();

        PrestoTopic topic = context.getTopic();
        PrestoType type = context.getType();
        PrestoView view = context.getView();

        TopicView result = TopicView.remoteView();

        result.setId(view.getId());
        result.setName(view.getName());
        result.setTopicId(topic.getId());

        String href;
        if (external) {
            href = lx.topicViewExternalHref(context);
        } else {
            String topicId = context.getTopicId();
            PrestoContext parentContext = context.getParentContext();
            PrestoField parentField = context.getParentField();
            href = lx.topicViewHref(parentContext, parentField, topicId, type, view, isReadOnlyMode());
        }
        result.setHref(href);

        return result;
    }

    public PrestoContextRules getPrestoContextRules(PrestoContext context) {
        return new PrestoContextRules(getDataProvider(), getSchemaProvider(), context) {
            @Override
            public boolean isReadOnlyType() {
                return isReadOnlyMode() || super.isReadOnlyType();
            }
            @Override
            public PrestoAttributes getAttributes() {
                return Presto.this.getAttributes();
            }
            @Override
            public PrestoContextRules getPrestoContextRules(PrestoContext context) {
                return Presto.this.getPrestoContextRules(context);
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

        ObjectNode extra = ExtraUtils.getViewExtraNode(view);
        Layout layout = getLayout(extra);
        if (layout != null) {
            result.setLayout(layout);
        }

        String topicId = context.getTopicId();
        PrestoContext parentContext = context.getParentContext();
        PrestoField parentField = context.getParentField();
        String href = lx.topicViewHref(parentContext, parentField, topicId, type, view, isReadOnlyMode());
        result.setHref(href);

        List<FieldData> fields = new ArrayList<FieldData>(); 
        boolean allFieldsReadOnly = true;
        for (PrestoField field : type.getFields(view)) {
            if (!rules.isHiddenField(field)) {
                fields.add(getFieldData(rules, field));

                if (!rules.isReadOnlyField(field)) {
                    allFieldsReadOnly = false;
                }
            }
        }
        result.setFields(fields);
        
        List<Link> links = new ArrayList<Link>();
        links.add(createLabel(type, view));

        if (!rules.isReadOnlyType()) {
            if (!allFieldsReadOnly && rules.isUpdatableType()) {
                links.add(lx.topicViewUpdateLink(parentContext, parentField, topicId, type, view));
            }
            if (rules.isRemovableType() && rules.isDeletableType()) {
                links.add(lx.topicViewDeleteLink(parentContext, parentField, topicId, type, view));
            }
            if (rules.isCreatableType() && !type.isInline()) {
                links.add(lx.topicTemplateLink(type));
            }
        }
        result.setLinks(links);

        return result;
    }

    public abstract URI getBaseUri();

    public TopicView getTopicViewTemplate(PrestoContext context) {

        PrestoContextRules rules = getPrestoContextRules(context);

        PrestoType type = context.getType();
        PrestoView view = context.getView();

        String typeId = type.getId();
        String viewId = view.getId();

        TopicView result = TopicView.view();
        result.setId(viewId);
        result.setName("*" + type.getName() + "*");

        result.setTopicTypeId(typeId);

        ObjectNode extra = ExtraUtils.getViewExtraNode(view);
        Layout layout = getLayout(extra);
        if (layout != null) {
            result.setLayout(layout);
        }

        List<FieldData> fields = new ArrayList<FieldData>(); 
        for (PrestoField field : type.getFields(view)) {
            if (!rules.isHiddenField(field)) {
                fields.add(getFieldData(rules, field));
            }
        }
        result.setFields(fields);

        List<Link> links = new ArrayList<Link>();
        links.add(createLabel(type, view));

        links.add(lx.topicViewCreateLink(type, view));

        result.setHref(lx.topicTemplateHref(type));
        result.setLinks(links);

        //        Status status = new Status();
        //        result = processor.preProcessTopicView(result, rules, status);
        result = processor.postProcessTopicView(result, rules, null);
        return result;
    }

    public TopicView getTopicViewTemplateField(PrestoContext parentContext, PrestoField parentField, PrestoType type, PrestoView view) {
        
        String typeId = type.getId();
        String viewId = view.getId();

        String topicId = "_" + type.getId();

        PrestoContext subcontext = PrestoContext.createSubContext(getDataProvider(), getSchemaProvider(), parentContext, parentField, topicId, viewId);
        PrestoContextRules subrules = getPrestoContextRules(subcontext);

        TopicView result = TopicView.view();
        result.setId(viewId);
        result.setName("*" + type.getName() + "*");

        result.setTopicTypeId(typeId);

        ObjectNode extra = ExtraUtils.getViewExtraNode(view);
        Layout layout = getLayout(extra);
        if (layout != null) {
            result.setLayout(layout);
        }

        List<FieldData> fields = new ArrayList<FieldData>(); 
        for (PrestoField field : type.getFields(view)) {
            if (!subrules.isHiddenField(field)) {
                fields.add(getFieldData(subrules, field));
            }
        }
        result.setFields(fields);

        List<Link> links = new ArrayList<Link>();
        links.add(createLabel(type, view));

        links.add(lx.topicViewCreateInlineLink(parentContext, parentField, type, view));

        result.setHref(lx.topicTemplateFieldHref(parentContext, parentField, type));
        result.setLinks(links);

        //        Status status = new Status();
        //        result = processor.preProcessTopicView(result, subrules, status);
        result = processor.postProcessTopicView(result, subrules, null);
        return result;
    }

    private void setParam(FieldData fieldData, String key, Object value) {
        Map<String, Object> params = fieldData.getParams();
        if (params == null) {
            params = new LinkedHashMap<String,Object>();
            fieldData.setParams(params);
        }
        params.put(key, value);
    }

    private Collection<? extends Link> getTopicTemplateFieldLinks(PrestoContext context, PrestoField field) {
        Collection<? extends PrestoType> availableFieldCreateTypes = getAvailableFieldCreateTypes(context, field);

        if (availableFieldCreateTypes.isEmpty()) {
            return Collections.emptyList();

        } else if (availableFieldCreateTypes.size() == 1) {
            PrestoType createType = availableFieldCreateTypes.iterator().next();
            Link link = getTopicTemplateFieldLink(context, field, createType);
            link.setName(getTopicTemplateLabel());
            return Collections.singleton(link);
        } else {
            Link link = new Link();
            link.setRel(Rel.REL_TOPIC_TEMPLATE_FIELD.getRel());
            link.setName(getTopicTemplateLabel());
            Collection<Link> links = new ArrayList<Link>(availableFieldCreateTypes.size());
            for (PrestoType createType : availableFieldCreateTypes) {
                links.add(getTopicTemplateFieldLink(context, field, createType));
            }
            link.setLinks(links);
            return Collections.singleton(link);
        }
    }
    
    protected String getTopicTemplateLabel() {
        return "Ny"; // FIXME: localize
    }

    protected Link getTopicTemplateFieldLink(PrestoContext context, PrestoField field, PrestoType createType) {
        Link result = lx.topicTemplateFieldLink(context, field, createType);
        result.setName(createType.getName());
        return result;
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

    public FieldDataValues setFieldDataValues(
            PrestoContextRules rules, final PrestoField field, Projection projection, FieldData fieldData) {

        FieldValues fieldValues = getFieldValues(rules, field, projection);
        return setFieldDataValues(rules, field, projection, fieldData, fieldValues);
    }

    public FieldValues getFieldValues(PrestoContextRules rules, PrestoField field) {
        return getFieldValues(rules, field, null);
    }
    
    private FieldValues getFieldValues(PrestoContextRules rules, PrestoField field, Projection projection) {
        FieldValues result = rules.getFieldValues(field, projection);
        PrestoContext context = rules.getContext();
        return filterByInlineReferenced(context, field, result);
    }

    private FieldValues filterByInlineReferenced(PrestoContext context, PrestoField field, FieldValues fieldValues) {
        // if inline reference field then find intersection with referenced field
        boolean inlineReference = field.getInlineReference() != null;
        if (inlineReference) {

            PrestoContextField valueContextField = getValueContextField(context, field);
            PrestoContext valueContext = valueContextField.getContext();
            PrestoField valueField = valueContextField.getField();
            
            if (!valueContext.isNewTopic()) {
                PrestoTopic referenceTopic = valueContext.getTopic();
                Set<String> referencedValueIds = toTopicIdSet(referenceTopic.getValues(valueField));
                List<? extends Object> values = fieldValues.getValues();
                
                List<Object> intersection = new ArrayList<Object>(values.size());
                
                // should only contain strings as they are references
                for (Object v : values) {
                    if (v instanceof String) {
                        if (referencedValueIds.contains(v)) {
                            intersection.add(v);
                        }
                    }
                }
                if (fieldValues.isPaging()) {
                    return FieldValues.create(intersection, fieldValues.getOffset(), fieldValues.getLimit(), fieldValues.getTotal());
                } else {
                    return FieldValues.create(intersection);
                }
            }
        }
        return fieldValues;
    }
    
    private Set<String> toTopicIdSet(List<? extends Object> values) {
        Set<String> result = new HashSet<String>(values.size());
        for (Object v : values) {
            if (v instanceof PrestoTopic) {
                PrestoTopic topic = (PrestoTopic)v;
                result.add(topic.getId());
            }
        }
        return result;
    }
    
    private PrestoContextField getValueContextField(PrestoContext context, PrestoField field) {
        String inlineReference = field.getInlineReference();
        if (inlineReference != null) {
            return ContextPathExpressions.getContextField(context, inlineReference);
        }
        return new PrestoContextField(context, field);
    }
    
    public FieldDataValues setFieldDataValues(PrestoContextRules rules, final PrestoField field, Projection projection,
            FieldData fieldData, FieldValues fieldValues) {

        ValueFactory valueFactory = createValueFactory(rules, field);

        List<? extends Object> values = fieldValues.getValues();

        // sort the result
        if (rules.isSortedField(field, projection)) {
            sortFieldValues(rules, field, projection, values);
        }

        int size = values.size();
        int start = 0;
        int end = size;

        List<Object> inputValues = new ArrayList<Object>(size);
        List<Value> outputValues = new ArrayList<Value>(size);
        for (int i=start; i < end; i++) {
            Object value = values.get(i);
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
            if (fieldValues.isPaging() && rules.isPageableField(field) && !rules.isSortedField(field, projection)) {
                fieldData.setValuesOffset(fieldValues.getOffset());
                fieldData.setValuesLimit(fieldValues.getLimit());
                fieldData.setValuesTotal(fieldValues.getTotal());
            }
        }
        return new FieldDataValues(inputValues, outputValues);
    }

    private void sortFieldValues(final PrestoContextRules rules, final PrestoField field, final Projection projection,
            List<? extends Object> fieldValues) {

        SortKeyGenerator sortKeyGenerator = createSortKeyGenerator(rules, field, projection);
        Comparator<Object> comp = sortKeyGenerator.getComparator(rules, field, projection);
        Collections.sort(fieldValues, comp);
    }

    private SortKeyGenerator createSortKeyGenerator(PrestoContextRules rules, PrestoField field, Projection projection) {
        ObjectNode extra = ExtraUtils.getFieldExtraNode(field);
        if (extra != null) {
            JsonNode handlerNode = extra.path("sortKeyGenerator");
            if (handlerNode.isObject()) {
                SortKeyGenerator handler = AbstractHandler.getHandler(dataProvider, schemaProvider, SortKeyGenerator.class, (ObjectNode)handlerNode);
                if (handler != null) {
                    handler.setPresto(this);
                    return handler;
                }
                log.warn("Not able to extract sortKeyGenerator instance from field " + field.getId() + ": " + extra);                    
            } else if (!handlerNode.isMissingNode()) {
                log.warn("Field " + field.getId() + " extra.sortKeyGenerator is not an object: " + extra);
            }
        }
        SortKeyGenerator result = new DefaultSortKeyGenerator();
        result.setPresto(this);
        result.setDataProvider(dataProvider);
        result.setSchemaProvider(schemaProvider);
        return result;
    }

    private ValueFactory createValueFactory(PrestoContextRules rules, PrestoField field) {
        ObjectNode extra = ExtraUtils.getFieldExtraNode(field);
        if (extra != null) {
            JsonNode handlerNode = extra.path("valueFactory");
            if (handlerNode.isObject()) {
                ValueFactory handler = AbstractHandler.getHandler(getDataProvider(), getSchemaProvider(), ValueFactory.class, handlerNode);
                if (handler != null) {
                    handler.setPresto(this);
                    return handler;
                }
                log.warn("Not able to extract valueFactory instance from field " + field.getId() + ": " + extra);                    
            } else if (!handlerNode.isMissingNode()) {
                log.warn("Field " + field.getId() + " extra.valueFactory is not an object: " + extra);
            }
        }
        return null;
    }

    protected Value getExistingFieldValue(ValueFactory valueFactory, PrestoContextRules rules, PrestoField field, Object fieldValue) {
        if (fieldValue instanceof PrestoTopic) {
            PrestoTopic topicValue = (PrestoTopic)fieldValue;
            return getExistingFieldValueTopic(valueFactory, rules, field, topicValue);
        } else {
            String stringValue = fieldValue.toString();
            return getExistingFieldValueString(valueFactory, rules, field, stringValue);
        }
    }

    protected Value getExistingFieldValueString(ValueFactory valueFactory, PrestoContextRules rules, PrestoField field, String value) {
        Value result;
        if (valueFactory != null) {
            result = valueFactory.createValue(rules, field, value);
        } else {
            result = new Value();
            result.setValue(value);
        }
        result.setRemovable(isRemovableFieldValue(rules, field, value));
        return result;
    }

    protected Value getExistingFieldValueTopic(ValueFactory valueFactory, PrestoContextRules rules, PrestoField field, PrestoTopic value) {
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

        result.setRemovable(isRemovableFieldValue(rules, field, value));

        List<Link> links = new ArrayList<Link>();
        if (rules.isTraversableField(field)) {
            PrestoView fieldsView = field.getEditView(valueType);
            if (valueType.isInline() || field.isParentRelation()) {
                PrestoContextField contextField = getValueContextField(context, field);
                links.add(lx.topicEditInlineLink(contextField.getContext(), contextField.getField(), value.getId(), valueType, fieldsView, isReadOnlyMode()));
            } else {
                links.add(lx.topicEditLink(value.getId(), valueType, fieldsView, isReadOnlyMode()));
            }
        }
        result.setLinks(links);

        return result;
    }

    private boolean isRemovableFieldValue(PrestoContextRules rules, PrestoField field, PrestoTopic value) {
        return !rules.isReadOnlyField(field) && rules.isRemovableFieldValue(field, value);
    }
    
    private boolean isRemovableFieldValue(PrestoContextRules rules, PrestoField field, String value) {
        return !rules.isReadOnlyField(field) && rules.isRemovableFieldValue(field, value);
    }
    
    public AvailableFieldValues getAvailableFieldValuesInfo(PrestoContextRules rules, PrestoField field, String query) {

        AvailableFieldValues result = new AvailableFieldValues();
        result.setId(field.getId());
        result.setName(field.getName());
        result.setValues(getAllowedFieldValues(rules, field, query));

        return result;
    }

    protected Collection<Value> getAllowedFieldValues(PrestoContextRules rules, PrestoField field, String query) {
        Collection<? extends Object> availableFieldValues = getAvailableFieldValues(rules, field, query);

        ValueFactory valueFactory = createValueFactory(rules, field);

        Collection<Value> result = new ArrayList<Value>(availableFieldValues.size());
        for (Object value : availableFieldValues) {
            if (value instanceof Value) {
                result.add((Value)value);
            } else {
                result.add(getAllowedFieldValue(valueFactory, rules, field, value));
            }
        }

        return result;
    }

    protected Value getAllowedFieldValue(ValueFactory valueFactory, PrestoContextRules rules, PrestoField field, Object fieldValue) {
        if (fieldValue instanceof PrestoTopic) {
            PrestoTopic topicValue = (PrestoTopic)fieldValue;
            return getAllowedFieldValueTopic(valueFactory, rules, field, topicValue);
        } else {
            String stringValue = fieldValue.toString();
            return getAllowedFieldValueString(valueFactory, rules, field, stringValue);
        }
    }

    protected Value getAllowedFieldValueString(ValueFactory valueFactory, PrestoContextRules rules, PrestoField field, String value) {
        Value result;
        if (valueFactory != null) {
            result = valueFactory.createValue(rules, field, value);
        } else {
            result = new Value();
            result.setValue(value);
        }
        return result;
    }

    protected Value getAllowedFieldValueTopic(ValueFactory valueFactory, PrestoContextRules rules, PrestoField field, PrestoTopic value) {
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
            links.add(lx.topicEditLink(value.getId(), valueType, fieldsView, false));
        }
        result.setLinks(links);

        return result;
    }

    protected Collection<? extends Object> getAvailableFieldValues(PrestoContextRules rules, PrestoField field, String query) {
        Collection<? extends Object> result = getCustomAvailableValues(rules, field, query);
        if (result != null) {
            return result;
        }
        if (!rules.isReadOnlyField(field) && rules.isAddableField(field)) {
            PrestoTopic topic = rules.getContext().getTopic();
            return dataProvider.getAvailableFieldValues(topic, field, query);
        } else {
            return Collections.emptyList();
        }
    }

    private boolean isCustomAvailableValuesQuery(PrestoContext context, PrestoField field) {
        ObjectNode extra = ExtraUtils.getFieldExtraNode(field);
        if (extra != null) {
            JsonNode availableValuesNode = extra.path("availableValues");
            if (availableValuesNode.isObject()) {
                return availableValuesNode.path("query").booleanValue();
            }
        }
        return false;
    }

    private Collection<? extends Object> getCustomAvailableValues(PrestoContextRules rules, PrestoField field, String query) {
        // TODO: shouldn't this be a PrestoFunction
        ObjectNode extra = ExtraUtils.getFieldExtraNode(field);
        if (extra != null) {
            JsonNode availableValuesNode = extra.path("availableValues");
            if (availableValuesNode.isArray()) {
                List<Object> result = new ArrayList<Object>();
                final boolean isReferenceField = field.isReferenceField();
                for (JsonNode availableValueNode : availableValuesNode) {
                    String availableValue = availableValueNode.textValue();
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
                AvailableFieldValuesResolver handler = AbstractHandler.getHandler(dataProvider, schemaProvider, AvailableFieldValuesResolver.class, (ObjectNode)availableValuesNode);
                if (handler != null) {
                    handler.setPresto(this);
                    return handler.getAvailableFieldValues(rules, field, query);
                }
                log.warn("Not able to extract availableValues instance from field " + field.getId() + ": " + extra);                    
            } else if (!availableValuesNode.isMissingNode()) {
                log.warn("Field " + field.getId() + " extra.availableValues is not an array: " + extra);
            }
        }
        return null;
    }

    public FieldData getFieldData(PrestoContextRules rules, PrestoField field) {
        Projection projection = null;
        return getFieldData(rules, field, projection, true);
    }

    public FieldData getFieldDataNoValues(PrestoContextRules rules, PrestoField field) {
        Projection projection = null;
        return getFieldData(rules, field, projection, false);
    }

    private FieldData getFieldData(PrestoContextRules rules, PrestoField field, 
            Projection projection, boolean includeValues) {

        PrestoContext context = rules.getContext();

        PrestoTopic topic = context.getTopic();
        PrestoType type = context.getType();
        PrestoView view = context.getView();

        boolean isNewTopic = context.isNewTopic();

        String topicId;
        if (isNewTopic) {
            topicId = "_" + type.getId();
        } else {
            topicId = topic.getId();
        }

        FieldData fieldData = new FieldData();
        fieldData.setId(field.getId());
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
        PrestoField parentField = context.getParentField();

        boolean allowCreate = rules.isCreatableField(field);
        boolean allowAdd = rules.isAddableField(field);

        Collection<Link> fieldLinks = new LinkedHashSet<Link>();      
        if (field.isReferenceField()) {
            fieldData.setDatatype("reference");
        } else {
            String dataType = field.getDataType();
            if (dataType != null) {
                fieldData.setDatatype(dataType);
            }
        }
        if (!isReadOnly) {
            boolean isSorted = rules.isSortedField(field, projection);

            boolean allowRemove = rules.isRemovableField(field);
            boolean allowMove = !isSorted && rules.isMovableField(field);

            if (allowAdd || allowCreate) {
                if (!isNewTopic) {
                    fieldLinks.add(lx.fieldAddValuesLink(parentContext, parentField, topicId, type, view, field));
                    if (!isSorted) {
                        fieldLinks.add(lx.fieldAddValuesAtIndexLink(parentContext, parentField, topicId, type, view, field));
                    }
                }
            }
            if (allowRemove && !isNewTopic) {
                fieldLinks.add(lx.fieldRemoveValuesLink(parentContext, parentField, topicId, type, view, field));
            }      

            if (allowMove && !isNewTopic) {
                fieldLinks.add(lx.fieldMoveValuesToIndexLink(parentContext, parentField, topicId, type, view, field));
            }
        }
        if (!isReadOnly && allowAdd) {
            // ISSUE: should add-values and remove-values be links on list result instead?
            if (!field.isReferenceField() || !getAvailableFieldValueTypes(context, field).isEmpty()) {
                boolean query = isCustomAvailableValuesQuery(context, field);
                fieldLinks.add(lx.fieldAvailableValuesLink(parentContext, parentField, topicId, type, view, field, query));
            }
        }

        if (!isReadOnly && allowCreate) {
            fieldLinks.addAll(getTopicTemplateFieldLinks(context, field));
        }

        if (rules.isPageableField(field)) {
            fieldLinks.add(lx.fieldPagingLink(parentContext, parentField, topicId, type, view, field));    
        }

        if (!fieldLinks.isEmpty()) {
            fieldData.setLinks(fieldLinks);
        }

        // get values (truncated if neccessary)
        if (includeValues) {
            setFieldDataValues(rules, field, projection, fieldData);
        }

        //        fieldData = processor.postProcessFieldData(fieldData, topic, field, null);

        return fieldData;
    }

    public FieldData getFieldDataAndProcess(PrestoContext context, PrestoField field) {
        PrestoContextRules rules = getPrestoContextRules(context);

        FieldData result = getFieldData(rules, field);

        return processor.postProcessFieldData(result, rules, field, null, null);
    }

    public FieldData getFieldDataAndProcess(PrestoContext context, PrestoField field, Projection projection) {
        PrestoContextRules rules = getPrestoContextRules(context);

        FieldData result = getFieldData(rules, field, projection, true);

        return processor.postProcessFieldData(result, rules, field, projection, null);
    }
    
    public FieldData addFieldValues(PrestoContextRules rules, PrestoField field, Integer index, FieldData fieldData, boolean isMove) {
        boolean resolveEmbedded = true;
        boolean includeExisting = false;
        boolean filterNonStorable = true;
        boolean validateValueTypes = true;
        List<? extends Object> addableValues = updateAndExtractValuesFromFieldData(rules, field, fieldData, resolveEmbedded, includeExisting, filterNonStorable, validateValueTypes);
        
        PrestoContext updatedContext = addFieldValues(rules, field, addableValues, index, isMove);

        return getFieldDataAndProcess(updatedContext, field);
    }
    
    public PrestoContext addFieldValues(PrestoContextRules rules, PrestoField field, List<? extends Object> addableValues, Integer index, boolean isMove) {
        validateAddableFieldValues(rules, field, addableValues, isMove);

        PrestoContext context = rules.getContext();
        PrestoTopic topic = context.getTopic();
        PrestoType type = context.getType();

        PrestoTopic updatedTopic;
        if (topic.isInline()) {
            List<Object> values = new ArrayList<Object>(topic.getStoredValues(field));
            if (index != null) {
                boolean allowAdd = !isMove;
                values = Utils.moveValuesToIndex(values, addableValues, index, allowAdd);
            } else {
                values.add(addableValues);
            }
            PrestoInlineTopicBuilder builder = dataProvider.createInlineTopic(type, topic.getId());
            builder.setValues(field, values);
            PrestoTopic newTopic = builder.build();
            updatedTopic = mergeInlineTopic(newTopic, topic);
        } else {

            PrestoDataProvider dataProvider = getDataProvider();
            PrestoChangeSet changeSet = dataProvider.newChangeSet(getChangeSetHandler());
            PrestoUpdate update = changeSet.updateTopic(topic, type);        

            if (index == null) {
                update.addValues(field, addableValues);
            } else {
                update.addValues(field, addableValues, index);        
            }
            changeSet.save();

            updatedTopic = update.getTopicAfterSave();
        }
        return updateParentContext(rules.getContext(), updatedTopic);
    }

    public FieldData removeFieldValues(PrestoContextRules rules, PrestoField field, FieldData fieldData) {
        boolean resolveEmbedded = false;
        boolean includeExisting = false;
        boolean filterNonStorable = false; // NOTE: instead of filtering we complain in removeFieldValues
        boolean validateValueTypes = false;
        List<? extends Object> removeableValues = updateAndExtractValuesFromFieldData(rules, field, fieldData, resolveEmbedded, includeExisting, filterNonStorable, validateValueTypes);

        PrestoContext updatedContext = removeFieldValues(rules, field, removeableValues);

        return getFieldDataAndProcess(updatedContext, field);
    }

    public PrestoContext removeFieldValues(PrestoContextRules rules, PrestoField field, List<? extends Object> removeableValues) {
        validateRemovableFieldValues(rules, field, removeableValues);
        
        PrestoContext context = rules.getContext();
        PrestoTopic topic = context.getTopic();
        PrestoType type = context.getType();
        
        PrestoTopic updatedTopic;
        if (topic.isInline()) {
            List<? extends Object> values = topic.getStoredValues(field);
            values.removeAll(removeableValues);
            PrestoInlineTopicBuilder builder = dataProvider.createInlineTopic(type, topic.getId());
            builder.setValues(field, values);
            PrestoTopic newTopic = builder.build();
            updatedTopic = mergeInlineTopic(newTopic, topic);
        } else {
            PrestoDataProvider dataProvider = getDataProvider();
            PrestoChangeSet changeSet = dataProvider.newChangeSet(getChangeSetHandler());
            PrestoUpdate update = changeSet.updateTopic(topic, type);        
    
            update.removeValues(field, removeableValues);
            changeSet.save();
    
            updatedTopic = update.getTopicAfterSave();
        }
        return updateParentContext(rules.getContext(), updatedTopic);
    }

    private void validateAddableFieldValues(PrestoContextRules rules, PrestoField field, List<? extends Object> addableValues, boolean isMove) {
        if (isMove) {
            for (Object addableValue : addableValues) {
                // make sure that non-movable values are not added
                if (!rules.isMovableFieldValue(field, addableValue)) {
                    throw new NotMovableValueConstraintException(rules.getContext(), field, addableValue);
                }
            }
            
        } else {
            for (Object addableValue : addableValues) {
                // make sure that non-addable values are not added
                if (!rules.isAddableFieldValue(field, addableValue)) {
                    throw new NotAddableValueConstraintException(rules.getContext(), field, addableValue);
                }
            }
        }
    }

    private void validateRemovableFieldValues(PrestoContextRules rules, PrestoField field, List<? extends Object> removableValues) {
        for (Object removableValue : removableValues) {
            // make sure that non-removable values are not removed
            if (removableValue instanceof PrestoTopic) {
                if (!isRemovableFieldValue(rules, field, (PrestoTopic)removableValue)) {
                    throw new NotRemovableValueConstraintException(rules.getContext(), field, removableValue);
                }
            } else {
                if (!isRemovableFieldValue(rules, field, (String)removableValue)) {
                    throw new NotRemovableValueConstraintException(rules.getContext(), field, removableValue);
                }
            }
        }
    }
    
    public TopicView validateTopic(PrestoContext context, TopicView topicView) {
        PrestoContextRules rules = getPrestoContextRules(context);
        Status status = new Status();

        topicView = processor.preProcessTopicView(topicView, rules, status);

        return processor.postProcessTopicView(topicView, rules, null);
    }
    
    public TopicView executeFieldAction(PrestoContext context, TopicView topicView, PrestoField field, String actionId) {
        PrestoContextRules rules = getPrestoContextRules(context);
        Status status = new Status();

        topicView = processor.preProcessTopicView(topicView, rules, status);
        
        FieldAction fieldAction = getFieldAction(field, actionId);
        
        if (fieldAction != null && fieldAction.isActive(rules, field, null, actionId)) {
            log.debug("Executing action: "+ actionId);
            topicView = fieldAction.executeAction(context, topicView, field, actionId);
        }
        
        return processor.postProcessTopicView(topicView, rules, null);
    }

    public FieldAction getFieldAction(PrestoField field, String actionId) {
        ObjectNode extra = ExtraUtils.getFieldExtraNode(field);
        if (extra != null) {
            JsonNode actionNode = extra.path("actions").path(actionId);
            if (actionNode.isObject()) {
                String className = actionNode.path("class").textValue();
                FieldAction fieldAction = Utils.newInstanceOf(className, FieldAction.class);
                fieldAction.setConfig((ObjectNode)actionNode);
                fieldAction.setPresto(this);
                return fieldAction;
            }
        }        
        return null;
    }
    
    public Object updateTopicView(PrestoContext context, TopicView topicView) {
        PrestoContextRules rules = getPrestoContextRules(context);
        Status status = new Status();

        topicView = processor.preProcessTopicView(topicView, rules, status);

        if (status.isValid()) {

            PrestoTopic updatedTopic = updatePrestoTopic(rules, topicView);
            PrestoContext newContext = updateParentContext(context, updatedTopic);

            if (isReturnParentOnUpdate(context, newContext)) {
                PrestoContext parentContext = newContext.getParentContext();
                if (parentContext != null) {
                    newContext = parentContext;
                }
            }

            PrestoContext parentContext = context.getParentContext();
            if (context.isNewTopic() && parentContext == null) {
                return getTopicAndProcess(newContext);
            } else {
                return getTopicViewAndProcess(newContext);
            }
        } else {
            return processor.postProcessTopicView(topicView, rules, null);
        }
    }

    private boolean isReturnParentOnUpdate(PrestoContext beforeContext, PrestoContext updatedContext) {

        PrestoField parentField = updatedContext.getParentField();
        if (parentField != null) {
            ObjectNode extra = ExtraUtils.getFieldExtraNode(parentField);
            if (extra != null) {
                boolean created = beforeContext.isNewTopic() && !updatedContext.isNewTopic();
                if (created) {
                    JsonNode onUpdateNode = extra.path("returnParentOnCreate");
                    if (onUpdateNode.isBoolean()) {
                        return onUpdateNode.booleanValue();
                    }
                } else {
                    JsonNode onUpdateNode = extra.path("returnParentOnUpdate");
                    if (onUpdateNode.isBoolean()) {
                        return onUpdateNode.booleanValue();
                    }
                }
            }
        }
        return true;
    }
    
    protected PrestoContext updateParentContext(PrestoContext oldContext, PrestoTopic updatedTopic) {
        PrestoContext oldParentContext = oldContext.getParentContext();
        PrestoField parentField = oldContext.getParentField();
        if (oldParentContext != null) {
            PrestoTopic updatedParent = updateContextFieldValues(oldParentContext, parentField, Collections.singletonList(updatedTopic));
            PrestoContext newParentContext = updateParentContext(oldParentContext, updatedParent);
            return PrestoContext.createSubContext(newParentContext, parentField, updatedTopic, oldContext.getType(), oldContext.getView());
        } else {            
            return PrestoContext.newContext(oldContext, updatedTopic);
        }
    }
    
    private PrestoTopic updateContextFieldValues(PrestoContext context, PrestoField field, List<? extends Object> updateableValues) {
        PrestoTopic topic = context.getTopic();
        PrestoType type = context.getType();
        
        PrestoContextRules rules = getPrestoContextRules(context);

        List<? extends Object> existingValues = topic.getStoredValues(field);
        List<? extends Object> newValues = mergeFieldValues(field, updateableValues, existingValues);

        filterNonStorableFieldValues(rules, field, newValues);

        if (topic.isInline()) {
            PrestoInlineTopicBuilder builder = dataProvider.createInlineTopic(type, topic.getId());
            builder.setValues(field, newValues);
            PrestoTopic newTopic = builder.build();
            return mergeInlineTopic(newTopic, topic);
        } else {

            PrestoDataProvider dataProvider = getDataProvider();
            PrestoChangeSet changeSet = dataProvider.newChangeSet(getChangeSetHandler());
            PrestoUpdate update = changeSet.updateTopic(topic, type);        

            update.setValues(field, newValues);
            changeSet.save();

            return update.getTopicAfterSave();
        }
    }

    private List<? extends Object> mergeFieldValues(PrestoField field, List<? extends Object> updateableValues, List<? extends Object> existingValues) {
        List<Object> result;
        if (field.isInline()) {
            boolean includeExisting = true;
            Collection<? extends Object> merged = mergeInlineTopics(updateableValues, existingValues, includeExisting);
            result = new ArrayList<Object>(merged);
        } else {
            result = new ArrayList<Object>(existingValues.size()+updateableValues.size());
            for (Object ev : existingValues) {
                result.add(ev);
            }
            for (Object uv : updateableValues) {
                result.add(uv);
            }
        }
        return result;
    }
    
    protected PrestoTopic updatePrestoTopic(PrestoContextRules rules, TopicView topicView) {

        PrestoDataProvider dataProvider = getDataProvider();
        PrestoChangeSet changeSet = dataProvider.newChangeSet(getChangeSetHandler());

        PrestoContext context = rules.getContext();
        PrestoType type = context.getType();
        PrestoView view = context.getView();

        boolean filterNonStorable = true;
        boolean validateValueTypes = true;

        if (type.isInline()) {
            PrestoContext parentContext = context.getParentContext();
            PrestoField parentField = context.getParentField();
            PrestoTopic inlineTopic = buildInlineTopic(parentContext, parentField, topicView, filterNonStorable, validateValueTypes);

            // merge inlineTopic with existing topic to avoid anemic topic
            return rehydrateInlineTopic(parentContext, parentField, inlineTopic);

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
                PrestoField field = type.getFieldById(fieldId, view);

                // ignore read-only or pageable fields 
                if (!rules.isReadOnlyField(field) && !rules.isPageableField(field)) {

                    boolean resolveEmbedded = true;
                    boolean includeExisting = false; 
                    List<? extends Object> values = updateAndExtractValuesFromFieldData(rules, field, fieldData, resolveEmbedded, includeExisting, filterNonStorable, validateValueTypes);
                    
                    update.setValues(field, values);
                }
            }

            changeSet.save();

            return update.getTopicAfterSave();
        }
    }

    public FieldData updateFieldValues(PrestoContextRules rules, PrestoField field, FieldData fieldData) {
        PrestoContext updatedContext = updatePrestoTopic(rules, fieldData);

        return getFieldDataAndProcess(updatedContext, field);
    }

    protected PrestoContext updatePrestoTopic(PrestoContextRules rules, FieldData fieldData) {

        PrestoDataProvider dataProvider = getDataProvider();
        PrestoChangeSet changeSet = dataProvider.newChangeSet(getChangeSetHandler());

        PrestoContext context = rules.getContext();
        PrestoType type = context.getType();
        PrestoView view = context.getView();

        boolean filterNonStorable = true;
        boolean validateValueTypes = true;

        if (type.isInline()) {
            throw new RuntimeException("Cannot update inline objects this way."); // or can we?
        } else {
            PrestoUpdate update;
            if (context.isNewTopic()) {
                // TODO: add support for assigning topic ids?
                update = changeSet.createTopic(type);
            } else {
                update = changeSet.updateTopic(context.getTopic(), type);
            }

            String fieldId = fieldData.getId();
            PrestoField field = type.getFieldById(fieldId, view);

            // ignore read-only or pageable fields 
            if (!rules.isReadOnlyField(field) && !rules.isPageableField(field)) {

                boolean resolveEmbedded = true;
                boolean includeExisting = false;
                List<? extends Object> values = updateAndExtractValuesFromFieldData(rules, field, fieldData, resolveEmbedded, includeExisting, filterNonStorable, validateValueTypes);

                update.setValues(field, values);
            }

            changeSet.save();

            PrestoTopic updatedTopic = update.getTopicAfterSave();
            return updateParentContext(context, updatedTopic);
        }
    }

    private List<? extends Object> updateAndExtractValuesFromFieldData(PrestoContextRules rules, PrestoField field, FieldData fieldData, 
            boolean resolveEmbedded, boolean includeExisting, boolean filterNonStorable, boolean validateValueTypes) {
        Collection<Value> values = fieldData.getValues();
        List<Object> result = new ArrayList<Object>(values.size());

        if (!values.isEmpty()) {
            PrestoContext context = rules.getContext();

            if (field.isReferenceField()) {
                boolean inlineReference = field.getInlineReference() != null;
                if (field.isInline()) {
                    // build inline topics from field data
                    List<Object> newValues = new ArrayList<Object>();
                    for (Value value : values) {
                        if (inlineReference) {
                            newValues.add(value.getValue());
                        } else {
                            TopicView embeddedTopic = getEmbeddedTopic(value);
                            if (embeddedTopic != null) {
                                boolean filterNonStorableNested = true;
                                newValues.add(buildInlineTopic(context, field, embeddedTopic, filterNonStorableNested, validateValueTypes));
                            } else {
                                String typeId = value.getType();
                                PrestoType type = schemaProvider.getTypeById(typeId, null);
                                if (type != null) {
                                    newValues.add(buildInlineTopic(context, type, value.getValue()));
                                } else {
                                    throw new InvalidValueTypeConstraintException(getSchemaProvider());
                                }
                            }
                        }
                    }
                    // merge new inline topics with existing ones
                    if (context.isNewTopic()) {
                        result.addAll(newValues);
                    } else {
                        PrestoTopic topic = context.getTopic();
                        List<? extends Object> existingValues = topic.getValues(field);
                        if (inlineReference) {
                            result.addAll(mergeInlineStrings(newValues, existingValues, includeExisting));
                        } else {
                            result.addAll(mergeInlineTopics(newValues, existingValues, includeExisting));
                        }
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
                // validate valueTypes
                if (validateValueTypes && !inlineReference) {
                    validateValueTypes(context, field, result);
                }
                
            } else {
                for (Value value : values) {
                    result.add(getPrimitiveValue(value));
                }
            }

            // filter out non-storable values
            if (filterNonStorable) {
                filterNonStorableFieldValues(rules, field, result);
            }
        }
        return result;
    }

    private void validateValueTypes(PrestoContext context, PrestoField field, List<Object> values) {
        Collection<String> valueTypeIds = getAvailableFieldValueTypesId(context, field);
        for (Object value : values) {
            PrestoTopic v = (PrestoTopic)value;
            if (!valueTypeIds.contains(v.getTypeId())) {
                throw new InvalidValueTypeConstraintException(getSchemaProvider());
            }
        }
    }

    private Collection<String> getAvailableFieldValueTypesId(PrestoContext context, PrestoField field) {
        Collection<PrestoType> valueTypes = getAvailableFieldValueTypes(context, field);
        if (valueTypes.isEmpty()) {
            return Collections.emptySet();
        }
        Collection<String> result = new HashSet<String>(valueTypes.size());
        for (PrestoType valueType : valueTypes) {
            result.add(valueType.getId());
        }
        return result;
    }

    private void filterNonStorableFieldValues(PrestoContextRules rules, PrestoField field, Collection<? extends Object> values) {
        // remove ignorable field values
        Iterator<?> iter = values.iterator();
        while (iter.hasNext()) {
            if (!rules.isStorableFieldValue(field, iter.next())) {
                iter.remove();
            }
        }
    }
    
    protected PrestoTopic buildInlineTopic(PrestoContext context, PrestoType type, String topicId) {
        PrestoInlineTopicBuilder builder = dataProvider.createInlineTopic(type, topicId);
        return builder.build();
    }

    protected PrestoTopic buildInlineTopic(PrestoContext _parentContext, PrestoField _parentField, TopicView embeddedTopic, 
            boolean filterNonStorable, boolean validateValueTypes) {

        PrestoSchemaProvider schemaProvider = getSchemaProvider();

        String topicId = embeddedTopic.getTopicId();
        String topicTypeId = embeddedTopic.getTopicTypeId();
        String viewId = embeddedTopic.getId();

        PrestoType type = schemaProvider.getTypeById(topicTypeId);

        if (!type.isInline()) {
            throw new NotInlineTypeConstraintException(getSchemaProvider());
        }

        PrestoContextField contextField = getValueContextField(_parentContext, _parentField);
        PrestoContext parentContext = contextField.getContext();
        PrestoField parentField = contextField.getField();
        
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
            PrestoField field = type.getFieldById(fieldId, view);

            boolean resolveEmbedded = true;
            boolean includeExisting = false;
            List<? extends Object> values = updateAndExtractValuesFromFieldData(subrules, field, fieldData, resolveEmbedded, includeExisting, filterNonStorable, validateValueTypes);
            builder.setValues(field, values);
        }

        return builder.build();
    }

    private PrestoTopic mergeInlineTopic(PrestoTopic t1, PrestoTopic t2) {
        String topicId = t1.getId();
        if (Utils.different(topicId, t2.getId())) {
            throw new IllegalArgumentException("Cannot merge topics with different ids: '" + topicId + "' and '" + t2.getId() + "'");
        }
        PrestoSchemaProvider schemaProvider = getSchemaProvider();
        PrestoType type = schemaProvider.getTypeById(t1.getTypeId());

        if (!type.isInline()) {
            throw new RuntimeException("Attempted to merge non-inline topics: " + t1.getId() + " and " + t2.getId());
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
                    boolean inlineReference = field.getInlineReference() != null;
                    Collection<? extends Object> merged;
                    if (inlineReference) {
                        merged = mergeInlineStrings(t1.getStoredValues(field), t2.getStoredValues(field), includeExisting);
                    } else {
                        merged = mergeInlineTopics(t1.getStoredValues(field), t2.getStoredValues(field), includeExisting);
                    }
                    builder.setValues(field, merged);
                } else {
                    builder.setValues(field, t1.getStoredValues(field));
                }
            } else if (hasValue1) {
                builder.setValues(field, t1.getStoredValues(field));
            } else if (hasValue2) {
                builder.setValues(field, t2.getStoredValues(field));
            }
        }

        return builder.build();
    }

    private Collection<? extends Object> mergeInlineStrings(List<? extends Object> valuesNew, List<? extends Object> valuesExisting, 
            boolean includeExisting) {
        if (includeExisting) {
            List<Object> result = new ArrayList<Object>(valuesNew.size() + valuesExisting.size());
            result.addAll(valuesExisting);
            result.addAll(valuesNew);
            return result;
        } else {
            return valuesNew;
        }
    }
    
    private Collection<? extends Object> mergeInlineTopics(List<? extends Object> valuesNew, List<? extends Object> valuesExisting, 
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
    
    protected PrestoTopic rehydrateInlineTopic(PrestoContext parentContext, PrestoField parentField, PrestoTopic inlineTopic) {
        if (!parentContext.isNewTopic()) {
            PrestoTopic parentTopic = parentContext.getTopic();
            for (Object value : parentTopic.getValues(parentField)) {
                if (value instanceof PrestoTopic) {
                    PrestoTopic valueTopic = (PrestoTopic)value;
                    String inlineTopicId = inlineTopic.getId();
                    if (inlineTopicId != null && inlineTopicId.equals(valueTopic.getId())) {
                        return mergeInlineTopic(inlineTopic, valueTopic);
                    }
                }
            }
        }
        return inlineTopic;
    }

    private Map<String,Object> toMapTopics(List<? extends Object> values) {
        Map<String,Object> result = new LinkedHashMap<String,Object>(values.size());
        for (Object value : values) {
            PrestoTopic topic = (PrestoTopic)value;
            result.put(topic.getId(), topic);
        }
        return result;
    }

    private PrestoTopic updateEmbeddedTopic(PrestoContextRules rules, PrestoField field, TopicView embeddedTopic) {

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
        links.add(lx.topicEditByIdLink());
        result.setLinks(links);      

        return result;
    }

    protected Collection<PrestoType> getAvailableFieldValueTypes(PrestoContext context, PrestoField field) {
        return field.getAvailableFieldValueTypes();
    }

    protected Collection<? extends PrestoType> getAvailableFieldCreateTypes(PrestoContext context, PrestoField field) {
        Collection<? extends PrestoType> result = getCustomAvailableFieldCreateTypes(context, field);
        if (result != null) {
            return result;
        }
        return field.getAvailableFieldCreateTypes();
    }

    private Collection<? extends PrestoType> getCustomAvailableFieldCreateTypes(PrestoContext context, PrestoField field) {
        ObjectNode extra = ExtraUtils.getFieldExtraNode(field);
        if (extra != null) {
            JsonNode handlerNode = extra.path("createTypes");
            if (handlerNode.isObject()) {
                AvailableFieldCreateTypesResolver handler = AbstractHandler.getHandler(dataProvider, schemaProvider, AvailableFieldCreateTypesResolver.class, (ObjectNode)handlerNode);
                if (handler != null) {
                    handler.setPresto(this);
                    return handler.getAvailableFieldCreateTypes(context, field);
                }
                log.warn("Not able to extract createTypes instance from field " + field.getId() + ": " + extra);                    
            } else if (!handlerNode.isMissingNode()) {
                log.warn("Field " + field.getId() + " extra.createTypes is not an object: " + extra);
            }
        }
        return null;
    }

    protected PrestoTopic findInlineTopicById(PrestoTopic parentTopic, PrestoField field, String topicId) {
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
    
    private Link createLabel(PrestoType type, PrestoView view) {
        String name = null;
        ObjectNode extra = ExtraUtils.getViewExtraNode(view);
        if (extra != null) {
            JsonNode labelNode = extra.path("label");
            if (labelNode.isTextual()) {
                name = labelNode.textValue();
            }
        }
        if (name == null) {
            name = type.getName();
        }
        Link link = new Link();
        link.setName(name);
        link.setRel("label");
        return link;
    }

}
