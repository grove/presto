package net.ontopia.presto.jaxrs;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import net.ontopia.presto.jaxb.AvailableDatabases;
import net.ontopia.presto.jaxb.AvailableFieldValues;
import net.ontopia.presto.jaxb.Database;
import net.ontopia.presto.jaxb.FieldData;
import net.ontopia.presto.jaxb.Link;
import net.ontopia.presto.jaxb.RootInfo;
import net.ontopia.presto.jaxb.Topic;
import net.ontopia.presto.jaxb.TopicMessage;
import net.ontopia.presto.jaxb.TopicView;
import net.ontopia.presto.jaxrs.constraints.ConstraintException;
import net.ontopia.presto.spi.PrestoChangeSet;
import net.ontopia.presto.spi.PrestoChanges;
import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoDataProvider.ChangeSetHandler;
import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoSchemaProvider;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.PrestoTopic.Projection;
import net.ontopia.presto.spi.PrestoType;
import net.ontopia.presto.spi.PrestoUpdate;
import net.ontopia.presto.spi.PrestoView;
import net.ontopia.presto.spi.utils.PrestoAttributes;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextField;
import net.ontopia.presto.spi.utils.PrestoContextRules;
import net.ontopia.presto.spi.utils.PrestoProjection;

@Path("/editor")
public abstract class EditorResource implements PrestoAttributes {

    private static final String REL_AVAILABLE_DATABASES = "available-databases";
    private static final String REL_DATABASE_EDIT = "edit";

    public final static String APPLICATION_JSON_UTF8 = "application/json;charset=UTF-8";

    protected @Context HttpServletRequest request;
    protected @Context UriInfo uriInfo;

    @GET
    @Produces(APPLICATION_JSON_UTF8)
    public Response getRootInfo() throws Exception {

        RootInfo result = new RootInfo();

        result.setVersion(0);
        result.setName("Presto - Editor REST API");

        List<Link> links = new ArrayList<Link>();
        links.add(new Link(REL_AVAILABLE_DATABASES, uriInfo.getBaseUri() + "editor/available-databases"));
        result.setLinks(links);      

        return Response.ok(result).build();
    }

    @GET
    @Produces(APPLICATION_JSON_UTF8)
    @Path("available-databases")
    public Response getDatabases() throws Exception {

        AvailableDatabases result = getAvailableDatabasesInfo();
        return Response.ok(result).build();
    }

    protected AvailableDatabases getAvailableDatabasesInfo() {
        AvailableDatabases result = new AvailableDatabases();

        result.setName("Presto - Editor REST API");

        Collection<Database> databases = new ArrayList<Database>();
        for (String databaseId : getDatabaseIds()) {
            Database database = new Database();
            database.setId(databaseId);
            database.setName(getDatabaseName(databaseId));

            List<Link> links = new ArrayList<Link>();
            links.add(new Link(REL_DATABASE_EDIT, uriInfo.getBaseUri() + "editor/database-info/" + database.getId()));
            database.setLinks(links);    

            databases.add(database);
        }
        result.setDatabases(databases);      
        return result;
    }

    @GET
    @Produces(APPLICATION_JSON_UTF8)
    @Path("database-info/{databaseId}")
    public Response getDatabaseInfo(
            @PathParam("databaseId") final String databaseId) throws Exception {

        boolean readOnly = false;
        Presto session = createPresto(databaseId, readOnly);

        try {
            Database result = session.getDatabaseInfo();
            return Response.ok(result).build();

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        }
    }

    @GET
    @Produces(APPLICATION_JSON_UTF8)
    @Path("topic-template/{databaseId}/{typeId}")
    public Response getTopicTemplate(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("typeId") final String typeId) throws Exception {

        boolean readOnly = false;
        Presto session = createPresto(databaseId, readOnly);

        try {
            PrestoSchemaProvider schemaProvider = session.getSchemaProvider();

            PrestoType type = schemaProvider.getTypeById(typeId);
            if (type == null) {
                return Response.status(Status.NOT_FOUND).build();
            }

            PrestoContext context = PrestoContext.create(session.getResolver(), type, type.getCreateView());

            TopicView result = session.getTopicViewTemplate(context);
            return Response.ok(result).build();

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        }
    }

    @GET
    @Produces(APPLICATION_JSON_UTF8)
    @Path("topic-template-field/{databaseId}/{path}/{typeId}")
    public Response getTopicTemplateField(
            @PathParam("databaseId") final String databaseId,
            @PathParam("path") final String path,
            @PathParam("typeId") final String typeId) throws Exception {

        boolean readOnly = false;
        Presto session = createPresto(databaseId, readOnly);

        try {
            PrestoSchemaProvider schemaProvider = session.getSchemaProvider();

            PrestoType type = schemaProvider.getTypeById(typeId);
            if (type == null) {
                return Response.status(Status.NOT_FOUND).build();
            }

            PrestoContextField contextField = PathParser.getContextField(session, path);

            PrestoField parentField = contextField.getField();
            PrestoView view = parentField.getCreateView(type);
            TopicView result = session.getTopicViewTemplateField(contextField.getContext(), parentField, type, view);

            return Response.ok(result).build();

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        }
    }
    
    @GET
    @Produces(APPLICATION_JSON_UTF8)
    @Path("topic-template-field/{databaseId}/{path}/{typeId}/{viewId}")
    public Response getTopicTemplateField(
            @PathParam("databaseId") final String databaseId,
            @PathParam("path") final String path,
            @PathParam("typeId") final String typeId,
            @PathParam("viewId") final String viewId) throws Exception {

        boolean readOnly = false;
        Presto session = createPresto(databaseId, readOnly);

        try {
            PrestoSchemaProvider schemaProvider = session.getSchemaProvider();

            PrestoType type = schemaProvider.getTypeById(typeId);
            if (type == null) {
                return Response.status(Status.NOT_FOUND).build();
            }

            PrestoContextField contextField = PathParser.getContextField(session, path);
            PrestoField parentField = contextField.getField();
            PrestoView view = type.getViewById(viewId);
            TopicView result = session.getTopicViewTemplateField(contextField.getContext(), parentField, type, view);

            return Response.ok(result).build();

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        }
    }

    @GET
    @Produces(APPLICATION_JSON_UTF8)
    @Path("paging-field/{databaseId}/{topicId}/{viewId}/{fieldId}/{start}/{limit}")
    public Response getFieldPaging(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId,
            @PathParam("fieldId") final String fieldId, 
            @PathParam("start") final int start, 
            @PathParam("limit") final int limit,
            @QueryParam("orderBy") final String orderBy) throws Exception {
        String path = null;
        return getFieldPagingPath(databaseId, path, topicId, viewId, fieldId, start, limit, orderBy);
    }

    @GET
    @Produces(APPLICATION_JSON_UTF8)
    @Path("paging-field/{databaseId}/{path}/{topicId}/{viewId}/{fieldId}/{start}/{limit}")
    public Response getFieldPagingPath(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("path") final String path, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId,
            @PathParam("fieldId") final String fieldId, 
            @PathParam("start") final int start, 
            @PathParam("limit") final int limit,
            @QueryParam("orderBy") final String orderBy) throws Exception {

        boolean readOnly = false;
        Presto session = createPresto(databaseId, readOnly);

        try {
            PrestoContext context = PathParser.getTopicByPath(session, path, topicId, viewId);

            if (context == null || (context.isMissingTopic() && !context.isLazyTopic())) {
                return Response.status(Status.NOT_FOUND).build();
            }

            PrestoField field = context.getFieldById(fieldId);
            if (field == null) {
                return Response.status(Status.NOT_FOUND).build();
            }

            Projection projection = new PrestoProjection(start, limit, orderBy);
            FieldData result = session.getFieldDataAndProcess(context, field, projection);

            return Response.ok(result).build();

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        } 
    }

    @DELETE
    @Produces(APPLICATION_JSON_UTF8)
    @Path("topic-view/{databaseId}/{topicId}/{viewId}")
    public Response deleteTopicView(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId,
            @PathParam("viewId") final String viewId) throws Exception {
        String path = null;
        return deleteTopicViewPath(databaseId, path, topicId, viewId);
    }

    @DELETE
    @Produces(APPLICATION_JSON_UTF8)
    @Path("topic-view/{databaseId}/{path}/{topicId}/{viewId}")
    public Response deleteTopicViewPath(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("path") final String path,
            @PathParam("topicId") final String topicId,
            @PathParam("viewId") final String viewId) throws Exception {

        boolean readOnly = false;
        Presto session = createPresto(databaseId, readOnly);

        try {
            PrestoContext context = PathParser.getTopicByPath(session, path, topicId, viewId);

            if (context == null || (context.isMissingTopic() && !context.isLazyTopic())) {
                return Response.status(Status.NOT_FOUND).build();        
            } else {
                PrestoType type = context.getType();
                if (type.isRemovable()) {
                    try {
                        PrestoTopic topic = context.getTopic();
                        if (type.isInline()) {
                            PrestoContext parentContext = context.getParentContext();
                            PrestoField parentField = context.getParentField();
                            PrestoContextRules parentRules = session.getPrestoContextRules(parentContext);
                            
                            PrestoContext updatedParentContext = session.removeFieldValues(parentRules, parentField, Collections.singletonList(topic));

                            // return field data of parent field
                            FieldData fieldData = session.getFieldDataAndProcess(updatedParentContext, parentField);
                            return Response.ok(fieldData).build();
                        } else {
                            session.deleteTopic(topic, type);
                            // 204
                            return Response.noContent().build();
                        }
                    } catch (ConstraintException ce) {
                        return getConstraintMessageResponse(ce);
                    }
                } else {
                    // 403
                    return Response.status(Status.FORBIDDEN).build();
                }
            }

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        }
    }

    @GET
    @Produces(APPLICATION_JSON_UTF8)
    @Path("topic/{databaseId}/{topicId}")
    public Response getTopicInDefaultView(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId,
            @QueryParam("readOnly") final boolean readOnly) throws Exception {
        String path = null;
        String viewId = null;
        return getTopicInViewPath(databaseId, path, topicId, viewId, readOnly);
    }

    @GET
    @Produces(APPLICATION_JSON_UTF8)
    @Path("topic/{databaseId}/{topicId}/{view}")
    public Response getTopicInView(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId,
            @PathParam("viewId") final String viewId,
            @QueryParam("readOnly") final boolean readOnly) throws Exception {
        String path = null;
        return getTopicInViewPath(databaseId, path, topicId, viewId, readOnly);
    }

    @GET
    @Produces(APPLICATION_JSON_UTF8)
    @Path("topic/{databaseId}/{path}/{topicId}/{viewId}")
    public Response getTopicInViewPath(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("path") final String path,
            @PathParam("topicId") final String topicId,
            @PathParam("viewId") final String viewId,
            @QueryParam("readOnly") final boolean readOnly) throws Exception {

        Presto session = createPresto(databaseId, readOnly);

        try {
            PrestoContext context = PathParser.getTopicByPath(session, path, topicId, viewId);

            if (context == null || (context.isMissingTopic() && !context.isLazyTopic())) {
                return Response.status(Status.NOT_FOUND).build();
            }

            Topic result = session.getTopicAndProcess(context);

            return Response.ok(result).build();

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        }
    }

    @GET
    @Produces(APPLICATION_JSON_UTF8)
    @Path("topic-view/{databaseId}/{topicId}")
    public Response getTopicViewInDefaultView(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId,
            @QueryParam("readOnly") final boolean readOnly) throws Exception {
        String path = null;
        String viewId = null;
        return getTopicViewInViewPath(databaseId, path, topicId, viewId, readOnly);
    }

    @GET
    @Produces(APPLICATION_JSON_UTF8)
    @Path("topic-view/{databaseId}/{topicId}/{viewId}")
    public Response getTopicViewInView(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId,
            @PathParam("viewId") final String viewId,
            @QueryParam("readOnly") final boolean readOnly) throws Exception {
        String path = null;
        return getTopicViewInViewPath(databaseId, path, topicId, viewId, readOnly);
    }

    @GET
    @Produces(APPLICATION_JSON_UTF8)
    @Path("topic-view/{databaseId}/{path}/{topicId}/{viewId}")
    public Response getTopicViewInViewPath(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("path") final String path,
            @PathParam("topicId") final String topicId,
            @PathParam("viewId") final String viewId,
            @QueryParam("readOnly") final boolean readOnly) throws Exception {

        Presto session = createPresto(databaseId, readOnly);

        try {
            PrestoContext context = PathParser.getTopicByPath(session, path, topicId, viewId);

            if (context == null || (context.isMissingTopic() && !context.isLazyTopic())) {
                return Response.status(Status.NOT_FOUND).build();
            }

            TopicView result = session.getTopicViewAndProcess(context);

            return Response.ok(result).build();

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        }
    }

    @PUT
    @Produces(APPLICATION_JSON_UTF8)
    @Consumes(APPLICATION_JSON_UTF8)
    @Path("validate-topic/{databaseId}/{topicId}/{viewId}")
    public Response validateTopic(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId, TopicView topicView) throws Exception {
        String path = null;
        return validateTopic(databaseId, path, topicId, viewId, topicView);
    }

    @PUT
    @Produces(APPLICATION_JSON_UTF8)
    @Consumes(APPLICATION_JSON_UTF8)
    @Path("validate-topic/{databaseId}/{path}/{topicId}/{viewId}")
    public Response validateTopic(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("path") final String path, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId, TopicView topicView) throws Exception {

        boolean readOnly = false;
        Presto session = createPresto(databaseId, readOnly);

        try {
            // NOTE: the topicId is the topic that requested the validation, but the 
            // validation needs to start with the topicId of the received topicView. The 
            // former is a descendant of the latter.
            String topicViewTopicId = topicView.getTopicId();

            PrestoContext context = PathParser.getTopicByPath(session, path, topicViewTopicId, viewId);

            if (context == null || (context.isMissingTopic() && !context.isLazyTopic())) {
                return Response.status(Status.NOT_FOUND).build();
            }

            try {
                TopicView result = session.validateTopic(context, topicView);
                session.commit();
    
                return Response.ok(result).build();
                
            } catch (ConstraintException ce) {
                return getConstraintMessageResponse(ce);
            }

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();
        }
    }

    @PUT
    @Produces(APPLICATION_JSON_UTF8)
    @Consumes(APPLICATION_JSON_UTF8)
    @Path("topic-view/{databaseId}/{topicId}/{viewId}")
    public Response updateTopicView(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId, TopicView topicView) throws Exception {
        String path = null;
        return updateTopicView(databaseId, path, topicId, viewId, topicView);
    }

    @PUT
    @Produces(APPLICATION_JSON_UTF8)
    @Consumes(APPLICATION_JSON_UTF8)
    @Path("topic-view/{databaseId}/{path}/{topicId}/{viewId}")
    public Response updateTopicView(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("path") final String path, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId, TopicView topicView) throws Exception {

        boolean readOnly = false;
        Presto session = createPresto(databaseId, readOnly);

        try {
            PrestoContext context = PathParser.getTopicByPath(session, path, topicId, viewId);

            if (context == null || (context.isMissingTopic() && !context.isLazyTopic())) {
                return Response.status(Status.NOT_FOUND).build();
            }
            try {
                Object result = session.updateTopicView(context, topicView);
                session.commit();

                return Response.ok(result).build();

            } catch (ConstraintException ce) {
                return getConstraintMessageResponse(ce);
            }

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();
        }
    }

    @POST
    @Produces(APPLICATION_JSON_UTF8)
    @Consumes(APPLICATION_JSON_UTF8)
    @Path("add-field-values/{databaseId}/{topicId}/{viewId}/{fieldId}")
    public Response addFieldValues( 
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId,
            @PathParam("fieldId") final String fieldId, 
            @QueryParam("index") final Integer index, FieldData fieldData) throws Exception {
        String path = null;
        return addFieldValuesPath(databaseId, path, topicId, viewId, fieldId, index, fieldData);
    }

    @POST
    @Produces(APPLICATION_JSON_UTF8)
    @Consumes(APPLICATION_JSON_UTF8)
    @Path("add-field-values/{databaseId}/{path}/{topicId}/{viewId}/{fieldId}")
    public Response addFieldValuesPath( 
            @PathParam("databaseId") final String databaseId, 
            @PathParam("path") final String path, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId,
            @PathParam("fieldId") final String fieldId, 
            @QueryParam("index") final Integer index, FieldData fieldData) throws Exception {
        boolean isMove = false;
        return performAddFieldValuesPath(databaseId, path, topicId, viewId, fieldId, index, fieldData, isMove);
    }

    private Response performAddFieldValuesPath(String databaseId, String path, String topicId, 
           String viewId, String fieldId, Integer index, FieldData fieldData, boolean isMove) throws Exception {
        boolean readOnly = false;
        Presto session = createPresto(databaseId, readOnly);

        try {
            PrestoContext context = PathParser.getTopicByPath(session, path, topicId, viewId);

            if (context == null || (context.isMissingTopic() && !context.isLazyTopic())) {
                return Response.status(Status.NOT_FOUND).build();
            }

            PrestoField field = context.getFieldById(fieldId);
            PrestoContextRules rules = session.getPrestoContextRules(context);

            if (!rules.isReadOnlyField(field) &&
                    (isMove ? rules.isMovableField(field) : (rules.isAddableField(field) || rules.isCreatableField(field)))) {
                try {
                    FieldData result = session.addFieldValues(rules, field, index, fieldData, isMove);

                    session.commit();

                    return Response.ok(result).build();
                } catch (ConstraintException ce) {
                    return getConstraintMessageResponse(ce);
                }
            } else {
                // 403
                return Response.status(Status.FORBIDDEN).build();
            }

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        } 
    }

    @POST
    @Produces(APPLICATION_JSON_UTF8)
    @Consumes(APPLICATION_JSON_UTF8)
    @Path("update-field-values/{databaseId}/{topicId}/{viewId}/{fieldId}")
    public Response updateFieldValuesPath( 
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId,
            @PathParam("fieldId") final String fieldId, FieldData fieldData) throws Exception {

        String path = null;
        return updateFieldValuesPath(databaseId, path, topicId, viewId, fieldId, fieldData);
    }

    @POST
    @Produces(APPLICATION_JSON_UTF8)
    @Consumes(APPLICATION_JSON_UTF8)
    @Path("update-field-values/{databaseId}/{path}/{topicId}/{viewId}/{fieldId}")
    public Response updateFieldValuesPath( 
            @PathParam("databaseId") final String databaseId, 
            @PathParam("path") final String path, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId,
            @PathParam("fieldId") final String fieldId, FieldData fieldData) throws Exception {

        boolean readOnly = false;
        Presto session = createPresto(databaseId, readOnly);

        try {
            PrestoContext context = PathParser.getTopicByPath(session, path, topicId, viewId);

            if (context == null || (context.isMissingTopic() && !context.isLazyTopic())) {
                return Response.status(Status.NOT_FOUND).build();
            }

            PrestoField field = context.getFieldById(fieldId);
            PrestoContextRules rules = session.getPrestoContextRules(context);

            if (!rules.isReadOnlyField(field) && 
                    (rules.isAddableField(field) || rules.isRemovableField(field) || rules.isCreatableField(field))) { // TODO: what are the rules?
                try {
                    FieldData result = session.updateFieldValues(rules, field, fieldData);

                    session.commit();

                    return Response.ok(result).build();
                } catch (ConstraintException ce) {
                    return getConstraintMessageResponse(ce);
                }
            } else {
                // 403
                return Response.status(Status.FORBIDDEN).build();
            }

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        } 
    }

    @POST
    @Produces(APPLICATION_JSON_UTF8)
    @Consumes(APPLICATION_JSON_UTF8)
    @Path("remove-field-values/{databaseId}/{topicId}/{viewId}/{fieldId}")
    public Response removeFieldValues(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId,
            @PathParam("fieldId") final String fieldId, FieldData fieldData) throws Exception {
        String path = null;
        return removeFieldValuesPath(databaseId, path, topicId, viewId, fieldId, fieldData);
    }

    @POST
    @Produces(APPLICATION_JSON_UTF8)
    @Consumes(APPLICATION_JSON_UTF8)
    @Path("remove-field-values/{databaseId}/{path}/{topicId}/{viewId}/{fieldId}")
    public Response removeFieldValuesPath(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("path") final String path,
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId,
            @PathParam("fieldId") final String fieldId, FieldData fieldData) throws Exception {

        boolean readOnly = false;
        Presto session = createPresto(databaseId, readOnly);

        try {
            PrestoContext context = PathParser.getTopicByPath(session, path, topicId, viewId);

            if (context == null || (context.isMissingTopic() && !context.isLazyTopic())) {
                return Response.status(Status.NOT_FOUND).build();
            }

            PrestoField field = context.getFieldById(fieldId);
            PrestoContextRules rules = session.getPrestoContextRules(context);

            if (!rules.isReadOnlyField(field) && rules.isRemovableField(field)) {
                try {
                    FieldData result =  session.removeFieldValues(rules, field, fieldData);

                    session.commit();

                    return Response.ok(result).build();
                } catch (ConstraintException ce) {
                    return getConstraintMessageResponse(ce);
                }
            } else {
                // 403
                return Response.status(Status.FORBIDDEN).build();
            }
        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        } 
    }

    @POST
    @Produces(APPLICATION_JSON_UTF8)
    @Consumes(APPLICATION_JSON_UTF8)
    @Path("move-field-values-to-index/{databaseId}/{topicId}/{viewId}/{fieldId}")
    public Response moveFieldValuesToIndex( 
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId,
            @PathParam("fieldId") final String fieldId, 
            @QueryParam("index") final Integer index, FieldData fieldData) throws Exception {
        String path = null;
        return moveFieldValuesToIndexPath(databaseId, path, topicId, viewId, fieldId, index, fieldData);
    }

    @POST
    @Produces(APPLICATION_JSON_UTF8)
    @Consumes(APPLICATION_JSON_UTF8)
    @Path("move-field-values-to-index/{databaseId}/{path}/{topicId}/{viewId}/{fieldId}")
    public Response moveFieldValuesToIndexPath( 
            @PathParam("databaseId") final String databaseId, 
            @PathParam("path") final String path, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId,
            @PathParam("fieldId") final String fieldId, 
            @QueryParam("index") final Integer index, FieldData fieldData) throws Exception {
        boolean isMove = true;
        return performAddFieldValuesPath(databaseId, path, topicId, viewId, fieldId, index, fieldData, isMove);
    }

    @GET
    @Produces(APPLICATION_JSON_UTF8)
    @Path("available-field-values/{databaseId}/{topicId}/{viewId}/{fieldId}")
    public Response getAvailableFieldValues( 
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId,
            @PathParam("fieldId") final String fieldId,
            @QueryParam("query") final String query) throws Exception {
        String path = null;
        return getAvailableFieldValuesPath(databaseId, path, topicId, viewId, fieldId, query);
    }

    @GET
    @Produces(APPLICATION_JSON_UTF8)
    @Path("available-field-values/{databaseId}/{path}/{topicId}/{viewId}/{fieldId}")
    public Response getAvailableFieldValuesPath( 
            @PathParam("databaseId") final String databaseId, 
            @PathParam("path") final String path, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId,
            @PathParam("fieldId") final String fieldId,
            @QueryParam("query") final String query) throws Exception {

        boolean readOnly = false;
        Presto session = createPresto(databaseId, readOnly);

        try {
            PrestoContext context = PathParser.getTopicByPath(session, path, topicId, viewId);

            if (context == null || (context.isMissingTopic() && !context.isLazyTopic())) {
                return Response.status(Status.NOT_FOUND).build();
            }

            PrestoField field = context.getFieldById(fieldId);

            PrestoContextRules rules = session.getPrestoContextRules(context);
            AvailableFieldValues result = session.getAvailableFieldValuesInfo(rules, field, query);
            return Response.ok(result).build();

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();      
        }
    }

    @POST
    @Produces(APPLICATION_JSON_UTF8)
    @Consumes(APPLICATION_JSON_UTF8)
    @Path("execute-field-action/{databaseId}/{topicId}/{viewId}/{fieldId}/{actionId}")
    public Response executeFieldAction(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId, 
            @PathParam("fieldId") final String fieldId, 
            @PathParam("actionId") final String actionId, 
            TopicView topicView) throws Exception {
        String path = null;
        return executeFieldActionPath(databaseId, path, topicId, viewId, fieldId, actionId, topicView);
    }

    @POST
    @Produces(APPLICATION_JSON_UTF8)
    @Consumes(APPLICATION_JSON_UTF8)
    @Path("execute-field-action/{databaseId}/{path}/{topicId}/{viewId}/{fieldId}/{actionId}")
    public Response executeFieldActionPath(
            @PathParam("databaseId") final String databaseId, 
            @PathParam("path") final String path, 
            @PathParam("topicId") final String topicId, 
            @PathParam("viewId") final String viewId, 
            @PathParam("fieldId") final String fieldId, 
            @PathParam("actionId") final String actionId, 
            TopicView topicView) throws Exception {

        boolean readOnly = false;
        Presto session = createPresto(databaseId, readOnly);

        try {
            PrestoContext context = PathParser.getTopicByPath(session, path, topicId, viewId);

            if (context == null || (context.isMissingTopic() && !context.isLazyTopic())) {
                return Response.status(Status.NOT_FOUND).build();
            }

            try {
                PrestoField field = context.getFieldById(fieldId);

                TopicView result = session.executeFieldAction(context, topicView, field, actionId);
                session.commit();
    
                return Response.ok(result).build();
                
            } catch (ConstraintException ce) {
                return getConstraintMessageResponse(ce);
            }

        } catch (Exception e) {
            session.abort();
            throw e;
        } finally {
            session.close();
        }
    }

    // overridable methods

    public abstract class EditorResourcePresto extends Presto {

        public EditorResourcePresto(String databaseId, String databaseName, PrestoSchemaProvider schemaProvider, PrestoDataProvider dataProvider, PrestoAttributes attributes) {
            super(databaseId, databaseName, schemaProvider, dataProvider, attributes);
        }

        @Override
        public URI getBaseUri() {
            return uriInfo.getBaseUri();
        }

        @Override
        protected ChangeSetHandler getChangeSetHandler() {
            return new DefaultChangeSetHandler() {

                @Override
                protected PrestoSchemaProvider getSchemaProvider() {
                    return EditorResourcePresto.this.getSchemaProvider();
                }

                @Override
                public void onAfterSave(PrestoChangeSet changeSet) {
                    super.onAfterSave(changeSet);
                    PrestoChanges changes = changeSet.getPrestoChanges();
                    for (PrestoUpdate create : changes.getCreated()) {
                        EditorResource.this.onTopicCreated(create.getTopicAfterSave());
                    }
                    for (PrestoUpdate update : changes.getUpdated()) {
                        if (update.isTopicUpdated()) {
                            EditorResource.this.onTopicUpdated(update.getTopic());
                        }
                    }
                    for (PrestoTopic delete : changes.getDeleted()) {
                        EditorResource.this.onTopicDeleted(delete);                        
                    }
                }

                @SuppressWarnings("unchecked")
                @Override
                protected Collection<? extends Object> getVariableValues(PrestoTopic topic, PrestoType type, PrestoField field, String variable) {
                    Object value = getAttribute(variable);
                    if (value instanceof Collection) {
                        return (Collection<? extends Object>)value;
                    } else if (value != null) {
                        return Collections.singleton(value);
                    } else {
                        return Collections.emptyList();
                    }
                }
            };
        }
    }

    private Response getConstraintMessageResponse(ConstraintException ce) {
        TopicMessage entity = new TopicMessage(ce.getType(), ce.getTitle(), ce.getMessage());
        return Response.status(422).entity(entity).build();
    }

    @Override
    public Object getAttribute(String name) {
        Object result = request.getAttribute(name);
        if (result == null) {
            if (name.equals("now")) {
                result = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date());
            } else if (name.equals("username")) {
                if (request != null) {
                    String remoteUser = request.getRemoteUser();
                    if (remoteUser != null) {
                        result = remoteUser;
                    }
                }
            }
        }
        return result;
    }

    protected PrestoAttributes getAttributes() {
        return this;
    }

    protected abstract Presto createPresto(String databaseId, boolean readOnlyMode);
    
    protected abstract Collection<String> getDatabaseIds();

    protected abstract String getDatabaseName(String databaseId);

    protected void onTopicCreated(PrestoTopic topic) {      
    }

    protected void onTopicUpdated(PrestoTopic topic) {      
    }

    protected void onTopicDeleted(PrestoTopic topic) {      
    }

}
