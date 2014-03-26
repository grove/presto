package net.ontopia.presto.jaxrs;

import net.ontopia.presto.spi.PrestoField;
import net.ontopia.presto.spi.PrestoTopic;
import net.ontopia.presto.spi.utils.PrestoContext;
import net.ontopia.presto.spi.utils.PrestoContextField;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PathParserTest extends AbstractPrestoTest  {

    private TestEditorResource resource;
    private Presto session;

    @Before 
    public void setUp() {
        String databaseId = "path-parser";
        createProviders(databaseId, "path-parser.data.json");
        this.resource = new TestEditorResource(schemaProvider, dataProvider);
        this.session = resource.createPresto(databaseId, false);
    }

    @Test
    public void testGetContextField() {
        PrestoContextField contextField = PathParser.getContextField(session, "o:root$pinfo$children$o:child1$cinfo$grandchildren");
        
        PrestoContext context = contextField.getContext();
        PrestoTopic topic = context.getTopic();
        Assert.assertEquals(topic.getId(), "o:child1");
        
        PrestoField field = contextField.getField();
        Assert.assertEquals(field.getId(), "grandchildren");
    }

    @Test
    public void testGetTopicByPathRoot() {
        PrestoContext topic = PathParser.getTopicByPath(session, "_", "o:root", "pinfo");
        Assert.assertEquals(topic.getTopicId(), "o:root");
    }

    @Test
    public void testGetTopicByPathChild() {
        PrestoContext topic = PathParser.getTopicByPath(session, "o:root$pinfo$children", "o:child1", "cinfo");
        Assert.assertEquals(topic.getTopicId(), "o:child1");
    }

    @Test
    public void testGetTopicByPathChildDirect() {
        PrestoContext topic = PathParser.getTopicByPath(session, "_", "o:child1", "cinfo");
        Assert.assertEquals(topic.getTopicId(), "o:child1");
    }

    @Test
    public void testGetTopicByPathGrandchild() {
        PrestoContext topic = PathParser.getTopicByPath(session, "o:root$pinfo$children$o:child1$cinfo$grandchildren", "o:grandchild1", "ginfo");
        Assert.assertEquals(topic.getTopicId(), "o:grandchild1");
    }

    @Test
    public void testGetTopicByPathGrandchildViaChild() {
        PrestoContext topic = PathParser.getTopicByPath(session, "o:child1$cinfo$grandchildren", "o:grandchild1", "ginfo");
        Assert.assertEquals(topic.getTopicId(), "o:grandchild1");
    }

    @Test
    public void testGetInlineTopicPath() {
        String inPath = "o:root$pinfo$children$o:child1$cinfo$grandchildren";
        PrestoContextField contextField = PathParser.getContextField(session, inPath);
        PrestoContext context = contextField.getContext();
        PrestoField field = contextField.getField();
        String outPath = PathParser.getInlineTopicPath(context, field);
        Assert.assertEquals(outPath, inPath);
    }
    
}
