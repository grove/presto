package net.ontopia.presto.jaxrs;

import net.ontopia.presto.spi.utils.PrestoContext;

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
    public void testGetTopicByPathGrandchild() {
        PrestoContext topic = PathParser.getTopicByPath(session, "o:root$pinfo$children$o:child1$cinfo$grandchildren", "o:grandchild1", "ginfo");
        Assert.assertEquals(topic.getTopicId(), "o:grandchild1");
    }

}
