package net.ontopia.presto.jaxrs;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;

import net.ontopia.presto.spi.PrestoDataProvider;
import net.ontopia.presto.spi.PrestoSchemaProvider;

import org.junit.Ignore;

@Ignore
public class TestEditorResource extends EditorResource {

    private PrestoSchemaProvider schemaProvider_;
    private PrestoDataProvider dataProvider_;

    public TestEditorResource(PrestoSchemaProvider schemaProvider, PrestoDataProvider dataProvider) {
        this.schemaProvider_ = schemaProvider;
        this.dataProvider_ = dataProvider;
    }
    
    @Override
    protected Presto createPresto(String databaseId, boolean readOnlyMode) {
        return new EditorResourcePresto(databaseId, getDatabaseName(databaseId), schemaProvider_, dataProvider_, getAttributes()) {
            @Override
            public URI getBaseUri() {
                try {
                    return new URI("http://example.org/test");
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Override
    protected Collection<String> getDatabaseIds() {
        return Collections.emptyList();
    }

    @Override
    protected String getDatabaseName(String databaseId) {
        return null;
    }
}