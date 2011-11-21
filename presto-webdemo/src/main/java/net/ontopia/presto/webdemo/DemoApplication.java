package net.ontopia.presto.webdemo;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.core.Application;


public class DemoApplication extends Application {

    @Override
    public Set<Class<?>> getClasses() {
        Set<Class<?>> result = new HashSet<Class<?>>();
        result.add(DemoEditorResource.class);
        return result;
    }

}
