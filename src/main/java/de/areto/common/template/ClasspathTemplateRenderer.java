package de.areto.common.template;

import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;

public class ClasspathTemplateRenderer extends TemplateRenderer {

    public ClasspathTemplateRenderer(String templateName) {
        super(templateName);
    }

    @Override
    VelocityEngine configureVelocityEngine() {
        final VelocityEngine engine = new VelocityEngine();
        engine.setProperty(RuntimeConstants.RESOURCE_LOADER, "classpath");
        engine.setProperty("classpath.resource.loader.class", ClasspathResourceLoader.class.getName());
        engine.setProperty(RuntimeConstants.RUNTIME_LOG_LOGSYSTEM_CLASS, "org.apache.velocity.runtime.log.NullLogChute");
        engine.init();
        return engine;
    }
}
