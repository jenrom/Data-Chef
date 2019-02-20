package de.areto.common.template;

import de.areto.datachef.config.TemplateConfig;
import de.areto.datachef.config.WebAppConfig;
import org.aeonbits.owner.ConfigCache;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;

public class WebTemplateRenderer extends TemplateRenderer {

    public WebTemplateRenderer(String templateName) {
        super(templateName);
    }

    @Override
    VelocityEngine configureVelocityEngine() {
        final String tplPath = ConfigCache.getOrCreate(WebAppConfig.class).templatePath();
        final VelocityEngine engine = new VelocityEngine();
        engine.setProperty("file.resource.loader.path", tplPath);
        engine.setProperty(RuntimeConstants.RUNTIME_LOG_LOGSYSTEM_CLASS,
                "org.apache.velocity.runtime.log.NullLogChute");
        engine.init();
        return engine;
    }

}
