package de.areto.common.template;

import de.areto.datachef.config.DataChefConfig;
import de.areto.datachef.config.TemplateConfig;
import org.aeonbits.owner.ConfigCache;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;

public class SQLTemplateRenderer extends TemplateRenderer {

    public SQLTemplateRenderer(String templateName) {
        super(templateName);
    }

    @Override
    VelocityEngine configureVelocityEngine() {
        final String tplPath = ConfigCache.getOrCreate(TemplateConfig.class).templatePath();
        final VelocityEngine engine = new VelocityEngine();
        engine.setProperty("file.resource.loader.path", tplPath);
        engine.setProperty(RuntimeConstants.RUNTIME_LOG_LOGSYSTEM_CLASS,
                "org.apache.velocity.runtime.log.NullLogChute");
        engine.init();
        return engine;
    }
}
