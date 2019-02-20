package de.areto.common.template;

import de.areto.datachef.config.*;
import de.areto.datachef.exceptions.RenderingException;
import de.areto.datachef.config.ConfigUtility;
import lombok.NonNull;
import org.aeonbits.owner.ConfigCache;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

public abstract class TemplateRenderer {

    private final VelocityEngine engine;
    private final String templateName;

    public TemplateRenderer(@NonNull String templateName) {
        this.templateName = templateName;
        this.engine = configureVelocityEngine();
    }

    abstract VelocityEngine configureVelocityEngine();

    private Map<String, Object> addDefaults(@NonNull Map<String, Object> context) {
        final Map<String, Object> contextWithDefaults = new HashMap<>();
        contextWithDefaults.put("ConfigUtility", ConfigUtility.class);
        contextWithDefaults.put("dwhConfig", ConfigCache.getOrCreate(DWHConfig.class));
        contextWithDefaults.put("sinkConfig", ConfigCache.getOrCreate(SinkConfig.class));
        contextWithDefaults.put("vaultConfig", ConfigCache.getOrCreate(DataVaultConfig.class));
        contextWithDefaults.put("chefConfig", ConfigCache.getOrCreate(DataChefConfig.class));

        contextWithDefaults.putAll(context);
        return contextWithDefaults;
    }

    public String render(@NonNull Map<String, Object> context) throws RenderingException {
        try {
            final Map<String, Object> contextWithDefaults = addDefaults(context);
            final Template t = engine.getTemplate(templateName);
            VelocityContext vc = new VelocityContext(contextWithDefaults);
            StringWriter writer = new StringWriter();
            t.merge(vc, writer);
            return writer.toString();
        } catch (Exception e) {
            final String mTpl = "Rendering template '%s' failed with %s%s";
            final String eMsg = e.getMessage() != null ? ": " + e.getMessage() : "";
            final String msg = String.format(mTpl, templateName, e.getClass().getSimpleName(), eMsg);
            throw new RenderingException(msg);
        }
    }
}
