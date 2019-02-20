package de.areto.common.template;

import de.areto.datachef.config.TemplateConfig;
import org.aeonbits.owner.ConfigCache;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class TemplateRendererTest {

    @Test
    @Ignore
    public void templateShouldBeFound() throws Exception {
        final String tplPath = ConfigCache.getOrCreate(TemplateConfig.class).templatePath();
        final String tplName = "test-" + UUID.randomUUID().toString() + ".vm";
        final File tempFile = new File("./" + tplPath + tplName);
        final PrintWriter writer = new PrintWriter(tempFile);
        writer.print("Hello $name.");
        writer.flush();
        writer.close();
        final SQLTemplateRenderer renderer = new SQLTemplateRenderer(tplName);
        final Map<String, Object> context = new HashMap<>();
        context.put("name", "Areto");
        final String r = renderer.render(context);
        assertThat(r).isEqualTo("Hello Areto.");
        assertThat(tempFile.delete()).isTrue();
    }

    @Test
    @Ignore
    public void templateShouldBeRendered() throws Exception {
        final ClasspathTemplateRenderer renderer = new ClasspathTemplateRenderer("test.vm");
        final Map<String, Object> context = new HashMap<>();
        context.put("name", "Areto");
        assertThat(renderer.render(context)).isEqualTo("Hello Areto.");
    }
}
