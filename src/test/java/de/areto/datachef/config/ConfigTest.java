package de.areto.datachef.config;

import org.aeonbits.owner.Accessible;
import org.aeonbits.owner.ConfigCache;
import org.junit.Ignore;
import org.junit.Test;
import org.reflections.Reflections;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ConfigTest {

    @Test
    public void configurationShouldBePresent() {
        final DWHConfig dwhConfig = ConfigCache.getOrCreate(DWHConfig.class);
        assertNotNull(dwhConfig);
        assertNotNull(dwhConfig.jdbcConnectionString());
        assertNotNull(dwhConfig.jdbcDriverURL());

        final SinkConfig sinkConfig = ConfigCache.getOrCreate(SinkConfig.class);
        assertNotNull(sinkConfig);

        final DataVaultConfig dvConfig = ConfigCache.getOrCreate(DataVaultConfig.class);
        assertNotNull(dvConfig);

        final DataChefConfig dcConfig = ConfigCache.getOrCreate(DataChefConfig.class);
        assertNotNull(dcConfig);
        assertEquals(InstanceRole.DEVELOPMENT, dcConfig.instanceRole());

        final TemplateConfig tplConfig = ConfigCache.getOrCreate(TemplateConfig.class);
        assertNotNull(tplConfig);

        assertThat(tplConfig.populateSatelliteTemplates()).isNotEmpty();

        final RepositoryConfig repositoryConfig = ConfigCache.getOrCreate(RepositoryConfig.class);
        assertThat(repositoryConfig).isNotNull();
    }

    @Test
    @Ignore
    public void writeConfigContents() throws Exception {
        Reflections r = new Reflections(Constants.DATA_CHEF_BASE_PACKAGE);
        final Set<Class<? extends Accessible>> configClasses = r.getSubTypesOf(Accessible.class);

        final Path dir = Paths.get("config/");
        if(!Files.exists(dir)) Files.createDirectory(dir);

        for (Class<? extends Accessible> configClass : configClasses) {
            final Accessible instance = ConfigCache.getOrCreate(configClass);
            String fName = configClass.getSimpleName() + ".properties";
            File configFile = new File("config/" + fName);
            if(configFile.exists()) configFile.delete();
            instance.store(new FileOutputStream(configFile), configClass.getSimpleName());
        }
    }
}
