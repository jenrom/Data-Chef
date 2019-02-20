package de.areto.datachef.config;

import org.aeonbits.owner.Config;
import org.aeonbits.owner.Config.PreprocessorClasses;

@Config.LoadPolicy(Config.LoadType.FIRST)
@Config.Sources({
        "file:./config/mart.config.properties",
        "classpath:./config/mart.config.properties"
})
@PreprocessorClasses(Trim.class)
public interface MartConfig extends Config {

    @DefaultValue("chef_id")
    String idColumnDataDomain();

}
