package de.areto.datachef.config;

import org.aeonbits.owner.Config;
import org.aeonbits.owner.Config.PreprocessorClasses;

@PreprocessorClasses(Trim.class)
public interface WebAppConfig extends Config {

    @DefaultValue("4567")
    int port();

    @DefaultValue("20")
    int maxThreads();
    
    @DefaultValue("1")
    int minThreads();
    
    @DefaultValue("30000")
    int timeout();

    @DefaultValue("/public")
    String staticFileLocation();

    @DefaultValue("true")
    boolean useExternalStaticFileLocation();

    @DefaultValue("templates/web/")
    String templatePath();

}
