package de.areto.datachef.config;

import org.aeonbits.owner.Accessible;
import org.aeonbits.owner.Config.LoadPolicy;
import org.aeonbits.owner.Config.LoadType;
import org.aeonbits.owner.Config.PreprocessorClasses;
import org.aeonbits.owner.Config.Sources;

@LoadPolicy(LoadType.FIRST)
@Sources({
        "file:./config/datachef.config.properties"
})
@PreprocessorClasses(Trim.class)
public interface DataChefConfig extends Accessible {

    @DefaultValue("DEVELOPMENT")
    InstanceRole instanceRole();

    @DefaultValue("areto")
    String customerName();

    @DefaultValue("true")
    boolean activateCronSchedules();

    @DefaultValue("true")
    boolean activateMouseTraps();
}
