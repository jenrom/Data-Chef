package de.areto.datachef.config;

import org.aeonbits.owner.Accessible;
import org.aeonbits.owner.Config.LoadPolicy;
import org.aeonbits.owner.Config.LoadType;
import org.aeonbits.owner.Config.PreprocessorClasses;
import org.aeonbits.owner.Config.Sources;

@LoadPolicy(LoadType.FIRST)
@Sources({
        "file:./config/repository.config.properties"
})
@PreprocessorClasses(Trim.class)
public interface RepositoryConfig extends Accessible {

    @DefaultValue("jdbc:mysql://localhost:3306/datachef?useSSL=false")
    String jdbcConnectionString();

    @DefaultValue("com.mysql.cj.jdbc.Driver")
    String driverClass();

    @DefaultValue("org.hibernate.dialect.MariaDB10Dialect")
    String databaseDialect();

    @DefaultValue("datachef")
    String catalog();

    @DefaultValue("datachef")
    String username();

    @DefaultValue("datachef")
    String password();
}
