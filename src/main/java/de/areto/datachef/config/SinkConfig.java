package de.areto.datachef.config;

import org.aeonbits.owner.Accessible;
import org.aeonbits.owner.Config.LoadPolicy;
import org.aeonbits.owner.Config.LoadType;
import org.aeonbits.owner.Config.PreprocessorClasses;
import org.aeonbits.owner.Config.Sources;

import java.util.List;

@LoadPolicy(LoadType.FIRST)
@Sources({
        "file:./config/sink.config.properties"
})
@PreprocessorClasses(Trim.class)
public interface SinkConfig extends Accessible {

    @DefaultValue("./sink")
    String path();

    @DefaultValue("./sink/served")
    String dirServed();

    @DefaultValue("./sink/rotten")
    String dirRotten();

    @DefaultValue("./sink/rollback")
    String dirRollback();

    @DefaultValue("sink")
    String mappingFileExtension();

    @DefaultValue("mart")
    String martFileExtension();

    @DefaultValue("csv,txt")
    List<String> dataFileExtensions();

    @DefaultValue("german_csv")
    String defaultCsvType();

    @DefaultValue("true")
    boolean moveFiles();

    @DefaultValue("true")
    boolean loadFilesOnStartup();

    @DefaultValue("true")
    boolean deleteDuplicates();

    @DefaultValue("true")
    boolean watchSink();

    @DefaultValue("10000")
    long waitGrowthTime();
}
