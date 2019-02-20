package de.areto.datachef.config;

import org.aeonbits.owner.Accessible;
import org.aeonbits.owner.Config.PreprocessorClasses;
import org.aeonbits.owner.Config.LoadPolicy;
import org.aeonbits.owner.Config.LoadType;
import org.aeonbits.owner.Config.Sources;

import java.util.List;

@LoadPolicy(LoadType.FIRST)
@Sources({
        "file:./config/template.config.properties"
})
@PreprocessorClasses(Trim.class)
public interface TemplateConfig extends Accessible {

    @DefaultValue("templates/exasol")
    String templatePath();

    @DefaultValue("dwh/setup_dwh.vm")
    String setupDwhTemplate();

    @DefaultValue("repository/initialize.vm")
    String initializeRepositoryTemplate();

    @DefaultValue("false")
    boolean enablePreRunScript();

    @DefaultValue("false")
    boolean enablePostRunScript();

    @DefaultValue("worker/pre_run.vm")
    String workerPreRunTemplate();

    @DefaultValue("worker/post_run.vm")
    String workerPostRunTemplate();

    @DefaultValue("table/table_create.vm")
    String createTableTemplate();

    @DefaultValue("table/table_drop.vm")
    String dropTableTemplate();

    @DefaultValue("table/view_drop.vm")
    String dropViewTemplate();

    @DefaultValue("table/table_truncate.vm")
    String truncateTableTemplate();

    @DefaultValue("table/table_delete_ids.vm")
    String deleteIdsFromTableTemplate();

    @DefaultValue("table/constraint_primary_key.vm")
    String createPrimaryKeyConstraintTemplate();

    @DefaultValue("table/constraint_foreign_key.vm")
    String createForeignKeyConstraintTemplate();

    @DefaultValue("staging/import_file.vm")
    String importFileTemplate();

    @DefaultValue("staging/import_connection.vm")
    String importConnectionTemplate();

    @DefaultValue("staging/import_insert.vm")
    String importInsertTemplate();

    @DefaultValue("datavault/populate_hub.vm")
    String populateHubTemplate();

    @DefaultValue("datavault/populate_link.vm")
    String populateLinkTemplate();

    @DefaultValue("datavault/populate_satellite.vm")
    String populateSatelliteTemplates();

    @DefaultValue("datavault/populate_satellite_delete_detection_full_load.vm")
    String satDeleteDetectionOnFullLoadTemplate();

    @DefaultValue("datavault/populate_history_satellite.vm")
    String populateHistorySatelliteTemplates();

    @DefaultValue("datavault/populate_history_satellite_delete_detection_full_load.vm")
    String histSatDeleteDetectionOnFullLoadTemplate();

    @DefaultValue("views/hub_1_latest_view.vm")
    List<String> viewForHubTemplates();

    @DefaultValue("views/link_1_latest_view.vm")
    List<String> viewForLinkTemplates();

    @DefaultValue("mart/populate_mart_load.vm")
    String martLoadTemplate();

    @DefaultValue("mart/populate_mart_reload.vm")
    String martReloadTemplate();

    @DefaultValue("mart/populate_mart_historical.vm")
    String martHistoricalTemplate();

    @DefaultValue("mart/populate_mart_merged.vm")
    String martMergedTemplate();
}
