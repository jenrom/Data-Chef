package de.areto.datachef.parser.mapping;

import de.areto.common.util.StringUtility;
import de.areto.datachef.config.SinkConfig;
import de.areto.datachef.model.mapping.ConnectionType;
import de.areto.datachef.model.mapping.CsvType;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.model.mapping.StagingMode;
import de.areto.datachef.parser.TriggerConfigurationParser;
import de.areto.datachef.parser.antlr4.SinkDSLParser;
import lombok.NonNull;
import org.aeonbits.owner.ConfigCache;
import org.hibernate.Session;

import java.util.Optional;

public class MappingConfigurationParser {

    private final Mapping mapping;
    private final Session session;

    private final SinkConfig sinkConfig = ConfigCache.getOrCreate(SinkConfig.class);

    public MappingConfigurationParser(Mapping mapping, Session session) {
        this.mapping = mapping;
        this.session = session;
    }

    public Mapping parseConfiguration(@NonNull SinkDSLParser.CompilationUnitContext ctx) {
        // Default assumptions:
        loadCsvType(sinkConfig.defaultCsvType());
        mapping.setStagingMode(StagingMode.FILE);
        mapping.setConnectionType(ConnectionType.JDBC);
        mapping.setTriggeredByCron(false);
        mapping.setTriggeredByMousetrap(false);

        if(ctx.configuration() != null && ctx.configuration().configParam != null) {
            for (SinkDSLParser.ConfigParamContext configCtxt : ctx.configuration().configParam()) {
                parseLoadTypeConfig(configCtxt.loadTypeConfig());
                parseStageConfig(configCtxt.stageConfig());
                parseTrigger(configCtxt.triggerConfig());
            }
        }

        if(mapping.getStagingMode().equals(StagingMode.FILE) && mapping.isTriggeredByCron()) {
            mapping.addIssue("Staging via file cannot be trigger via cron");
        }

        if(mapping.getStagingMode().equals(StagingMode.FILE) && mapping.isTriggeredByMousetrap()) {
            mapping.addIssue("Staging via file cannot be trigger via trap");
        }

        if(ctx.sql() != null) {
            final String customSqlCode = StringUtility.trimAndRemoveQuotes(ctx.sql().code.getText().trim());
            mapping.setCustomSqlCode(customSqlCode);
        }

        if(mapping.getStagingMode().equals(StagingMode.CONNECTION) && !mapping.hasCustomSql()) {
            mapping.addIssue("Staging via import requires custom SQL");
        }

        if(mapping.getStagingMode().equals(StagingMode.INSERT) && !mapping.hasCustomSql()) {
            mapping.addIssue("Staging via insert requires custom SQL");
        }

        return mapping;
    }

    /**
     * ANTLR Grammar:
     * <pre>
     *     'trigger' ':' ( defaultTrigger | cronTrigger | mousetrapTrigger )
     * </pre>
     * @param ctx
     */
    private void parseTrigger(SinkDSLParser.TriggerConfigContext ctx) {
        if(ctx == null) return;

        TriggerConfigurationParser triggerConfigurationParser = new TriggerConfigurationParser(mapping, session);
        if(ctx.defaultTrigger() != null)
            return;
        else
            triggerConfigurationParser.parseTrigger(ctx);
    }

    /**
     * ANTLR Grammar:
     * <pre>
     *     'stage' ':' (
     *            mod = 'import' 'from' ( 'exa' | 'ora' | 'jdbc' )? connName = ID
     *          | mod = 'insert'
     *          | mod = 'file' ('of' 'type' csvType = ID)?
     *      )
     * </pre>
     * @param ctx
     */
    private void parseStageConfig(SinkDSLParser.StageConfigContext ctx) {
        if(ctx == null || ctx.mod == null) return;

        final String mode = ctx.mod.getText();

        switch (mode) {
            case "import": mapping.setStagingMode(StagingMode.CONNECTION); break;
            case "insert": mapping.setStagingMode(StagingMode.INSERT); break;
            case "file" : mapping.setStagingMode(StagingMode.FILE); break;
            default: mapping.setStagingMode(StagingMode.FILE); break;
        }

        if(ctx.csvType != null) {
            loadCsvType(ctx.csvType.getText());
        }

        if(ctx.connectionName != null) {
            mapping.setConnectionName(ctx.connectionName.getText());
        }

        if(ctx.connectionType != null) {
            switch (ctx.connectionType.getText()) {
                case "exa": mapping.setConnectionType(ConnectionType.EXA); break;
                case "ora": mapping.setConnectionType(ConnectionType.ORA); break;
                case "jdbc": mapping.setConnectionType(ConnectionType.JDBC); break;
                default: mapping.setConnectionType(ConnectionType.JDBC); break;
            }
        }
    }

    /**
     * ANTLR Grammar:
     * <pre>'load' ':' type = ( 'full' | 'partial' )</pre>
     * @param ctx
     */
    private void parseLoadTypeConfig(SinkDSLParser.LoadTypeConfigContext ctx) {
        if(ctx != null && ctx.type != null) {
            if(ctx.type.getText().equals("full"))
                mapping.setFullLoad(true);
            else if(ctx.type.getText().equals("partial"))
                mapping.setFullLoad(false);
            else
                mapping.setFullLoad(false);
        }
    }

    private void loadCsvType(String csvType) {
        final Optional<CsvType> typeOptional = session.byId(CsvType.class).loadOptional(csvType);
        if(!typeOptional.isPresent()) {
            final String msg = String.format("Unable to load CsvType '%s'", typeOptional);
            mapping.addIssue(msg);
        } else {
            mapping.setCsvType(typeOptional.get());
        }
    }
}
