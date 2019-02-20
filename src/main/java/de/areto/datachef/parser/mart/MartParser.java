package de.areto.datachef.parser.mart;

import de.areto.common.util.StringUtility;
import de.areto.datachef.model.mart.Mart;
import de.areto.datachef.model.mart.MartColumn;
import de.areto.datachef.model.mart.MartType;
import de.areto.datachef.parser.TriggerConfigurationParser;
import de.areto.datachef.parser.antlr4.MartDSLBaseVisitor;
import de.areto.datachef.parser.antlr4.MartDSLParser;
import lombok.NonNull;
import org.hibernate.Session;

import java.util.HashSet;
import java.util.Set;

public class MartParser extends MartDSLBaseVisitor<Mart> {

    private final Session session;
    private final Mart mart = new Mart();
    private final Set<String> columnNames = new HashSet<>();

    public MartParser(@NonNull Session session, @NonNull String martName) {
        this.session = session;
        this.mart.setName(martName);
    }

    @Override
    public Mart visitCompilationUnit(MartDSLParser.CompilationUnitContext unitContext) {

        int idColumnCount=0;
        int kcColumnCount=0;

        for (MartDSLParser.MartColumnContext colCtx : unitContext.mapping().martColumn()) {
            final MartColumn column = new MartColumnParser(session, mart).visitMartColumn(colCtx);
            if(column.isIdentityColumn()){
                idColumnCount++;
            }

            if(idColumnCount > 1){
                String msg = "Column '%s' - Multiple id columns is prohibited";
                msg = String.format(msg, column.getName());
                mart.addIssue(msg);
                idColumnCount--;
            } else if (columnNames.contains(column.getName())) {
                String msg = "Column '%s' - Duplicate column names is prohibited";
                msg = String.format(msg, column.getName());
                mart.addIssue(msg);
            } else {
                columnNames.add(column.getName());
                mart.addMartColumn(column);
            }
        }

        for (MartDSLParser.ConfigParamContext cfgParamCtx : unitContext.configuration().configParam()) {
            if(cfgParamCtx.triggerConfig() != null)
                parseTrigger(cfgParamCtx.triggerConfig());
            if(cfgParamCtx.typeConfig() != null)
                parseType(cfgParamCtx.typeConfig());
        }

        if(mart.getMartType().equals(MartType.MERGED) || mart.getMartType().equals(MartType.HISTORICAL)){
            for (MartDSLParser.MartColumnContext colCtx : unitContext.mapping().martColumn()) {
                final MartColumn column = new MartColumnParser(session, mart).visitMartColumn(colCtx);
                if (column.isKeyColumn()) {
                    kcColumnCount++;
                }
            }
            if(kcColumnCount == 0){
                String msg = "Mart '%s' with Type: '%s' - Should have one or more Key columns";
                msg = String.format(msg, mart.getName(), mart.getMartType());
                mart.addIssue(msg);
            }
        }

        final String customSqlCode = StringUtility.trimAndRemoveQuotes(unitContext.sql().code.getText());
        mart.setCustomSqlCode(customSqlCode);

        return mart;
    }


    private void parseType(MartDSLParser.TypeConfigContext typeConfigContext) {
        switch (typeConfigContext.type.getText()) {
            case "merged" : mart.setMartType(MartType.MERGED); break;
            case "historical" : mart.setMartType(MartType.HISTORICAL); break;
            case "load" : mart.setMartType(MartType.LOAD); break;
            case "reload" : mart.setMartType(MartType.RELOAD); break;
            default: throw new IllegalArgumentException();
        }
    }

    /**
     * ANTLR Grammar:
     * <pre>
     *     'trigger' ':' ( defaultTrigger | cronTrigger | mousetrapTrigger )
     * </pre>
     * @param ctx
     */
    private void parseTrigger(MartDSLParser.TriggerConfigContext ctx) {
        if(ctx == null) return;
        new TriggerConfigurationParser(mart, session).parseTrigger(ctx);
    }

}