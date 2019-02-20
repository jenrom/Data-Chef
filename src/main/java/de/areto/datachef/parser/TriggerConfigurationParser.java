package de.areto.datachef.parser;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import de.areto.common.util.StringUtility;
import de.areto.datachef.model.compilation.CompilationUnit;
import de.areto.datachef.parser.antlr4.MartDSLParser;
import de.areto.datachef.parser.antlr4.SinkDSLParser;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.hibernate.Session;

import java.util.Optional;
import java.util.stream.Collectors;

public class TriggerConfigurationParser {
    
    private final CompilationUnit compilationUnit;
    private final Session session;

    public TriggerConfigurationParser(CompilationUnit compilationUnit, Session session) {
        this.compilationUnit = compilationUnit;
        this.session = session;
    }

    public void parseTrigger(SinkDSLParser.TriggerConfigContext ctx) {
        if(ctx == null) return;

        TriggerConfiguration sinkTriggerConfig = new TriggerConfiguration();
        if(ctx.cronTrigger() != null) {
            sinkTriggerConfig.setCronExpression(ctx.cronTrigger().cron.getText());
            sinkTriggerConfig.setCronTrigger(true);
            parseCronTrigger(sinkTriggerConfig);
        } else {
            // Mousetrap
            sinkTriggerConfig.setDependencies(ctx.mousetrapTrigger().ID()
                    .stream()
                    .map(TerminalNode::getText)
                    .collect(Collectors.toSet()));
            sinkTriggerConfig.setCronTrigger(false);
            sinkTriggerConfig.setTimeout(Integer.parseInt(ctx.mousetrapTrigger().timeout.getText()));
            sinkTriggerConfig.setTimeoutUnit(ctx.mousetrapTrigger().unit.getText());
            parseMousetrapTrigger(sinkTriggerConfig);
        }
    }

    public void parseTrigger(MartDSLParser.TriggerConfigContext ctx) {
        if(ctx == null) return;
        TriggerConfiguration martTriggerConfig = new TriggerConfiguration();
        if(ctx.cronTrigger() != null) {
            martTriggerConfig.setCronExpression(ctx.cronTrigger().cron.getText());
            martTriggerConfig.setCronTrigger(true);
            parseCronTrigger(martTriggerConfig);
        } else {
            // Mousetrap
            martTriggerConfig.setDependencies(ctx.mousetrapTrigger().ID()
                    .stream()
                    .map(TerminalNode::getText)
                    .collect(Collectors.toSet()));
            martTriggerConfig.setCronTrigger(false);
            martTriggerConfig.setTimeout(Integer.parseInt(ctx.mousetrapTrigger().timeout.getText()));
            martTriggerConfig.setTimeoutUnit(ctx.mousetrapTrigger().unit.getText());
            parseMousetrapTrigger(martTriggerConfig);
        }
    }

    private void parseMousetrapTrigger(TriggerConfiguration triggerConfiguration) {
        compilationUnit.setTriggeredByMousetrap(true);

        if(triggerConfiguration.getDependencies().isEmpty()) {
            compilationUnit.addIssue("Please provide a list of dependent names of Compilation Units");
        }

        compilationUnit.setMousetrapTimeout(triggerConfiguration.getTimeout());

        if(compilationUnit.getMousetrapTimeout() <= 0) {
            compilationUnit.addIssue("Mousetrap timeout has to be > 0");
        }

        compilationUnit.setTimeoutUnit(triggerConfiguration.getTimeoutUnit());

        for(String unitName : triggerConfiguration.getDependencies()) {
            final Optional<CompilationUnit> depUnit = session.byNaturalId(CompilationUnit.class)
                    .using("name", unitName)
                    .loadOptional();

            if(!depUnit.isPresent()) {
                final String msg = String.format("Referenced dependent Compilation Unit '%s' does not exist", unitName);
                compilationUnit.addIssue(msg);
            } else {
                compilationUnit.addDependency(unitName);
            }
        }
    }

    private void parseCronTrigger(TriggerConfiguration triggerConfiguration) {
        final String cronExpression = StringUtility.trimAndRemoveQuotes(triggerConfiguration.getCronExpression());

        try {
            final CronDefinition cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ);
            new CronParser(cronDefinition).parse(cronExpression).validate();

            compilationUnit.setCronExpression(cronExpression);
            compilationUnit.setTriggeredByCron(true);
        } catch (IllegalArgumentException e) {
            final String msg = String.format("CRON trigger expression '%s' (QUARTZ) is invalid", cronExpression);
            compilationUnit.addIssue(msg);
        }
    }
}
