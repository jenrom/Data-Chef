package de.areto.datachef.parser.mapping;

import de.areto.common.util.StringUtility;
import de.areto.datachef.model.mapping.MappingColumn;
import de.areto.datachef.parser.antlr4.SinkDSLBaseVisitor;
import de.areto.datachef.parser.antlr4.SinkDSLParser;
import org.apache.commons.text.StringEscapeUtils;

public class MappingColumnParser extends SinkDSLBaseVisitor<MappingColumn> {

    private final MappingColumn column = new MappingColumn();

    @Override
    public MappingColumn visitMappingColumn(SinkDSLParser.MappingColumnContext ctx) {
        column.setName(ctx.name.getText());

        for (SinkDSLParser.ColumnParamContext param : ctx.columnParam()) {
            if (param.keyAndId() != null)
                addProperty(param.keyAndId());
            if (param.keyAndString() != null)
                addProperty(param.keyAndString());
            if (param.singleKey() != null)
                addProperty(param.singleKey());
        }

        return column;
    }

    private void addProperty(SinkDSLParser.SingleKeyContext ctx) {
        switch (ctx.key.getText()) {
            case "kc":
                column.setKeyColumn(true);
                break;
            case "ign":
                column.setIgnored(true);
                break;
            case "ls":
                column.setPartOfLinkSatellite(true);
                break;
        }
    }

    private void addProperty(SinkDSLParser.KeyAndStringContext ctx) {
        final String value = StringUtility.trimAndRemoveQuotes(ctx.value.getText());

        switch (ctx.key.getText()) {
            case "cmnt":
                column.setComment(value);
                break;
            case "ocmnt":
                column.setObjectComment(value);
                break;
            case "clc":
                column.setCalculation(StringEscapeUtils.unescapeJava(value));
                break;
        }
    }

    private void addProperty(SinkDSLParser.KeyAndIdContext ctx) {
        final String value = ctx.value.getText();

        switch (ctx.key.getText()) {
            case "on":
                column.setObjectName(value);
                break;
            case "oa":
                column.setObjectAlias(value);
                break;
            case "dd":
                column.setDataDomainName(value);
                break;
            case "sn":
                column.setSatelliteName(value);
                break;
            case "ls":
                column.setPartOfLinkSatellite(true);
                column.setLinkSatelliteName(value);
                break;
            case "rn":
                column.setNewName(value);
                break;
            case "rl":
                column.setRoleName(value);
                break;
        }
    }
}