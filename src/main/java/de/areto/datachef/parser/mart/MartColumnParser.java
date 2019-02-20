package de.areto.datachef.parser.mart;

import de.areto.common.util.StringUtility;
import de.areto.datachef.config.MartConfig;
import de.areto.datachef.model.datavault.DataDomain;
import de.areto.datachef.model.mart.Mart;
import de.areto.datachef.model.mart.MartColumn;
import de.areto.datachef.parser.antlr4.MartDSLBaseVisitor;
import de.areto.datachef.parser.antlr4.MartDSLParser;
import lombok.NonNull;
import org.aeonbits.owner.ConfigCache;
import org.hibernate.Session;

import java.util.Optional;

public class MartColumnParser extends MartDSLBaseVisitor<MartColumn> {

    private final MartConfig martConfig = ConfigCache.getOrCreate(MartConfig.class);
    private final Session session;
    private final Mart mart;

    private final MartColumn column = new MartColumn();

    public MartColumnParser(@NonNull Session session, @NonNull Mart mart) {
        this.session = session;
        this.mart = mart;
    }

    @Override
    public MartColumn visitMartColumn(MartDSLParser.MartColumnContext columnContext) {
        column.setName(columnContext.name.getText());

        for (MartDSLParser.ColumnParamContext pCtx : columnContext.columnParam()) {
            if(pCtx.keyParam() != null) {
                column.setKeyColumn(true);
            }

            if(pCtx.identityParam() != null) {
                column.setIdentityColumn(true);
            }
            if(pCtx.ddParam() != null) {
                final String ddName = pCtx.ddParam().value.getText();
                final Optional<DataDomain> dataDomain = session.byId(DataDomain.class).loadOptional(ddName);
                if(!dataDomain.isPresent()) {
                    final String msg = String.format("Column '%s' - Data Domain '%s' not found",
                            column.getName(), ddName);
                    mart.addIssue(msg);
                } else {
                    column.setDataDomain(dataDomain.get());
                }
            }
            if(pCtx.cmntParam() != null) {
                final String comment = StringUtility.trimAndRemoveQuotes(pCtx.cmntParam().value.getText());
                column.setComment(comment);
            }
        }

        if(column.isIdentityColumn()) {
            final String ddName = martConfig.idColumnDataDomain();
            final Optional<DataDomain> dataDomain = session.byId(DataDomain.class).loadOptional(ddName);
            if(!dataDomain.isPresent()) {
                final String msg = String.format("Column '%s' - Data Domain '%s' not found",
                        column.getName(), ddName);
                mart.addIssue(msg);
            } else {
                column.setDataDomain(dataDomain.get());
            }
        }

        validateColumn();

        return column;
    }

    private void validateColumn() {
        if(column.isKeyColumn() && column.isIdentityColumn()) {
            final String msg = String.format("Column '%s' - combination of 'kc' and 'id' is prohibited",
                    column.getName());
            mart.addIssue(msg);
        }
        if(!column.isIdentityColumn() && column.getDataDomain() == null) {
            final String msg = String.format("Column '%s' - 'dd' has to be specified",
                    column.getName());
            mart.addIssue(msg);
        }
    }
}
