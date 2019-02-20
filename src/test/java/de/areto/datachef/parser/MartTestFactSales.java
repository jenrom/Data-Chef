package de.areto.datachef.parser;

import de.areto.common.template.SQLTemplateRenderer;
import de.areto.datachef.Setup;
import de.areto.datachef.config.DWHConfig;
import de.areto.datachef.config.DataVaultConfig;
import de.areto.datachef.config.TemplateConfig;
import de.areto.datachef.creator.expression.ddl.CreateTableExpressionCreator;
import de.areto.datachef.creator.table.MartTableCreator;
import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.exceptions.RenderingException;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.jdbc.DBTable;
import de.areto.datachef.model.mart.Mart;
import de.areto.datachef.model.mart.MartColumn;
import de.areto.datachef.model.sink.SinkFile;
import de.areto.datachef.model.template.PrimaryKey;
import de.areto.datachef.persistence.HibernateUtility;
import org.aeonbits.owner.ConfigCache;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static de.areto.test.TestUtility.getSinkFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;

public class MartTestFactSales {

    @BeforeClass
    public static void setup() throws Exception {
        Setup.setup();
    }

    @Test
    public void scriptShouldBeParsed() throws Exception {
        final SinkFile martFile = getSinkFileFromResource("/test_sink/fact_sales.mart");

        try (Session session = HibernateUtility.getSessionFactory().openSession()) {
            final Transaction transaction = session.beginTransaction();
            final String martName = martFile.getMappingName();
            MartScriptParser martScriptParser = new MartScriptParser(martName, session);

            final Mart mart = martScriptParser.parse(martFile.getContentString());
            session.persist(mart);
            mart.getIssueList().forEach(System.err::println);

            if(mart.isValid())
                transaction.commit();
            else
                transaction.rollback();

            MartTableCreator martTableCreator = new MartTableCreator(mart);
            final DBTable martTable = martTableCreator.createDbTable();

            assertThat(martTable).isNotNull();
            assertThat(martTable.getColumns()).hasSize(6);

            System.out.println(new CreateTableExpressionCreator(martTable).createExpression().getSqlCode());

            final String schemaName = martTable.getSchema();
            final String tableName = martTable.getName();
            final String pkName = ConfigCache.getOrCreate(DWHConfig.class).primaryKeyPrefix() + tableName;
            final PrimaryKey primaryKey = new PrimaryKey(schemaName, tableName, pkName);

            for(MartColumn martColumn : mart.getColumns()){
                if(martColumn.isIdentityColumn() || martColumn.isKeyColumn())
                    primaryKey.addColumn(martColumn.getName());
            }

            final Map<String, Object> context = new HashMap<>();
            context.put("primaryKey", primaryKey);
            final String desc = String.format("Primary Key for Mart %s",  mart.getName());
            final String template = ConfigCache.getOrCreate(TemplateConfig.class).createPrimaryKeyConstraintTemplate();
            final String sql = new SQLTemplateRenderer(template).render(context);
            System.out.println(sql);

        }

    }
}
