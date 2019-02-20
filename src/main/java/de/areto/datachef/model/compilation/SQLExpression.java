package de.areto.datachef.model.compilation;

import de.areto.datachef.model.common.DefaultPrimaryKeyEntity;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;

@Entity
@Getter
@Setter
@NoArgsConstructor
public class SQLExpression extends DefaultPrimaryKeyEntity {

    public enum ExpressionType {
        DDL,
        DML
    }

    public enum QueryType {
        POPULATE,
        CREATE,
        RAW_2_COOKED,
        IMPORT_FILE,
        IMPORT_CONNECTION,
        IMPORT_INSERT,
        DELETE,
        VIEW,
        CONSTRAINT,
        TRUNCATE,
        DROP
    }

    @ManyToOne
    private CompilationUnit mapping;

    @Enumerated(EnumType.STRING)
    private QueryType queryType;

    @Enumerated(EnumType.STRING)
    private ExpressionType expressionType;

    @Lob
    private String sqlCode;

    private String description;

    private int orderNumber;

    public SQLExpression(QueryType queryType, String sqlCode, String description) {
        this.queryType = queryType;
        this.sqlCode = sqlCode;
        this.description = description;
    }
}