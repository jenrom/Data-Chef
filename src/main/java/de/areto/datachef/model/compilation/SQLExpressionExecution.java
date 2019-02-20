package de.areto.datachef.model.compilation;

import de.areto.datachef.model.common.DefaultPrimaryKeyEntity;
import de.areto.datachef.model.worker.WorkerCargo;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Entity;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.Transient;
import java.time.LocalDateTime;

@Entity
@Getter
@Setter
@NoArgsConstructor
public class SQLExpressionExecution extends DefaultPrimaryKeyEntity {

    @ManyToOne
    private SQLExpression expression;

    @ManyToOne
    private WorkerCargo cargo;

    private LocalDateTime executedTime;

    private long runtime;

    private long updateCount = -1;

    private boolean errorFlag;

    private int stmntOrder;

    @Lob
    private String errorMessage;

    @Transient
    private String customSqlCode;
}
