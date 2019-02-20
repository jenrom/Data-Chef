package de.areto.datachef.model.compilation;

import de.areto.datachef.model.common.IdentifiableEntity;
import de.areto.datachef.scheduler.CronUtil;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;

import javax.persistence.*;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Entity
@Getter
@Setter
@NoArgsConstructor
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@DiscriminatorColumn(name = "unitType")
public abstract class CompilationUnit extends IdentifiableEntity {

    private final static Predicate<SQLExpression> DDL_EXPR = (e) -> e.getExpressionType().equals(SQLExpression.ExpressionType.DDL);
    private final static Predicate<SQLExpression> DML_EXPR = (e) -> e.getExpressionType().equals(SQLExpression.ExpressionType.DML);

    @Lob
    private String scriptCode;

    @Lob
    private String customSqlCode;

    @Column(insertable = false, updatable = false)
    private String unitType;

    private boolean triggeredByCron;

    private String cronExpression;

    private boolean triggeredByMousetrap;

    private int mousetrapTimeout;

    private String timeoutUnit;

    @ElementCollection
    private Set<String> dependencyList = new HashSet<>();

    public CompilationUnit(@NonNull String name) {
        this.setName(name);
    }

    @Transient
    private List<String> issueList = new ArrayList<>();

    @OneToMany(mappedBy = "mapping", cascade = CascadeType.ALL, orphanRemoval = true)
    private Set<SQLExpression> expressions = new HashSet<>();

    public void addIssue(String problem) {
        this.issueList.add(problem);
    }

    public boolean isValid() {
        return issueList.isEmpty();
    }

    public boolean hasCustomSql() {
        return customSqlCode != null && !customSqlCode.isEmpty();
    }

    public String describeCronExpression() {
        return isTriggeredByCron() ? CronUtil.describeCronExpression(cronExpression) : null;
    }

    public boolean isCronExpressionValid() {
        if(cronExpression == null)
            return false;
        if(cronExpression.isEmpty())
            return false;

        try {
            CronUtil.getParser().parse(cronExpression).validate();
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    public void addDependency(@NonNull String mappingName) {
        this.dependencyList.add(mappingName);
    }

    public void setExpressions(Set<SQLExpression> expressionReferences) {
        this.expressions.clear();
        this.expressions.addAll(expressionReferences);
    }

    public void addDefinitionExpression(@NonNull SQLExpression expression, int orderNumber) {
        expression.setMapping(this);
        expression.setOrderNumber(orderNumber);
        expression.setExpressionType(SQLExpression.ExpressionType.DDL);
        this.expressions.add(expression);
    }

    public void addManipulationExpression(@NonNull SQLExpression expression, int orderNumber) {
        expression.setMapping(this);
        expression.setOrderNumber(orderNumber);
        expression.setExpressionType(SQLExpression.ExpressionType.DML);
        this.expressions.add(expression);
    }

    public Collection<SQLExpression> getDefinitionExpressionsSorted() {
        return this.expressions.stream()
                .filter(DDL_EXPR)
                .sorted(Comparator.comparing(SQLExpression::getOrderNumber))
                .collect(Collectors.toList());
    }

    public Collection<SQLExpression> getManipulationExpressionsSorted() {
        return this.expressions.stream()
                .filter(DML_EXPR)
                .sorted(Comparator.comparing(SQLExpression::getOrderNumber))
                .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "CompilationUnit{" + getName() + (isTransient() ? "transient" : "" ) + '}';
    }
}