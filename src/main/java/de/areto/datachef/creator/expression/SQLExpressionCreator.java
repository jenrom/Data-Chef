package de.areto.datachef.creator.expression;

import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.model.compilation.SQLExpression;

public interface SQLExpressionCreator {

    SQLExpression createExpression() throws CreatorException;

}
