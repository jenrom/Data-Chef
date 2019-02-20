package de.areto.datachef.creator.expression;

import de.areto.datachef.exceptions.CreatorException;
import de.areto.datachef.model.compilation.SQLExpression;

import java.util.Queue;

public interface SQLExpressionQueueCreator {

    Queue<SQLExpression> createExpressionQueue() throws CreatorException;

}
