package de.areto.datachef.web_app.handler;

import de.areto.datachef.creator.DMLQueueCreator;
import de.areto.datachef.creator.ViewQueueCreator;
import de.areto.datachef.exceptions.WebException;
import de.areto.datachef.model.compilation.SQLExpression;
import de.areto.datachef.model.mapping.Mapping;
import de.areto.datachef.persistence.HibernateUtility;
import de.areto.datachef.web_app.common.RouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import org.hibernate.Session;
import spark.Request;
import spark.Response;

import java.util.Queue;

@WebRoute(
        path = "/mapping/regenerate/:mappingName"
)
public class MappingRegenerateHandler extends RouteHandler {
    @Override
    public Object doWork(Request request, Response response) throws Exception {
        try(Session session = HibernateUtility.getSessionFactory().openSession()) {
            final String mappingName = request.params("mappingName");
            response.type("application/sql");
            final String contentDisposition = String.format("attachment; filename=\"%s.sql\"", mappingName);
            response.header("Content-Disposition", contentDisposition);

            final String eMsg = String.format("Mapping '%s' not found", mappingName);

            final Mapping mapping = session.byNaturalId(Mapping.class).using("name", mappingName)
                    .loadOptional()
                    .orElseThrow(() -> new WebException(eMsg));

            final StringBuilder builder = new StringBuilder();
            builder.append("-- DML and View SQLs for Mapping ").append(mappingName).append("\r\n");
            renderExpressionQueue(builder, new DMLQueueCreator(mapping).createExpressionQueue(), "DML");
            renderExpressionQueue(builder, new ViewQueueCreator(mapping, session).createExpressionQueue(), "View");

            return builder.toString();
        }
    }

    private void renderExpressionQueue(StringBuilder builder, Queue<SQLExpression> queue, String type) {
        builder.append("\r\n\r\n-- ").append(type).append(" ---------------------\r\n\n");

        int i = 1;
        for (SQLExpression expression : queue) {
            builder.append("-- ").append(type).append(" #").append(i).append(": ")
                    .append(expression.getDescription()).append("\r\n");
            builder.append(expression.getSqlCode()).append(";\r\n\r\n");
            i++;
        }
    }
}
