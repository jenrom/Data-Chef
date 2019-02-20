package de.areto.datachef.web_app.handler;

import de.areto.datachef.web_app.common.RouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import spark.Request;
import spark.Response;

import static de.areto.datachef.config.Constants.USER_SESSION_ATTRIBUTE_NAME;

@WebRoute(
        path = "/logout"
)
public class LogoutHandler extends RouteHandler {

    @Override
    public Object doWork(Request request, Response response) {
        request.session().removeAttribute(USER_SESSION_ATTRIBUTE_NAME);
        response.redirect("/login");
        return null;
    }
}
