package de.areto.datachef.web_app.handler;

import de.areto.datachef.model.web.User;
import de.areto.datachef.persistence.HibernateUtility;
import de.areto.datachef.web_app.common.TemplateRouteHandler;
import de.areto.datachef.web_app.common.WebRoute;
import org.apache.commons.codec.digest.DigestUtils;
import org.hibernate.Session;
import spark.Request;
import spark.Response;
import spark.route.HttpMethod;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static de.areto.datachef.config.Constants.USER_SESSION_ATTRIBUTE_NAME;

@WebRoute(
        path = "/login",
        requestType = HttpMethod.post,
        template = "login.vm"
)
public class LoginCallbackHandler extends TemplateRouteHandler {

    @Override
    public Map<String, Object> createContext(Request request, Response response) {
        final String inputUsername = request.queryParams("username");
        final String inputPassword = request.queryParams("password");
        final Map<String, Object> context = Collections.singletonMap("errorMessage", "Credentials incorrect");

        if (inputUsername == null || inputUsername.isEmpty() || inputPassword == null || inputPassword.isEmpty()) {
            return context;
        }

        final String passwordHash = DigestUtils.md5Hex(inputPassword);

        try(Session session = HibernateUtility.getSessionFactory().openSession()) {
            Optional<User> user = session.byId(User.class).loadOptional(inputUsername);

            if(!user.isPresent() || !user.get().getPasswordHash().equals(passwordHash)) {
                return context;
            } else {
                request.session().attribute(USER_SESSION_ATTRIBUTE_NAME, user);
            }
        }

        String targetPath = "/";
        if (request.session().attribute("loginRedirect") != null) {
            targetPath = request.session().attribute("loginRedirect");
        }

        response.redirect(targetPath);
        return Collections.emptyMap();
    }
}
