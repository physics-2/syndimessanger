package v2.auth;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import v2.services.AuthTokenService;

import java.io.IOException;

/**
 * Проверяет Authorization: Bearer <токен> на ВСЕХ /api/**-запросах.
 *
 * Пропускаются без токена:
 *  - OPTIONS               — CORS preflight (браузер не прикладывает к нему заголовки);
 *  - POST /api/v2/auth/login — сам вход (иначе ключ не обменять на токен);
 *  - всё остальное, если app.auth.key не задан (enabled() == false) — режим разработки.
 *
 * GET /api/v2/auth/check СОЗНАТЕЛЬНО не исключён: без действительного токена он
 * отвечает 401, и фронт по этому 401 показывает экран входа. Старый фронт со старым
 * бэкендом (где нет AuthController) получит 404 и продолжит работать как ни в чём не бывало.
 *
 * Статика (messenger.html из src/main/resources/static) НЕ защищается — страница
 * загрузится, но без токена не получит ни одного ответа от API и сразу покажет
 * экран входа. Если хочется закрыть и сам HTML — добавьте его путь в условие ниже.
 */
@Component
@Order(1)   // до остальных фильтров (в т.ч. до CORS-обработки, чтобы 401 не перехватывался)
public class AuthFilter extends OncePerRequestFilter {

    private final AuthTokenService tokens;

    public AuthFilter(AuthTokenService tokens) {
        this.tokens = tokens;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse resp, FilterChain chain)
            throws ServletException, IOException {

        String uri = req.getRequestURI();
        boolean isApi = uri.startsWith("/api/");
        boolean isLogin = uri.endsWith("/api/v2/auth/login");
        boolean isPreflight = "OPTIONS".equalsIgnoreCase(req.getMethod());

        if (!isApi || isLogin || isPreflight || !tokens.enabled()) {
            chain.doFilter(req, resp);
            return;
        }

        String header = req.getHeader("Authorization");
        String token = (header != null && header.regionMatches(true, 0, "Bearer ", 0, 7))
                ? header.substring(7).trim()
                : null;

        if (tokens.verify(token)) {
            chain.doFilter(req, resp);
            return;
        }

        resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        resp.setContentType("application/json;charset=UTF-8");
        resp.getWriter().write("{\"success\":false,\"message\":\"Требуется авторизация: токен отсутствует или устарел\"}");
    }
}