package v2.api;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import v2.services.AuthTokenService;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Точки авторизации. Единственные /api/**-пути, куда фильтр пускает без токена:
 *   POST /api/v2/auth/login — обмен ключа на токен;
 *   GET  /api/v2/auth/check — проверка токена при старте фронта (старый бэкенд без
 *                             этого контроллера отвечает 404, и фронт понимает,
 *                             что авторизация не включена — совместимость сохранена).
 */
@RestController
@RequestMapping("/api/v2/auth")
public class AuthController {

    private final AuthTokenService tokens;

    public AuthController(AuthTokenService tokens) {
        this.tokens = tokens;
    }

    /**
     * POST /api/v2/auth/login   тело: {"key":"..."}   (или {"password":"..."})
     * Успех: {"success":true,"token":"...","expiresAt":1750000000000}
     * Провал: {"success":false,"message":"Неверный ключ доступа"} (HTTP 200 — так проще фронту)
     */
    @PostMapping("/login")
    public Map<String, Object> login(@RequestBody(required = false) Map<String, String> body) {
        Map<String, Object> out = new LinkedHashMap<>();

        String input = body == null ? null : body.getOrDefault("key", body.get("password"));

        if (!tokens.enabled()) {
            // ключ на сервере не задан — авторизация выключена, токен выдаём «для единообразия»
            out.put("success", true);
            out.put("token", tokens.issue());
            out.put("expiresAt", System.currentTimeMillis() + tokens.ttlMillis());
            out.put("message", "Авторизация не настроена (app.auth.key пуст) — доступ свободный");
            return out;
        }

        if (!tokens.checkKey(input)) {
            // микро-пауза: замедляет перебор ключа, не влияя на обычного пользователя
            try { Thread.sleep(400); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            out.put("success", false);
            out.put("message", "Неверный ключ доступа");
            return out;
        }

        out.put("success", true);
        out.put("token", tokens.issue());
        out.put("expiresAt", System.currentTimeMillis() + tokens.ttlMillis());
        return out;
    }

    /**
     * GET /api/v2/auth/check — досюда доходят только запросы, прошедшие фильтр,
     * то есть с действительным токеном (или при выключенной авторизации).
     * Фронт дёргает его при загрузке, чтобы решить: пускать сразу или показать экран входа.
     */
    @GetMapping("/check")
    public Map<String, Object> check() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("success", true);
        out.put("authEnabled", tokens.enabled());
        return out;
    }
}