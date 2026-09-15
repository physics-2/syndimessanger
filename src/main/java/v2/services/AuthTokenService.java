package v2.services;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;

/**
 * Минимальная авторизация по ключу — БЕЗ аккаунтов, регистраций и таблиц в БД.
 *
 * Как работает:
 *  - в application.properties задаётся app.auth.key — секретный ключ доступа;
 *  - пользователь вводит его на экране входа, POST /api/v2/auth/login выдаёт токен;
 *  - токен = base64url(дата_истечения) + "." + base64url(HMAC-SHA256(дата_истечения, secret));
 *  - токен stateless: сервер ничего не хранит, он просто проверяет подпись и срок;
 *  - «отозвать все токены» = поменять app.auth.secret (или key) и перезапустить —
 *    все выданные ранее токены мгновенно перестанут проходить проверку.
 *
 * Если app.auth.key пустой — авторизация ВЫКЛЮЧЕНА: enabled()=false, фильтр
 * пропускает всё, приложение работает как раньше (удобно для локальной разработки).
 */
@Component
public class AuthTokenService {

    /** Ключ доступа — то, что вводит пользователь на фронте. */
    private final String key;
    /** Секрет подписи токенов. Если не задан отдельно — используется ключ. */
    private final byte[] secret;
    private final long ttlMillis;

    public AuthTokenService(
            @Value("${app.auth.key:}") String key,
            @Value("${app.auth.secret:}") String secret,
            @Value("${app.auth.ttlDays:30}") long ttlDays) {
        this.key = key == null ? "" : key.trim();
        String sec = (secret == null || secret.isBlank()) ? this.key : secret.trim();
        this.secret = sec.getBytes(StandardCharsets.UTF_8);
        this.ttlMillis = Math.max(1, ttlDays) * 24L * 3600L * 1000L;
    }

    /** Включена ли защита: нет ключа — нет и авторизации. */
    public boolean enabled() {
        return !key.isEmpty();
    }

    public long ttlMillis() {
        return ttlMillis;
    }

    /** Сравнение ключа без утечки по времени (constant-time). */
    public boolean checkKey(String input) {
        if (!enabled()) {
            return true;
        }
        if (input == null) {
            return false;
        }
        return MessageDigest.isEqual(
                input.trim().getBytes(StandardCharsets.UTF_8),
                key.getBytes(StandardCharsets.UTF_8));
    }

    /** Выпустить токен, действительный ttlDays суток. */
    public String issue() {
        String payload = Long.toString(System.currentTimeMillis() + ttlMillis);
        return b64(payload.getBytes(StandardCharsets.UTF_8)) + "." + b64(hmac(payload));
    }

    /** Проверить подпись и срок действия токена. При выключенной авторизации — всегда true. */
    public boolean verify(String token) {
        if (!enabled()) {
            return true;
        }
        if (token == null || token.isBlank()) {
            return false;
        }
        String[] parts = token.trim().split("\\.");
        if (parts.length != 2) {
            return false;
        }
        try {
            byte[] payloadBytes = Base64.getUrlDecoder().decode(parts[0]);
            byte[] signature = Base64.getUrlDecoder().decode(parts[1]);
            String payload = new String(payloadBytes, StandardCharsets.UTF_8);
            long expiresAt = Long.parseLong(payload);
            if (expiresAt < System.currentTimeMillis()) {
                return false;                                   // токен истёк
            }
            return MessageDigest.isEqual(hmac(payload), signature);   // подпись верна?
        } catch (Exception e) {
            return false;
        }
    }

    private byte[] hmac(String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret, "HmacSHA256"));
            return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new IllegalStateException("HmacSHA256 недоступен", e);
        }
    }

    private static String b64(byte[] data) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(data);
    }
}