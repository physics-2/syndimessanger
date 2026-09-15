package v2.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import v2.entity.Config;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * ЗАМЕНЯЕТ v2/repository/ConfigRepository.java целиком.
 *
 * Что исправлено:
 *
 * 1. get() больше НЕ падает с NoSuchElementException("No value present").
 *    Раньше: findById(1L).get() — на пустой таблице config это исключение, и именно
 *    оно возвращалось фронту как {"success":false,"message":"Ошибка обновления конфига:
 *    No value present"} при POST /api/tg/updateConfig. TG — единственный коннектор,
 *    чей updateConfig() ходит в этот репозиторий, поэтому VK работал, а TG нет.
 *    Теперь отсутствующая строка-сингл (id=1) создаётся на месте.
 *
 * 2. Это же чинит пустые tg_ids: TgConnector.initializeUserConfig() вызывается в
 *    getMeAsync().whenComplete(...) и тоже начинается с get(); раньше она падала,
 *    а исключение молча проглатывалось CompletableFuture — строка config не создавалась
 *    никогда. Теперь после авторизации TG строка появится и tg_ids заполнятся.
 *
 * 3. В saveOrUpdate() убран дублирующий БЕЗУСЛОВНЫЙ found.setVk_ids(config.getVk_ids())
 *    (он стоял сразу после守卫 if(!isEmpty) и перезатирал vk_ids пустым списком,
 *    обнуляя смысл защиты). Заодно проверки сделаны null-безопасными: раньше
 *    config.getTg_ids().isEmpty() дало бы NPE, если список пришёл как null.
 */
@Repository
public interface ConfigRepository extends JpaRepository<Config, Long> {

    Optional<Config> findById(Long id);

    /**
     * Конфиг-сингл (id = 1). Если строки ещё нет — создаёт пустую и сохраняет,
     * так что вызывающий код всегда получает готовый объект.
     */
    default Config get() {
        return findById(1L).orElseGet(() -> {
            Config c = new Config(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
            c.setId(1L);
            return save(c);
        });
    }

    default Config saveOrUpdate(Config config) {
        Optional<Config> existing = findById(1L);
        if (existing.isPresent()) {
            Config found = existing.get();
            found.setId(1L);
            // непустые списки перезаписываем, пустые/null — не трогаем существующие значения
            if (nonEmpty(config.getTg_ids()))  found.setTg_ids(config.getTg_ids());
            if (nonEmpty(config.getMax_ids())) found.setMax_ids(config.getMax_ids());
            if (nonEmpty(config.getVk_ids()))  found.setVk_ids(config.getVk_ids());
            return save(found);
        }
        config.setId(1L);
        return save(config);
    }

    private static boolean nonEmpty(List<String> ids) {
        return ids != null && !ids.isEmpty();
    }
}