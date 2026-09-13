package v2.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import v2.entity.Config;

import java.util.Optional;

@Repository
public interface ConfigRepository extends JpaRepository<Config, Long> {

    Optional<Config> findById(Long id);

    default Config get(){
        return findById(1L).get();
    }

    default Config saveOrUpdate(Config config){
        // Предполагаем, что конфиг у нас один (синглтон) и его ID = 1
        Optional<Config> existing = findById(1L);
        if (existing.isPresent()) {
            Config found = existing.get();
            found.setId(1L);
            found.setMax_ids(config.getMax_ids());
            found.setTg_ids(config.getTg_ids());
            found.setVk_ids(config.getVk_ids());
            return save(found);
        }
        // ИСПРАВЛЕНО: Раньше здесь было save(existing.get()), что вызывало падение.
        config.setId(1L);
        return save(config);
    }
}