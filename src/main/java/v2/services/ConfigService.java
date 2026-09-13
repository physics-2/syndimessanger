package v2.services;

import org.springframework.stereotype.Service;
import v2.entity.Config;
import v2.repository.ConfigRepository;

@Service
public class ConfigService {
    private final ConfigRepository configRepository;

    public ConfigService(ConfigRepository configRepository) {
        this.configRepository = configRepository;
    }

    public void saveOrUpdateConfig(Config config){
        configRepository.saveOrUpdate(config);
    }

    public Config getConfig() {
        // Возвращаем конфиг с ID=1 или бросаем исключение, если его нет
        return configRepository.findById(1L).orElseThrow();
    }
}