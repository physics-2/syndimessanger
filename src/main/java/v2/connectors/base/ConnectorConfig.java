package v2.connectors.base;

import java.util.ArrayList;
import java.util.List;

public class ConnectorConfig {
    public boolean scanPersonal = true;
    public boolean scanGroups   = true;
    public List<Long> whitelist = new ArrayList<>(); // пусто = всё разрешено
    public int limitPerChat     = 200;
    public boolean downloadMedia = false;

    // getters/setters
}