package v2.connectors.base;

import java.util.List;

public record ScanOptions(
        List<Long> chatIds,      // null = все разрешённые
        Integer limitPerChat     // null = взять из конфига
) {}