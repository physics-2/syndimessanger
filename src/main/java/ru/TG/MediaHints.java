package ru.TG;

/**
 * Подсказки о файле, которые знает клиент (браузер), но не знает сервер.
 *
 * Зачем: для видео Telegram хочет знать duration и width/height — иначе в сообщении
 * будет «0:00» и отсутствие превью. Браузер измеряет это сам
 * (video.onloadedmetadata / Image.naturalWidth) и присылает вместе с файлом.
 *
 * Все поля необязательны: null/0 = «не известно, определи сам».
 *
 * @param kind        принудительный тип контента: AUTO / PHOTO / VIDEO / DOCUMENT
 * @param durationSec длительность в секундах (для видео)
 * @param width       ширина в пикселях (видео/фото)
 * @param height      высота в пикселях (видео/фото)
 * @param mimeType    MIME из браузера (часто application/octet-stream — тогда решают байты)
 * @param fileName    настоящее имя файла (на диске он может лежать под временным именем)
 */
public record MediaHints(
        MediaKind kind,
        Integer durationSec,
        Integer width,
        Integer height,
        String mimeType,
        String fileName
) {
    public static MediaHints none() {
        return new MediaHints(MediaKind.AUTO, null, null, null, null, null);
    }

    public static MediaHints ofKind(MediaKind kind) {
        return new MediaHints(kind, null, null, null, null, null);
    }

    public static MediaHints ofKind(String kind) {
        return new MediaHints(MediaKind.parse(kind), null, null, null, null, null);
    }

    public MediaKind kindOrAuto() {
        return kind != null ? kind : MediaKind.AUTO;
    }

    public int durationOrZero() {
        return durationSec != null && durationSec > 0 ? durationSec : 0;
    }

    public int widthOrZero() {
        return width != null && width > 0 ? width : 0;
    }

    public int heightOrZero() {
        return height != null && height > 0 ? height : 0;
    }
}