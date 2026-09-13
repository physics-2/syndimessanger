package ru.TG;

/**
 * Каким типом контента отправить файл в Telegram.
 *
 * Осознанно оставили только три рабочих варианта — всё остальное уходит файлом:
 *   PHOTO    -> InputMessagePhoto
 *   VIDEO    -> InputMessageVideo
 *   DOCUMENT -> InputMessageDocument (disableContentTypeDetection = true)
 *
 * AUTO = определить автоматически (явный тип -> магические байты -> MIME -> расширение).
 *
 * Из REST/UI передаётся строкой: "photo", "video", "doc"/"file", "auto".
 * Старые значения ("voice", "round", "gif", "sticker", "audio") принимаются,
 * но приводятся к ближайшему поддерживаемому типу, чтобы ничего не падало.
 */
public enum MediaKind {
    AUTO,
    PHOTO,
    VIDEO,
    DOCUMENT;

    public static MediaKind parse(String s) {
        if (s == null || s.isBlank()) return AUTO;
        String v = s.trim().toLowerCase().replace("-", "_").replace(" ", "_");
        return switch (v) {
            case "", "auto", "detect" -> AUTO;

            case "photo", "image", "picture", "фото", "картинка",
                 "gif", "animation", "sticker", "webp",
                 "heic", "heif" -> PHOTO;          // HEIC потом даунгрейдится в DOCUMENT по MIME

            case "video", "movie", "clip", "видео", "ролик",
                 "round", "video_note", "videonote", "кружок" -> VIDEO;

            case "doc", "document", "file", "файл", "документ",
                 "audio", "music", "voice", "voice_note", "voicenote",
                 "аудио", "музыка", "голосовое", "войс" -> DOCUMENT;

            default -> {
                try {
                    yield MediaKind.valueOf(v.toUpperCase());
                } catch (IllegalArgumentException e) {
                    yield AUTO;
                }
            }
        };
    }
}