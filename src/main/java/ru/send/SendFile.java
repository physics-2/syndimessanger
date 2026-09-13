package ru.send;

/**
 * Файл для отправки: либо уже лежит на диске (path), либо пришёл байтами из браузера.
 */
public record SendFile(String name, String mimeType, long size, String path, byte[] bytes) {

    public static SendFile of(String name, String mimeType, long size, String path) {
        return new SendFile(name, mimeType, size, path, null);
    }

    public static SendFile of(String name, String mimeType, long size, byte[] bytes) {
        return new SendFile(name, mimeType, size, null, bytes);
    }

    public boolean hasBytes() {
        return bytes != null && bytes.length > 0;
    }

    /**
     * Гарантированно возвращает путь на диске: если файл пришёл байтами —
     * кладём его во временный файл. Удалить через {@link #deleteIfTemp(java.nio.file.Path)}.
     */
    public java.nio.file.Path materialize(java.nio.file.Path tempDir) throws java.io.IOException {
        if (path != null && !path.isBlank()) return java.nio.file.Path.of(path);
        java.nio.file.Files.createDirectories(tempDir);
        String safe = name == null || name.isBlank() ? "file" : name.replaceAll("[\\\\/:*?\"<>|]", "_");
        return java.nio.file.Files.write(tempDir.resolve(System.currentTimeMillis() + "_" + safe), bytes);
    }

    public static void deleteIfTemp(java.nio.file.Path p) {
        if (p == null) return;
        try { java.nio.file.Files.deleteIfExists(p); } catch (Exception ignored) { }
    }
}
