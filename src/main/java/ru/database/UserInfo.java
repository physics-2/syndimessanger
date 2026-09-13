package ru.database;
import java.util.List;

public record UserInfo(
        String source,
        long platformId,
        String firstName,
        String lastName,
        String username,
        String photoUrl,
        String phoneNumber,
        List<String> tags // <-- Новое поле
) {}