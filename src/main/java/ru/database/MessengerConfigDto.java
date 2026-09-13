package ru.database;

import java.util.List;

public class MessengerConfigDto {
    private List<String> vkIds;
    private List<String> maxIds;
    private List<String> telegramIds;

    // Геттеры и сеттеры (или используй record, если Java 14+)
    public List<String> getVkIds() { return vkIds; }
    public void setVkIds(List<String> vkIds) { this.vkIds = vkIds; }
    public List<String> getMaxIds() { return maxIds; }
    public void setMaxIds(List<String> maxIds) { this.maxIds = maxIds; }
    public List<String> getTelegramIds() { return telegramIds; }
    public void setTelegramIds(List<String> telegramIds) { this.telegramIds = telegramIds; }
}