package ru.VK;

public class GroupData {
    String name;
    String shortName;
    String photoUrl;

    public GroupData(String name, String shortName, String photoUrl) {
        this.name = name;
        this.shortName = shortName;
        this.photoUrl = photoUrl;
    }

    public GroupData() {
    }

    public String getName() {
        return name;
    }

    public String getShortName() {
        return shortName;
    }

    public String getPhotoUrl() {
        return photoUrl;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setShortName(String shortName) {
        this.shortName = shortName;
    }

    public void setPhotoUrl(String photoUrl) {
        this.photoUrl = photoUrl;
    }
}
