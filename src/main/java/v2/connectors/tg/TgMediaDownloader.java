package v2.connectors.tg;

import it.tdlight.client.SimpleTelegramClient;
import it.tdlight.jni.TdApi;
import java.util.concurrent.TimeUnit;

public class TgMediaDownloader {
    private final SimpleTelegramClient client;
    private final boolean downloadEnabled;

    public TgMediaDownloader(SimpleTelegramClient client, boolean downloadEnabled) {
        this.client = client;
        this.downloadEnabled = downloadEnabled;
    }

    public String extractMediaLocalPath(TdApi.MessageContent c) {
        if (!downloadEnabled) return "";
        int fileId = extractFileId(c);
        return (fileId > 0) ? downloadFileToLocal(fileId) : "";
    }

    public String downloadUserAvatar(TdApi.User u) {
        if (!downloadEnabled || u.profilePhoto == null) return "";
        return downloadFileToLocal(u.profilePhoto.small.id);
    }

    private int extractFileId(TdApi.MessageContent c) {
        if (c instanceof TdApi.MessagePhoto p && p.photo != null && p.photo.sizes.length > 0) {
            return p.photo.sizes[p.photo.sizes.length - 1].photo.id;
        } else if (c instanceof TdApi.MessageVideo v && v.video != null) {
            return v.video.video.id;
        } else if (c instanceof TdApi.MessageDocument d && d.document != null) {
            return d.document.document.id;
        }
        return -1;
    }

    private String downloadFileToLocal(int fileId) {
        try {
            TdApi.File file = client.send(new TdApi.DownloadFile(fileId, 1, 0, 0, true))
                    .get(60, TimeUnit.SECONDS);
            if (file.local != null && file.local.isDownloadingCompleted) {
                return file.local.path;
            }
        } catch (Exception e) {
            System.err.println("⚠️ [TG] Ошибка скачивания файла " + fileId + ": " + e.getMessage());
        }
        return "";
    }
}