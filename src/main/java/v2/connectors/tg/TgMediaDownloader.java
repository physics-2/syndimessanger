package v2.connectors.tg;

import it.tdlight.client.SimpleTelegramClient;
import it.tdlight.jni.TdApi;
import java.util.concurrent.TimeUnit;

public class TgMediaDownloader {
    private boolean downloadEnabled;

    public TgMediaDownloader(boolean downloadEnabled) {
        this.downloadEnabled = downloadEnabled;
    }

    public String extractMediaLocalPath(TdApi.MessageContent c) {
        if (!downloadEnabled) return "";
        return extractFileId(c);
    }

    public String downloadUserAvatar(TdApi.User u) {
        if (!downloadEnabled || u.profilePhoto == null) return "";
        return u.profilePhoto.big.local.path;
    }

    private String extractFileId(TdApi.MessageContent c) {
        if (c instanceof TdApi.MessagePhoto p && p.photo != null && p.photo.sizes.length > 0) {
            return p.photo.sizes[p.photo.sizes.length - 1].photo.local.path;
        } else if (c instanceof TdApi.MessageVideo v && v.video != null) {
            return v.video.video.local.path;
        } else if (c instanceof TdApi.MessageDocument d && d.document != null) {
            return d.document.document.local.path;
        }
        return "";
    }


    public boolean isDownloadEnabled() {
        return downloadEnabled;
    }

    public void setDownloadEnabled(boolean downloadEnabled) {
        this.downloadEnabled = downloadEnabled;
    }
}