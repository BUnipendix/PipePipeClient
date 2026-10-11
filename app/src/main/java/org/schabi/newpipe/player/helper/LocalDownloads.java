package org.schabi.newpipe.player.helper;

import android.content.Context;
import android.content.res.AssetFileDescriptor;
import android.net.Uri;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import us.shandian.giga.get.FinishedMission;
import us.shandian.giga.get.sqlite.FinishedMissionStore;

/**
 * Checks whether a video that is about to be played is already sitting on disk from an earlier
 * download, so that it can be read locally instead of being streamed a second time.
 *
 * <p>The finished download table keeps the page url a download came from next to the uri of the
 * file, which is exactly the mapping needed here.</p>
 */
public final class LocalDownloads {
    private LocalDownloads() {
    }

    /**
     * @param videoUrl the page url of the video about to be played
     * @return the uri of the downloaded file, or null when there is none that can be read
     */
    /**
     * The download table is read once and reused for a short while: the player asks for a
     * source for the current item and its neighbours, and opening the database for every one
     * of those would be wasteful.
     */
    private static final long CACHE_TTL_MS = 10_000L;

    @Nullable
    private static Map<String, String> cachedPaths;
    private static long cacheBuiltAt;

    private static synchronized Map<String, String> downloadPaths(
            @NonNull final Context context) {
        final long now = System.currentTimeMillis();
        if (cachedPaths != null && now - cacheBuiltAt <= CACHE_TTL_MS) {
            return cachedPaths;
        }

        final Map<String, String> paths = new HashMap<>();
        final FinishedMissionStore store = new FinishedMissionStore(context);
        try {
            for (final FinishedMission mission : store.loadFinishedMissions()) {
                if (mission.source == null || mission.storage == null
                        || mission.storage.isInvalid()) {
                    continue;
                }
                paths.put(mission.source, mission.storage.getUri().toString());
            }
        } finally {
            store.close();
        }

        cachedPaths = paths;
        cacheBuiltAt = now;
        return paths;
    }

    @Nullable
    public static Uri findLocalFile(@NonNull final Context context,
                                    @Nullable final String videoUrl) {
        if (videoUrl == null || videoUrl.isEmpty()) {
            return null;
        }

        try {
            final String path = downloadPaths(context).get(videoUrl);
            if (path == null) {
                return null;
            }

            // Works for both a plain file path and a document uri picked through the system
            // file chooser, but only if the file is still readable.
            final Uri uri = Uri.parse(path);
            try (AssetFileDescriptor descriptor =
                         context.getContentResolver().openAssetFileDescriptor(uri, "r")) {
                if (descriptor == null) {
                    android.util.Log.d("PipePipeLocal", "unreadable " + uri);
                    return null;
                }
                android.util.Log.d("PipePipeLocal", "HIT " + videoUrl + " -> " + uri);
                return uri;
            }
        } catch (final Exception e) {
            android.util.Log.d("PipePipeLocal", "error " + e);
            return null;
        }
    }
}
