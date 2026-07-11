package org.schabi.newpipe.util.sonos;

import android.app.Activity;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.media.MediaMetadataRetriever;
import android.net.Uri;
import android.provider.OpenableColumns;
import android.util.Log;
import android.os.Handler;
import android.os.IBinder;
import android.os.Looper;
import android.widget.Toast;

import androidx.annotation.Nullable;
import androidx.appcompat.app.AlertDialog;
import androidx.preference.PreferenceManager;

import org.schabi.newpipe.R;
import org.schabi.newpipe.extractor.MediaFormat;
import org.schabi.newpipe.extractor.stream.AudioStream;
import org.schabi.newpipe.extractor.stream.DeliveryMethod;
import org.schabi.newpipe.extractor.stream.StreamInfo;
import org.schabi.newpipe.extractor.stream.StreamSegment;
import org.schabi.newpipe.streams.io.StoredFileHelper;
import org.schabi.newpipe.util.StreamTypeUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.reactivex.rxjava3.android.schedulers.AndroidSchedulers;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.schedulers.Schedulers;

import us.shandian.giga.get.DownloadMission;
import us.shandian.giga.get.HlsDownloadStreamHelper;
import us.shandian.giga.get.MissionRecoveryInfo;
import us.shandian.giga.service.DownloadManagerService;
import us.shandian.giga.service.MissionState;

/**
 * Plays a stream's audio on a Sonos speaker. Entry point for both the video
 * detail screen and the share flow (RouterActivity).
 *
 * <p>Flow: pick speaker (last-used or discovery + dialog) → if the stream has a
 * direct progressive URL, hand it to the speaker; otherwise (YouTube SABR)
 * download the audio to cache via the app's download pipeline, then serve it to
 * the speaker from {@link SonosStreamService}. Everything after speaker
 * selection runs on the application context, so a transient caller (share
 * activity) may finish once {@code onSpeakerChosen} fires.</p>
 */
public final class SonosPlayer {
    private static final String TAG = "SonosPlayer";
    static final String PREF_LAST_IP = "sonos_last_ip";
    static final String PREF_LAST_NAME = "sonos_last_name";
    static final String PREF_LAST_TITLE = "sonos_last_title";
    static final String PREF_LAST_DURATION = "sonos_last_duration";
    /** Chapter markers of the current track, "seconds|title" per line; "" = none. */
    static final String PREF_LAST_CHAPTERS = "sonos_last_chapters";
    /** True while the last-started playback is an endless live relay (no seek). */
    static final String PREF_LAST_LIVE = "sonos_last_live";
    /** Cache size cap in MB; set from the control screen's "Cache limit" menu. */
    static final String PREF_CACHE_MAX_MB = "sonos_cache_max_mb";
    static final long DEFAULT_CACHE_MAX_MB = 1024;

    private SonosPlayer() {
    }

    public static void play(final Activity activity, final StreamInfo info,
                            final boolean useLastSpeaker,
                            @Nullable final Runnable onSpeakerChosen) {
        final Context appContext = activity.getApplicationContext();
        if (StreamTypeUtil.isLiveStream(info.getStreamType())) {
            // live = endless, so download-then-serve can't work; relay the HLS
            // audio instead (see SonosStreamService.startLive)
            if (info.getHlsUrl() == null || info.getHlsUrl().isEmpty()) {
                Toast.makeText(appContext, R.string.sonos_no_compatible_stream,
                        Toast.LENGTH_LONG).show();
                settle(onSpeakerChosen);
                return;
            }
            chooseSpeaker(activity, useLastSpeaker, onSpeakerChosen, device -> {
                // a leftover queue session would later see STOPPED and tear down
                // the stream service under the new playback
                SonosQueuePlayer.stop();
                playLive(appContext, activity, device, info);
            });
            return;
        }
        if (pickStream(info, true) == null && pickStream(info, false) == null) {
            Toast.makeText(appContext, R.string.sonos_no_compatible_stream, Toast.LENGTH_LONG)
                    .show();
            settle(onSpeakerChosen);
            return;
        }
        chooseSpeaker(activity, useLastSpeaker, onSpeakerChosen, device -> {
            SonosQueuePlayer.stop();
            resolve(appContext, info, false,
                    (url, mimeType) ->
                            playUri(appContext, activity, device, info, url, mimeType),
                    throwable -> showError(appContext, throwable));
        });
    }

    /**
     * Plays a local audio file (content:// or file:// URI, e.g. from a file manager
     * or share sheet) on a Sonos speaker: pick speaker → copy into cache → serve.
     * The speaker must support the format natively (S2: MP3/AAC/FLAC/WAV/OGG;
     * Opus/WebM will fail with a Sonos error — no transcoding).
     */
    public static void playLocalFile(final Activity activity, final Uri uri,
                                     @Nullable final Runnable onSpeakerChosen) {
        final Context appContext = activity.getApplicationContext();
        chooseSpeaker(activity, false, onSpeakerChosen, device ->
                //noinspection ResultOfMethodCallIgnored
                Single.fromCallable(() -> importLocalFile(appContext, uri))
                        .subscribeOn(Schedulers.io())
                        .observeOn(AndroidSchedulers.mainThread())
                        .subscribe(item -> serve(appContext, item.title, item.durationSeconds,
                                        item.mimeType, item.file,
                                        (url, mimeType) -> playUri(appContext, activity, device,
                                                item.title, null, item.durationSeconds, "",
                                                url, mimeType),
                                        throwable -> showError(appContext, throwable)),
                                throwable -> showError(appContext, throwable)));
    }

    static final class LocalItem {
        final File file;
        final String title;
        final long durationSeconds;
        final String mimeType;

        LocalItem(final File file, final String title, final long durationSeconds,
                  final String mimeType) {
            this.file = file;
            this.title = title;
            this.durationSeconds = durationSeconds;
            this.mimeType = mimeType;
        }
    }

    /** Reads metadata and copies the URI's content into the Sonos cache (io thread). */
    static LocalItem importLocalFile(final Context appContext, final Uri uri)
            throws IOException {
        String mime = appContext.getContentResolver().getType(uri);
        String title = null;
        long durationSeconds = 0;
        final MediaMetadataRetriever retriever = new MediaMetadataRetriever();
        try {
            retriever.setDataSource(appContext, uri);
            title = retriever.extractMetadata(MediaMetadataRetriever.METADATA_KEY_TITLE);
            final String durationMs =
                    retriever.extractMetadata(MediaMetadataRetriever.METADATA_KEY_DURATION);
            if (durationMs != null) {
                durationSeconds = Long.parseLong(durationMs) / 1000;
            }
            if (mime == null) {
                mime = retriever.extractMetadata(MediaMetadataRetriever.METADATA_KEY_MIMETYPE);
            }
        } catch (final RuntimeException ignored) {
            // unreadable metadata — play anyway with the fallbacks below
        } finally {
            retriever.release();
        }
        if (title == null || title.isEmpty()) {
            title = displayName(appContext, uri);
        }
        if (mime == null) {
            mime = "audio/mpeg";
        }

        final File dir = new File(appContext.getCacheDir(), "sonos");
        //noinspection ResultOfMethodCallIgnored
        dir.mkdirs();
        trimCache(appContext, dir);
        // extension matters: the HTTP server derives its Content-Type from it
        final File file = new File(dir, "sonos-local-"
                + Math.abs(uri.toString().hashCode()) + extensionFor(mime));
        final File done = new File(file.getPath() + ".done");
        if (!done.exists() || file.length() == 0) {
            try (InputStream in = appContext.getContentResolver().openInputStream(uri);
                 OutputStream out = new FileOutputStream(file)) {
                if (in == null) {
                    throw new IOException("Cannot open " + uri);
                }
                final byte[] buffer = new byte[64 * 1024];
                int read;
                while ((read = in.read(buffer)) != -1) {
                    out.write(buffer, 0, read);
                }
            }
            //noinspection ResultOfMethodCallIgnored
            done.createNewFile();
        }
        return new LocalItem(file, title, durationSeconds, mime);
    }

    static String displayName(final Context appContext, final Uri uri) {
        try (Cursor cursor = appContext.getContentResolver().query(uri,
                new String[]{OpenableColumns.DISPLAY_NAME}, null, null, null)) {
            if (cursor != null && cursor.moveToFirst() && !cursor.isNull(0)) {
                return cursor.getString(0);
            }
        } catch (final RuntimeException ignored) {
        }
        final String segment = uri.getLastPathSegment();
        return segment != null ? segment : "Audio";
    }

    private static String extensionFor(final String mime) {
        switch (mime) {
            case "audio/mpeg": return ".mp3";
            case "audio/flac": case "audio/x-flac": return ".flac";
            case "audio/wav": case "audio/x-wav": return ".wav";
            case "audio/ogg": case "application/ogg": return ".ogg";
            default: return ".m4a";
        }
    }

    /**
     * Resolves a URL the speaker can play for this stream: the direct progressive
     * URL if one exists, otherwise (YouTube SABR) download-to-cache + local serve.
     * {@code onReady(url, mimeType)} fires on the main thread; may take as long as
     * the download. Also used per-item by {@link SonosQueuePlayer}.
     */
    static void resolve(final Context appContext, final StreamInfo info, final boolean quiet,
                        final BiConsumer<String, String> onReady,
                        final Consumer<Throwable> onError) {
        final AudioStream direct = pickStream(info, true);
        if (direct != null) {
            final String mimeType =
                    direct.getFormat() == MediaFormat.MP3 ? "audio/mpeg" : "audio/mp4";
            onReady.accept(direct.getContent(), mimeType);
            return;
        }
        final AudioStream sabr = pickStream(info, false);
        if (sabr == null) {
            onError.accept(new IOException(
                    appContext.getString(R.string.sonos_no_compatible_stream)));
            return;
        }
        downloadAndServe(appContext, info, sabr, quiet, onReady, onError);
    }

    @Nullable
    private static AudioStream pickStream(final StreamInfo info, final boolean directUrlOnly) {
        return info.getAudioStreams().stream()
                .filter(s -> directUrlOnly
                        ? s.isUrl() && s.getDeliveryMethod() == DeliveryMethod.PROGRESSIVE_HTTP
                        : s.getDeliveryMethod() == DeliveryMethod.SABR)
                .filter(s -> s.getFormat() == MediaFormat.M4A || s.getFormat() == MediaFormat.MP3)
                .max(Comparator.comparing((AudioStream s) -> s.getFormat() == MediaFormat.M4A)
                        .thenComparingInt(AudioStream::getAverageBitrate))
                .orElse(null);
    }

    private static void settle(@Nullable final Runnable onSpeakerChosen) {
        if (onSpeakerChosen != null) {
            onSpeakerChosen.run();
        }
    }

    static void chooseSpeaker(final Activity activity, final boolean useLastSpeaker,
                              @Nullable final Runnable onSpeakerChosen,
                              final Consumer<SonosDevice> callback) {
        final Context appContext = activity.getApplicationContext();
        final SharedPreferences prefs = PreferenceManager.getDefaultSharedPreferences(appContext);
        final String lastIp = prefs.getString(PREF_LAST_IP, null);
        if (useLastSpeaker && lastIp != null) {
            settle(onSpeakerChosen);
            callback.accept(new SonosDevice(lastIp, prefs.getString(PREF_LAST_NAME, lastIp)));
            return;
        }
        Toast.makeText(appContext, R.string.sonos_searching, Toast.LENGTH_SHORT).show();
        //noinspection ResultOfMethodCallIgnored
        Single.fromCallable(() -> SonosDiscovery.discover(appContext))
                .subscribeOn(Schedulers.io())
                .observeOn(AndroidSchedulers.mainThread())
                .subscribe(devices -> {
                    if (devices.isEmpty()) {
                        Toast.makeText(appContext, R.string.sonos_no_speakers_found,
                                Toast.LENGTH_LONG).show();
                        settle(onSpeakerChosen);
                    } else if (devices.size() == 1 || !isUsable(activity)) {
                        settle(onSpeakerChosen);
                        callback.accept(devices.get(0));
                    } else {
                        new AlertDialog.Builder(activity)
                                .setTitle(R.string.sonos_select_speaker)
                                .setItems(devices.stream().map(SonosDevice::getRoomName)
                                                .toArray(CharSequence[]::new),
                                        (dialog, which) -> {
                                            settle(onSpeakerChosen);
                                            callback.accept(devices.get(which));
                                        })
                                .setOnCancelListener(dialog -> settle(onSpeakerChosen))
                                .show();
                    }
                }, throwable -> {
                    showError(appContext, throwable);
                    settle(onSpeakerChosen);
                });
    }

    private static boolean isUsable(final Activity activity) {
        return !activity.isFinishing() && !activity.isDestroyed();
    }

    private static File cacheFile(final Context appContext, final StreamInfo info) {
        final File dir = new File(appContext.getCacheDir(), "sonos");
        //noinspection ResultOfMethodCallIgnored
        dir.mkdirs();
        return new File(dir, "sonos-" + Math.abs(info.getUrl().hashCode()) + ".m4a");
    }

    private static void downloadAndServe(final Context appContext, final StreamInfo info,
                                         final AudioStream sabrStream, final boolean quiet,
                                         final BiConsumer<String, String> onReady,
                                         final Consumer<Throwable> onError) {
        final File file = cacheFile(appContext, info);
        if (new File(file.getPath() + ".done").exists() && file.length() > 0) {
            Log.i(TAG, "cache hit: " + file.getName());
            serve(appContext, info, file, onReady, onError);
            return;
        }
        Log.i(TAG, "cache miss, downloading: " + file.getName());
        try {
            trimCache(appContext, file.getParentFile());
            //noinspection ResultOfMethodCallIgnored
            file.createNewFile(); // no-op if a partial download already exists
            final StoredFileHelper storage = new StoredFileHelper(appContext,
                    Uri.fromFile(file.getParentFile()), Uri.fromFile(file), "sonos");
            awaitDownload(appContext, file, storage,
                    () -> serve(appContext, info, file, onReady, onError), onError, () -> {
                        DownloadManagerService.startMission(appContext,
                                new String[]{sabrStream.getContent()}, storage, 'a', 1,
                                info.getUrl(), null, null, 0,
                                new MissionRecoveryInfo[]{
                                        new MissionRecoveryInfo(sabrStream)},
                                HlsDownloadStreamHelper
                                        .buildResourceDeliveryMethods(sabrStream, null),
                                HlsDownloadStreamHelper
                                        .buildResourceManifestUrls(sabrStream, null),
                                HlsDownloadStreamHelper
                                        .buildResourceIsUrls(sabrStream, null));
                    });
            if (!quiet) {
                Toast.makeText(appContext, R.string.sonos_downloading, Toast.LENGTH_LONG)
                        .show();
            }
        } catch (final IOException e) {
            onError.accept(e);
        }
    }

    /**
     * Evicts the least-recently-played files (never currently-served ones) until
     * the cache dir is back under the configured cap. Files otherwise persist across
     * playbacks so replays and prev-skips are instant cache hits.
     */
    private static void trimCache(final Context appContext, @Nullable final File dir) {
        final File[] files = dir == null
                ? null : dir.listFiles((d, name) -> !name.endsWith(".done"));
        if (files == null) {
            return;
        }
        final long maxBytes = PreferenceManager.getDefaultSharedPreferences(appContext)
                .getLong(PREF_CACHE_MAX_MB, DEFAULT_CACHE_MAX_MB) * 1048576L;
        long total = 0;
        for (final File f : files) {
            total += f.length();
        }
        Arrays.sort(files, Comparator.comparingLong(File::lastModified));
        for (final File f : files) {
            if (total <= maxBytes) {
                break;
            }
            if (SonosStreamService.isServing(f)) {
                continue;
            }
            total -= f.length();
            //noinspection ResultOfMethodCallIgnored
            f.delete();
            //noinspection ResultOfMethodCallIgnored
            new File(f.getPath() + ".done").delete();
        }
    }

    private static void serve(final Context appContext, final StreamInfo info, final File file,
                              final BiConsumer<String, String> onReady,
                              final Consumer<Throwable> onError) {
        serve(appContext, info.getName(), info.getDuration(), "audio/mp4", file,
                onReady, onError);
    }

    static void serve(final Context appContext, final String title,
                      final long durationSeconds, final String mimeType,
                      final File file, final BiConsumer<String, String> onReady,
                      final Consumer<Throwable> onError) {
        //noinspection ResultOfMethodCallIgnored
        file.setLastModified(System.currentTimeMillis()); // LRU touch for trimCache
        try {
            onReady.accept(SonosStreamService.start(appContext, file, title, durationSeconds),
                    mimeType);
        } catch (final IOException e) {
            onError.accept(e);
        }
    }

    /**
     * Binds to the download service, attaches a finish/error listener for
     * {@code file}, then either adopts an already-known mission for it or runs
     * {@code startFreshMission}. Adoption covers the app being killed
     * mid-download: the reloaded mission auto-resumes on service start, and
     * blindly starting a second one would corrupt the shared target file.
     */
    private static void awaitDownload(final Context appContext, final File file,
                                      final StoredFileHelper storage,
                                      final Runnable onFinished,
                                      final Consumer<Throwable> onError,
                                      final Runnable startFreshMission) {
        final Uri expectedUri = Uri.fromFile(file);
        final ServiceConnection connection = new ServiceConnection() {
            private DownloadManagerService.DownloadManagerBinder binder;
            private boolean detached;
            private final Handler.Callback callback = msg -> {
                if (detached || !(msg.obj instanceof DownloadMission)) {
                    return false;
                }
                final DownloadMission mission = (DownloadMission) msg.obj;
                if (!expectedUri.equals(mission.storage.getUri())) {
                    return false;
                }
                if (msg.what == DownloadManagerService.MESSAGE_FINISHED) {
                    try {
                        //noinspection ResultOfMethodCallIgnored
                        new File(file.getPath() + ".done").createNewFile();
                    } catch (final IOException ignored) {
                    }
                    // The Sonos download is an implementation detail: drop its row from
                    // the downloads list (keeps the file — forgetMission nulls storage
                    // before delete(); SonosStreamService removes the file on stop).
                    binder.getDownloadManager().forgetMission(mission.storage);
                    detach();
                    onFinished.run();
                } else if (msg.what == DownloadManagerService.MESSAGE_ERROR) {
                    detach();
                    onError.accept(new IOException("audio download failed, code "
                            + mission.errCode));
                }
                return false;
            };

            private void detach() {
                if (detached) {
                    return;
                }
                detached = true;
                // posted: we're called from inside the download service's observer
                // loop — removing the listener synchronously would throw a
                // ConcurrentModificationException in its iteration
                new Handler(Looper.getMainLooper()).post(() -> {
                    if (binder != null) {
                        binder.removeMissionEventListener(callback);
                    }
                    appContext.unbindService(this);
                });
            }

            @Override
            public void onServiceConnected(final ComponentName name, final IBinder service) {
                binder = (DownloadManagerService.DownloadManagerBinder) service;
                binder.addMissionEventListener(callback);
                final MissionState state =
                        binder.getDownloadManager().checkForExistingMission(storage);
                if (state == MissionState.Finished) {
                    // finished while nobody was listening (app killed mid-download,
                    // giga auto-resumed and completed it) — just mark and serve
                    Log.i(TAG, "adopting finished mission for " + file.getName());
                    try {
                        //noinspection ResultOfMethodCallIgnored
                        new File(file.getPath() + ".done").createNewFile();
                    } catch (final IOException ignored) {
                    }
                    binder.getDownloadManager().forgetMission(storage);
                    detach();
                    onFinished.run();
                } else if (state == MissionState.None) {
                    startFreshMission.run();
                } else {
                    // Pending/PendingRunning: reuse it — the listener above gets its
                    // finish event. ponytail: a manually-paused mission stays paused;
                    // resume it from the downloads screen if that ever happens.
                    Log.i(TAG, "adopting " + state + " mission for " + file.getName());
                }
            }

            @Override
            public void onServiceDisconnected(final ComponentName name) {
            }
        };
        appContext.bindService(new Intent(appContext, DownloadManagerService.class),
                connection, Context.BIND_AUTO_CREATE);
    }

    /**
     * The stream's chapter markers (video-description timestamps) serialized for
     * {@link #PREF_LAST_CHAPTERS}: one "seconds|title" line each, "" if none.
     * The control screen renders them as a tap-to-seek chapter list.
     */
    static String chaptersOf(final StreamInfo info) {
        final StringBuilder chapters = new StringBuilder();
        for (final StreamSegment segment : info.getStreamSegments()) {
            if (chapters.length() > 0) {
                chapters.append('\n');
            }
            chapters.append(segment.getStartTimeSeconds()).append('|').append(
                    segment.getTitle() == null ? "" : segment.getTitle().replace('\n', ' '));
        }
        return chapters.toString();
    }

    /** Persists what the control screen shows on open: speaker + current track. */
    static void persistLast(final Context appContext, final SonosDevice device,
                            final String title, final long durationSeconds) {
        persistLast(appContext, device, title, durationSeconds, "");
    }

    static void persistLast(final Context appContext, final SonosDevice device,
                            final String title, final long durationSeconds,
                            final String chapters) {
        persistLast(appContext, device, title, durationSeconds, chapters, false);
    }

    private static void persistLast(final Context appContext, final SonosDevice device,
                                    final String title, final long durationSeconds,
                                    final String chapters, final boolean live) {
        PreferenceManager.getDefaultSharedPreferences(appContext).edit()
                .putString(PREF_LAST_IP, device.getIp())
                .putString(PREF_LAST_NAME, device.getRoomName())
                .putString(PREF_LAST_TITLE, title)
                .putLong(PREF_LAST_DURATION, durationSeconds)
                .putString(PREF_LAST_CHAPTERS, chapters)
                .putBoolean(PREF_LAST_LIVE, live)
                .apply();
        SonosStreamService.refreshNotification();
    }

    private static void playUri(final Context appContext, final Activity activity,
                                final SonosDevice device, final StreamInfo info,
                                final String url, final String mimeType) {
        playUri(appContext, activity, device, info.getName(), info.getThumbnailUrl(),
                info.getDuration(), chaptersOf(info), url, mimeType);
    }

    private static void playUri(final Context appContext, final Activity activity,
                                final SonosDevice device, final String title,
                                @Nullable final String thumbnailUrl,
                                final long durationSeconds, final String chapters,
                                final String url, final String mimeType) {
        persistLast(appContext, device, title, durationSeconds, chapters);
        //noinspection ResultOfMethodCallIgnored
        Completable.fromAction(() -> device.playUri(url, title,
                        thumbnailUrl, durationSeconds, mimeType))
                .subscribeOn(Schedulers.io())
                .observeOn(AndroidSchedulers.mainThread())
                .subscribe(() -> announcePlaying(appContext, activity, device),
                        throwable -> showError(appContext, throwable));
    }

    /**
     * Live streams have no downloadable end — instead the phone relays them:
     * pick a muxed HLS variant carrying AAC-LC (YouTube live has no audio-only
     * rendition), and {@link SonosStreamService#startLive} remuxes its audio to
     * an endless ADTS stream the speaker treats as web radio.
     */
    private static void playLive(final Context appContext, final Activity activity,
                                 final SonosDevice device, final StreamInfo info) {
        persistLast(appContext, device, info.getName(), 0, "", true);
        //noinspection ResultOfMethodCallIgnored
        Completable.fromAction(() -> device.playLiveUri(
                        SonosStreamService.startLive(appContext,
                                pickLiveVariant(info.getHlsUrl()), info.getName()),
                        info.getName(), info.getThumbnailUrl(), "audio/aac"))
                .subscribeOn(Schedulers.io())
                .observeOn(AndroidSchedulers.mainThread())
                .subscribe(() -> announcePlaying(appContext, activity, device),
                        throwable -> showError(appContext, throwable));
    }

    /**
     * Picks the variant to relay from an HLS master playlist: lowest bandwidth
     * whose CODECS contain AAC-LC ("mp4a.40.2" — copies straight to ADTS; the
     * lower itags carry HE-AAC the speaker may not decode), else lowest overall.
     * Returns the input unchanged if it's already a media playlist.
     */
    static String pickLiveVariant(final String masterUrl) throws IOException {
        final List<String> lines = new ArrayList<>();
        final HttpURLConnection connection =
                (HttpURLConnection) new URL(masterUrl).openConnection();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                connection.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line.trim());
            }
        } finally {
            connection.disconnect();
        }
        final Pattern bandwidthPattern = Pattern.compile("BANDWIDTH=(\\d+)");
        String bestUrl = null;
        long bestBandwidth = Long.MAX_VALUE;
        boolean bestIsAacLc = false;
        for (int i = 0; i < lines.size() - 1; i++) {
            if (!lines.get(i).startsWith("#EXT-X-STREAM-INF:")) {
                continue;
            }
            String uri = null;
            for (int j = i + 1; j < lines.size(); j++) {
                if (!lines.get(j).isEmpty() && !lines.get(j).startsWith("#")) {
                    uri = lines.get(j);
                    break;
                }
            }
            if (uri == null) {
                continue;
            }
            final Matcher matcher = bandwidthPattern.matcher(lines.get(i));
            final long bandwidth = matcher.find()
                    ? Long.parseLong(matcher.group(1)) : Long.MAX_VALUE;
            final boolean aacLc = lines.get(i).contains("mp4a.40.2");
            if (bestUrl == null || (aacLc && !bestIsAacLc)
                    || (aacLc == bestIsAacLc && bandwidth < bestBandwidth)) {
                bestUrl = new URL(new URL(masterUrl), uri).toString();
                bestBandwidth = bandwidth;
                bestIsAacLc = aacLc;
            }
        }
        return bestUrl != null ? bestUrl : masterUrl;
    }

    private static void announcePlaying(final Context appContext, final Activity activity,
                                        final SonosDevice device) {
        Toast.makeText(appContext,
                appContext.getString(R.string.sonos_playing_toast, device.getRoomName()),
                Toast.LENGTH_SHORT).show();
        if (isUsable(activity)) {
            activity.startActivity(new Intent(activity, SonosControlActivity.class));
        }
    }

    private static void showError(final Context appContext, final Throwable throwable) {
        Toast.makeText(appContext,
                appContext.getString(R.string.sonos_error, String.valueOf(throwable.getMessage())),
                Toast.LENGTH_LONG).show();
    }
}
