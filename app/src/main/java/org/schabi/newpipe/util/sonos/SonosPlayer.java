package org.schabi.newpipe.util.sonos;

import android.app.Activity;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.ServiceConnection;
import android.content.SharedPreferences;
import android.net.Uri;
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
import org.schabi.newpipe.streams.io.StoredFileHelper;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
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
    private static final String PREF_LAST_IP = "sonos_last_ip";
    private static final String PREF_LAST_NAME = "sonos_last_name";

    private SonosPlayer() {
    }

    public static void play(final Activity activity, final StreamInfo info,
                            final boolean useLastSpeaker,
                            @Nullable final Runnable onSpeakerChosen) {
        final Context appContext = activity.getApplicationContext();
        if (pickStream(info, true) == null && pickStream(info, false) == null) {
            Toast.makeText(appContext, R.string.sonos_no_compatible_stream, Toast.LENGTH_LONG)
                    .show();
            settle(onSpeakerChosen);
            return;
        }
        chooseSpeaker(activity, useLastSpeaker, onSpeakerChosen, device ->
                resolve(appContext, info, false,
                        (url, mimeType) ->
                                playUri(appContext, activity, device, info, url, mimeType),
                        throwable -> showError(appContext, throwable)));
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
            serve(appContext, info, file, onReady, onError);
            return;
        }
        try {
            trimCache(file.getParentFile());
            //noinspection ResultOfMethodCallIgnored
            file.createNewFile();
            final StoredFileHelper storage = new StoredFileHelper(appContext,
                    Uri.fromFile(file.getParentFile()), Uri.fromFile(file), "sonos");
            awaitDownload(appContext, file,
                    () -> serve(appContext, info, file, onReady, onError), onError);
            DownloadManagerService.startMission(appContext,
                    new String[]{sabrStream.getContent()}, storage, 'a', 1, info.getUrl(),
                    null, null, 0,
                    new MissionRecoveryInfo[]{new MissionRecoveryInfo(sabrStream)},
                    HlsDownloadStreamHelper.buildResourceDeliveryMethods(sabrStream, null),
                    HlsDownloadStreamHelper.buildResourceManifestUrls(sabrStream, null),
                    HlsDownloadStreamHelper.buildResourceIsUrls(sabrStream, null));
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
     * the cache dir is back under the cap. Files otherwise persist across playbacks
     * so replays and prev-skips are instant cache hits.
     */
    // ponytail: fixed 1 GB cap, no setting; Android may evict the cache dir earlier anyway
    private static final long MAX_CACHE_BYTES = 1024L * 1024 * 1024;

    private static void trimCache(@Nullable final File dir) {
        final File[] files = dir == null
                ? null : dir.listFiles((d, name) -> !name.endsWith(".done"));
        if (files == null) {
            return;
        }
        long total = 0;
        for (final File f : files) {
            total += f.length();
        }
        Arrays.sort(files, Comparator.comparingLong(File::lastModified));
        for (final File f : files) {
            if (total <= MAX_CACHE_BYTES) {
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
        //noinspection ResultOfMethodCallIgnored
        file.setLastModified(System.currentTimeMillis()); // LRU touch for trimCache
        try {
            onReady.accept(SonosStreamService.start(appContext, file, info.getName(),
                    info.getDuration()), "audio/mp4");
        } catch (final IOException e) {
            onError.accept(e);
        }
    }

    private static void awaitDownload(final Context appContext, final File file,
                                      final Runnable onFinished,
                                      final Consumer<Throwable> onError) {
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
            }

            @Override
            public void onServiceDisconnected(final ComponentName name) {
            }
        };
        appContext.bindService(new Intent(appContext, DownloadManagerService.class),
                connection, Context.BIND_AUTO_CREATE);
    }

    /** Persists what the control screen shows on open: speaker + current track. */
    static void persistLast(final Context appContext, final SonosDevice device,
                            final StreamInfo info) {
        PreferenceManager.getDefaultSharedPreferences(appContext).edit()
                .putString(PREF_LAST_IP, device.getIp())
                .putString(PREF_LAST_NAME, device.getRoomName())
                .putString("sonos_last_title", info.getName())
                .putLong("sonos_last_duration", info.getDuration())
                .apply();
    }

    private static void playUri(final Context appContext, final Activity activity,
                                final SonosDevice device, final StreamInfo info,
                                final String url, final String mimeType) {
        persistLast(appContext, device, info);
        //noinspection ResultOfMethodCallIgnored
        Completable.fromAction(() -> device.playUri(url, info.getName(),
                        info.getThumbnailUrl(), info.getDuration(), mimeType))
                .subscribeOn(Schedulers.io())
                .observeOn(AndroidSchedulers.mainThread())
                .subscribe(() -> {
                    Toast.makeText(appContext,
                            appContext.getString(R.string.sonos_playing_toast,
                                    device.getRoomName()),
                            Toast.LENGTH_SHORT).show();
                    if (isUsable(activity)) {
                        activity.startActivity(
                                new Intent(activity, SonosControlActivity.class));
                    }
                }, throwable -> showError(appContext, throwable));
    }

    private static void showError(final Context appContext, final Throwable throwable) {
        Toast.makeText(appContext,
                appContext.getString(R.string.sonos_error, String.valueOf(throwable.getMessage())),
                Toast.LENGTH_LONG).show();
    }
}
