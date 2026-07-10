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
import android.widget.Button;
import android.widget.LinearLayout;
import android.widget.SeekBar;
import android.widget.TextView;
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
import java.util.Comparator;
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
        final AudioStream direct = pickStream(info, true);
        final AudioStream sabr = direct == null ? pickStream(info, false) : null;
        if (direct == null && sabr == null) {
            Toast.makeText(appContext, R.string.sonos_no_compatible_stream, Toast.LENGTH_LONG)
                    .show();
            settle(onSpeakerChosen);
            return;
        }
        chooseSpeaker(activity, useLastSpeaker, onSpeakerChosen, device -> {
            if (direct != null) {
                final String mimeType =
                        direct.getFormat() == MediaFormat.MP3 ? "audio/mpeg" : "audio/mp4";
                playUri(appContext, activity, device, info, direct.getContent(), mimeType);
            } else {
                downloadAndServe(appContext, activity, device, info, sabr);
            }
        });
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

    private static void chooseSpeaker(final Activity activity, final boolean useLastSpeaker,
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

    private static void downloadAndServe(final Context appContext, final Activity activity,
                                         final SonosDevice device, final StreamInfo info,
                                         final AudioStream sabrStream) {
        final File file = cacheFile(appContext, info);
        if (new File(file.getPath() + ".done").exists() && file.length() > 0) {
            serveAndPlay(appContext, activity, device, info, file);
            return;
        }
        try {
            final File[] stale = file.getParentFile() == null
                    ? null : file.getParentFile().listFiles();
            if (stale != null) {
                for (final File f : stale) {
                    //noinspection ResultOfMethodCallIgnored
                    f.delete();
                }
            }
            //noinspection ResultOfMethodCallIgnored
            file.createNewFile();
            final StoredFileHelper storage = new StoredFileHelper(appContext,
                    Uri.fromFile(file.getParentFile()), Uri.fromFile(file), "sonos");
            awaitDownload(appContext, file,
                    () -> serveAndPlay(appContext, activity, device, info, file));
            DownloadManagerService.startMission(appContext,
                    new String[]{sabrStream.getContent()}, storage, 'a', 1, info.getUrl(),
                    null, null, 0,
                    new MissionRecoveryInfo[]{new MissionRecoveryInfo(sabrStream)},
                    HlsDownloadStreamHelper.buildResourceDeliveryMethods(sabrStream, null),
                    HlsDownloadStreamHelper.buildResourceManifestUrls(sabrStream, null),
                    HlsDownloadStreamHelper.buildResourceIsUrls(sabrStream, null));
            Toast.makeText(appContext, R.string.sonos_downloading, Toast.LENGTH_LONG).show();
        } catch (final IOException e) {
            showError(appContext, e);
        }
    }

    private static void awaitDownload(final Context appContext, final File file,
                                      final Runnable onFinished) {
        final Uri expectedUri = Uri.fromFile(file);
        final ServiceConnection connection = new ServiceConnection() {
            private DownloadManagerService.DownloadManagerBinder binder;
            private final Handler.Callback callback = msg -> {
                if (!(msg.obj instanceof DownloadMission)) {
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
                    detach();
                    onFinished.run();
                } else if (msg.what == DownloadManagerService.MESSAGE_ERROR) {
                    detach();
                    showError(appContext, new IOException("audio download failed, code "
                            + mission.errCode));
                }
                return false;
            };

            private void detach() {
                if (binder != null) {
                    binder.removeMissionEventListener(callback);
                }
                appContext.unbindService(this);
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

    private static void serveAndPlay(final Context appContext, final Activity activity,
                                     final SonosDevice device, final StreamInfo info,
                                     final File file) {
        final String serveUrl;
        try {
            serveUrl = SonosStreamService.start(appContext, file, info.getName(),
                    info.getDuration());
        } catch (final IOException e) {
            showError(appContext, e);
            return;
        }
        playUri(appContext, activity, device, info, serveUrl, "audio/mp4");
    }

    private static void playUri(final Context appContext, final Activity activity,
                                final SonosDevice device, final StreamInfo info,
                                final String url, final String mimeType) {
        PreferenceManager.getDefaultSharedPreferences(appContext).edit()
                .putString(PREF_LAST_IP, device.getIp())
                .putString(PREF_LAST_NAME, device.getRoomName())
                .apply();
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
                        showControls(activity, device);
                    }
                }, throwable -> showError(appContext, throwable));
    }

    public static void showControls(final Activity activity, final SonosDevice device) {
        final Context appContext = activity.getApplicationContext();
        final int pad = (int) (20 * activity.getResources().getDisplayMetrics().density);
        final LinearLayout layout = new LinearLayout(activity);
        layout.setOrientation(LinearLayout.VERTICAL);
        layout.setPadding(pad, pad / 2, pad, 0);

        final LinearLayout buttons = new LinearLayout(activity);
        buttons.setOrientation(LinearLayout.HORIZONTAL);
        final LinearLayout.LayoutParams buttonParams =
                new LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1);
        final Button resumeButton = new Button(activity);
        resumeButton.setText(R.string.sonos_resume);
        resumeButton.setOnClickListener(v -> runAction(appContext, device::play));
        final Button pauseButton = new Button(activity);
        pauseButton.setText(R.string.pause);
        pauseButton.setOnClickListener(v -> runAction(appContext, device::pause));
        final Button stopButton = new Button(activity);
        stopButton.setText(R.string.stop);
        stopButton.setOnClickListener(v -> runAction(appContext, device::stop));
        buttons.addView(resumeButton, buttonParams);
        buttons.addView(pauseButton, buttonParams);
        buttons.addView(stopButton, buttonParams);
        layout.addView(buttons);

        final TextView volumeLabel = new TextView(activity);
        volumeLabel.setText(R.string.sonos_volume);
        layout.addView(volumeLabel);
        final SeekBar volumeBar = new SeekBar(activity);
        volumeBar.setMax(100);
        layout.addView(volumeBar);
        //noinspection ResultOfMethodCallIgnored
        Single.fromCallable(device::getVolume)
                .subscribeOn(Schedulers.io())
                .observeOn(AndroidSchedulers.mainThread())
                .subscribe(volumeBar::setProgress, throwable -> { });
        volumeBar.setOnSeekBarChangeListener(new SeekBar.OnSeekBarChangeListener() {
            @Override
            public void onProgressChanged(final SeekBar seekBar, final int progress,
                                          final boolean fromUser) {
            }

            @Override
            public void onStartTrackingTouch(final SeekBar seekBar) {
            }

            @Override
            public void onStopTrackingTouch(final SeekBar seekBar) {
                runAction(appContext, () -> device.setVolume(seekBar.getProgress()));
            }
        });

        new AlertDialog.Builder(activity)
                .setTitle(device.getRoomName())
                .setView(layout)
                .setPositiveButton(android.R.string.ok, null)
                .show();
    }

    private static void runAction(final Context appContext,
                                  final io.reactivex.rxjava3.functions.Action action) {
        //noinspection ResultOfMethodCallIgnored
        Completable.fromAction(action)
                .subscribeOn(Schedulers.io())
                .observeOn(AndroidSchedulers.mainThread())
                .subscribe(() -> { }, throwable -> showError(appContext, throwable));
    }

    private static void showError(final Context appContext, final Throwable throwable) {
        Toast.makeText(appContext,
                appContext.getString(R.string.sonos_error, String.valueOf(throwable.getMessage())),
                Toast.LENGTH_LONG).show();
    }
}
