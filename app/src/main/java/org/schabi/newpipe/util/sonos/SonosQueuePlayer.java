package org.schabi.newpipe.util.sonos;

import android.app.Activity;
import android.content.Context;
import android.content.Intent;
import android.util.Log;
import android.widget.Toast;

import org.schabi.newpipe.R;
import org.schabi.newpipe.extractor.stream.StreamInfo;
import org.schabi.newpipe.player.playqueue.PlayQueueItem;
import androidx.annotation.Nullable;

import org.schabi.newpipe.util.ExtractorHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.reactivex.rxjava3.android.schedulers.AndroidSchedulers;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.functions.Action;
import io.reactivex.rxjava3.schedulers.Schedulers;

/**
 * Plays a whole play queue on a Sonos speaker, advancing track to track.
 *
 * <p>Per item: fetch {@link StreamInfo} → {@link SonosPlayer#resolve} (direct URL
 * or download-to-cache + local serve). The first item plays via
 * {@code SetAVTransportURI}; the following one is prefetched and queued with
 * {@code SetNextAVTransportURI} so the speaker auto-advances near-gapless.
 * A 5 s poll of {@code GetPositionInfo} detects the advance (TrackURI switched to
 * the queued URL), retires the played file and queues the item after next.
 * Items that fail to resolve are skipped. One session at a time.</p>
 */
public final class SonosQueuePlayer {
    private static final String TAG = "SonosQueuePlayer";
    @SuppressWarnings("StaticFieldLeak") // holds the application context only
    private static Session session;

    private SonosQueuePlayer() {
    }

    public static void play(final Activity activity, final List<PlayQueueItem> items) {
        if (items.isEmpty()) {
            return;
        }
        SonosPlayer.chooseSpeaker(activity, false, null, device -> {
            stop();
            session = new Session(activity.getApplicationContext(), device, items);
            session.start(activity);
        });
    }

    /** Ends the active queue session, if any (speaker keeps playing the current track). */
    public static void stop() {
        if (session != null) {
            session.disposables.clear();
            session = null;
        }
    }

    /** @return titles of the active queue for display, or null if no queue session. */
    @Nullable
    public static List<String> queueTitles() {
        if (session == null) {
            return null;
        }
        final List<String> titles = new ArrayList<>(session.items.size());
        for (final PlayQueueItem item : session.items) {
            titles.add(item.getTitle());
        }
        return titles;
    }

    /** @return index of the item currently playing, or -1 if no queue session. */
    public static int queueIndex() {
        return session == null ? -1 : session.currentIndex;
    }

    /** Jumps the active queue session to the given item (no-op without a session). */
    public static void skipTo(final int index) {
        if (session != null) {
            session.skipTo(index);
        }
    }

    public static void next() {
        if (session != null) {
            session.skipTo(session.currentIndex + 1);
        }
    }

    public static void previous() {
        if (session != null) {
            session.skipTo(session.currentIndex - 1);
        }
    }

    private interface OnPrepared {
        void ready(int index, StreamInfo info, String url, String mimeType);
    }

    private static final class Session {
        private final Context appContext;
        private final SonosDevice device;
        private final List<PlayQueueItem> items;
        private final CompositeDisposable disposables = new CompositeDisposable();

        private int currentIndex;
        private String currentUrl;
        private String nextUrl;
        private StreamInfo nextInfo;
        private int nextIndex;
        private int stoppedPolls;
        /** Bumped on skip; stale prepare callbacks (e.g. a superseded download) bail out. */
        private int generation;

        Session(final Context appContext, final SonosDevice device,
                final List<PlayQueueItem> items) {
            this.appContext = appContext;
            this.device = device;
            this.items = items;
        }

        void start(final Activity activity) {
            prepare(0, false, (index, info, url, mimeType) -> {
                currentIndex = index;
                currentUrl = url;
                SonosPlayer.persistLast(appContext, device, info);
                soap(() -> device.playUri(url, info.getName(), info.getThumbnailUrl(),
                        info.getDuration(), mimeType), () -> {
                    Toast.makeText(appContext,
                            appContext.getString(R.string.sonos_playing_toast,
                                    device.getRoomName()),
                            Toast.LENGTH_SHORT).show();
                    if (!activity.isFinishing() && !activity.isDestroyed()) {
                        activity.startActivity(
                                new Intent(activity, SonosControlActivity.class));
                    }
                    queueNext(index + 1);
                    startPolling();
                });
            }, () -> Toast.makeText(appContext, R.string.sonos_no_compatible_stream,
                    Toast.LENGTH_LONG).show());
        }

        /**
         * Resolves item {@code index} to a playable URL; skips unplayable items;
         * {@code onExhausted} fires when the end of the queue is reached.
         */
        private void prepare(final int index, final boolean quiet, final OnPrepared onReady,
                             final Runnable onExhausted) {
            final int gen = generation;
            if (index >= items.size()) {
                onExhausted.run();
                return;
            }
            final PlayQueueItem item = items.get(index);
            final Runnable skip = () -> {
                if (gen != generation) {
                    return;
                }
                Log.w(TAG, "skipping unplayable queue item: " + item.getUrl());
                prepare(index + 1, quiet, onReady, onExhausted);
            };
            disposables.add(ExtractorHelper
                    .getStreamInfo(item.getServiceId(), item.getUrl(), false)
                    .subscribeOn(Schedulers.io())
                    .observeOn(AndroidSchedulers.mainThread())
                    .subscribe(info -> {
                        if (gen != generation) {
                            return;
                        }
                        SonosPlayer.resolve(appContext, info, quiet,
                                (url, mimeType) -> {
                                    if (gen == generation) {
                                        onReady.ready(index, info, url, mimeType);
                                    }
                                },
                                throwable -> skip.run());
                    }, throwable -> skip.run()));
        }

        /** Jumps to item {@code index}: resolve it, play it, re-queue the following one. */
        void skipTo(final int index) {
            if (index < 0 || index >= items.size()) {
                return;
            }
            generation++;
            nextUrl = null;
            stoppedPolls = 0;
            final String oldUrl = currentUrl;
            prepare(index, false, (i, info, url, mimeType) -> {
                currentIndex = i;
                currentUrl = url;
                SonosPlayer.persistLast(appContext, device, info);
                soap(() -> device.playUri(url, info.getName(), info.getThumbnailUrl(),
                        info.getDuration(), mimeType), () -> {
                    if (oldUrl != null && !oldUrl.equals(url)) {
                        SonosStreamService.drop(oldUrl);
                    }
                    // ponytail: the speaker may briefly keep the previously queued next
                    // track until this overwrites it — harmless unless the new track
                    // ends within the download time of its successor
                    queueNext(i + 1);
                });
            }, () -> Toast.makeText(appContext, R.string.sonos_no_compatible_stream,
                    Toast.LENGTH_LONG).show());
        }

        private void queueNext(final int fromIndex) {
            prepare(fromIndex, true, (index, info, url, mimeType) -> soap(
                    () -> device.setNextUri(url, info.getName(), info.getThumbnailUrl(),
                            info.getDuration(), mimeType),
                    () -> {
                        nextIndex = index;
                        nextInfo = info;
                        nextUrl = url;
                    }), () -> nextUrl = null);
        }

        private void startPolling() {
            disposables.add(Flowable.interval(5, 5, TimeUnit.SECONDS)
                    .onBackpressureDrop()
                    .observeOn(Schedulers.io(), false, 1)
                    .map(tick -> {
                        try {
                            return new String[]{device.getTransportState(),
                                    device.getTrackUri()};
                        } catch (final Exception e) {
                            return new String[]{null, null}; // network blip, retry next tick
                        }
                    })
                    .observeOn(AndroidSchedulers.mainThread())
                    .subscribe(status -> onPoll(status[0], status[1])));
        }

        private void onPoll(final String state, final String trackUri) {
            if (state == null) {
                return;
            }
            if (nextUrl != null && nextUrl.equals(trackUri)) {
                // speaker advanced to the queued track
                SonosStreamService.drop(currentUrl);
                currentIndex = nextIndex;
                currentUrl = nextUrl;
                nextUrl = null;
                SonosPlayer.persistLast(appContext, device, nextInfo);
                queueNext(nextIndex + 1);
                stoppedPolls = 0;
                return;
            }
            // two consecutive STOPPED polls = end of queue or user stop → tear down
            // (a single one could be a mid-advance race)
            if ("STOPPED".equals(state)) {
                if (++stoppedPolls >= 2) {
                    teardown();
                }
            } else {
                stoppedPolls = 0;
            }
        }

        private void teardown() {
            disposables.clear();
            SonosStreamService.shutdown(appContext);
            if (session == this) {
                session = null;
            }
        }

        private void soap(final Action action, final Runnable onDone) {
            final int gen = generation;
            disposables.add(Completable.fromAction(action)
                    .subscribeOn(Schedulers.io())
                    .observeOn(AndroidSchedulers.mainThread())
                    .subscribe(() -> {
                        if (gen == generation) {
                            onDone.run();
                        }
                    }, throwable -> Toast.makeText(appContext,
                            appContext.getString(R.string.sonos_error,
                                    String.valueOf(throwable.getMessage())),
                            Toast.LENGTH_LONG).show()));
        }
    }
}
