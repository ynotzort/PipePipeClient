package org.schabi.newpipe.util.sonos;

import android.app.Activity;
import android.content.Context;
import android.content.Intent;
import android.net.Uri;
import android.util.Log;
import android.widget.Toast;

import androidx.annotation.Nullable;

import org.schabi.newpipe.R;
import org.schabi.newpipe.player.playqueue.PlayQueueItem;
import org.schabi.newpipe.util.ExtractorHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.reactivex.rxjava3.android.schedulers.AndroidSchedulers;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Action;
import io.reactivex.rxjava3.schedulers.Schedulers;

/**
 * Plays a queue of items on a Sonos speaker, advancing track to track.
 *
 * <p>Items are anything that can {@link Item#prepare} itself into a playable URL:
 * PipePipe streams (extract → direct URL or download-to-cache + serve), local
 * files (copy-to-cache + serve, e.g. from an m3u playlist) or plain http URLs.
 * The first item plays via {@code SetAVTransportURI}; the following one is
 * prefetched and queued with {@code SetNextAVTransportURI} so the speaker
 * auto-advances near-gapless. A 5 s poll of {@code GetPositionInfo} detects the
 * advance (TrackURI switched to the queued URL), retires the played file and
 * queues the item after next. Items that fail to prepare are skipped.
 * One session at a time.</p>
 */
public final class SonosQueuePlayer {
    private static final String TAG = "SonosQueuePlayer";
    @SuppressWarnings("StaticFieldLeak") // holds the application context only
    private static Session session;

    private SonosQueuePlayer() {
    }

    /** A queue entry that can resolve itself into something the speaker can fetch. */
    public interface Item {
        /** Display title for the queue list (available before preparing). */
        String title();

        /**
         * Resolves to a playable URL (may download/copy first); calls exactly one
         * of the callbacks on the main thread.
         */
        Disposable prepare(Context appContext, boolean quiet,
                           Consumer<Prepared> onReady, Consumer<Throwable> onError);
    }

    /** A prepared item: everything needed for SetAVTransportURI + DIDL. */
    public static final class Prepared {
        final String url;
        final String mimeType;
        final String title;
        final long durationSeconds;
        @Nullable
        final String thumbnailUrl;

        Prepared(final String url, final String mimeType, final String title,
                 final long durationSeconds, @Nullable final String thumbnailUrl) {
            this.url = url;
            this.mimeType = mimeType;
            this.title = title;
            this.durationSeconds = durationSeconds;
            this.thumbnailUrl = thumbnailUrl;
        }
    }

    /** Plays a PipePipe play queue (local/remote playlist fragments). */
    public static void play(final Activity activity, final List<PlayQueueItem> queueItems) {
        final List<Item> items = new ArrayList<>(queueItems.size());
        for (final PlayQueueItem queueItem : queueItems) {
            items.add(new StreamItem(queueItem));
        }
        playItems(activity, items, null);
    }

    /** Plays arbitrary items (e.g. parsed from an m3u playlist). */
    public static void playItems(final Activity activity, final List<Item> items,
                                 @Nullable final Runnable onSpeakerChosen) {
        if (items.isEmpty()) {
            return;
        }
        SonosPlayer.chooseSpeaker(activity, false, onSpeakerChosen, device -> {
            stop();
            session = new Session(activity.getApplicationContext(), device, items);
            session.start(activity);
        });
    }

    /** Ends the active queue session, if any (speaker keeps playing the current track). */
    public static void stop() {
        if (session != null) {
            session.generation++; // invalidate in-flight prepare callbacks
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
        for (final Item item : session.items) {
            titles.add(item.title());
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

    /** A PipePipe stream: extract StreamInfo, then direct URL or download+serve. */
    private static final class StreamItem implements Item {
        private final PlayQueueItem queueItem;

        StreamItem(final PlayQueueItem queueItem) {
            this.queueItem = queueItem;
        }

        @Override
        public String title() {
            return queueItem.getTitle();
        }

        @Override
        public Disposable prepare(final Context appContext, final boolean quiet,
                                  final Consumer<Prepared> onReady,
                                  final Consumer<Throwable> onError) {
            return ExtractorHelper
                    .getStreamInfo(queueItem.getServiceId(), queueItem.getUrl(), false)
                    .subscribeOn(Schedulers.io())
                    .observeOn(AndroidSchedulers.mainThread())
                    .subscribe(info -> SonosPlayer.resolve(appContext, info, quiet,
                                    (url, mimeType) -> onReady.accept(new Prepared(url,
                                            mimeType, info.getName(), info.getDuration(),
                                            info.getThumbnailUrl())),
                                    onError::accept),
                            onError::accept);
        }
    }

    /** A local audio file (content:// or file://): copy into cache, then serve. */
    static final class LocalFileItem implements Item {
        private final Uri uri;
        private final String displayTitle;

        LocalFileItem(final Uri uri, final String displayTitle) {
            this.uri = uri;
            this.displayTitle = displayTitle;
        }

        @Override
        public String title() {
            return displayTitle;
        }

        @Override
        public Disposable prepare(final Context appContext, final boolean quiet,
                                  final Consumer<Prepared> onReady,
                                  final Consumer<Throwable> onError) {
            return Single.fromCallable(() -> SonosPlayer.importLocalFile(appContext, uri))
                    .subscribeOn(Schedulers.io())
                    .observeOn(AndroidSchedulers.mainThread())
                    .subscribe(local -> SonosPlayer.serve(appContext, local.title,
                                    local.durationSeconds, local.mimeType, local.file,
                                    (url, mimeType) -> onReady.accept(new Prepared(url,
                                            mimeType, local.title, local.durationSeconds,
                                            null)),
                                    onError::accept),
                            onError::accept);
        }
    }

    /** A plain http(s) URL (e.g. a web radio entry in an m3u): pass straight through. */
    static final class HttpItem implements Item {
        private final String url;
        private final String displayTitle;

        HttpItem(final String url, final String displayTitle) {
            this.url = url;
            this.displayTitle = displayTitle;
        }

        @Override
        public String title() {
            return displayTitle;
        }

        @Override
        public Disposable prepare(final Context appContext, final boolean quiet,
                                  final Consumer<Prepared> onReady,
                                  final Consumer<Throwable> onError) {
            onReady.accept(new Prepared(url, mimeFor(url), displayTitle, 0, null));
            return Disposable.disposed();
        }

        private static String mimeFor(final String url) {
            final String lower = url.toLowerCase(java.util.Locale.US);
            if (lower.contains(".m4a") || lower.contains(".mp4") || lower.contains(".aac")) {
                return "audio/mp4";
            }
            if (lower.contains(".flac")) {
                return "audio/flac";
            }
            if (lower.contains(".ogg")) {
                return "audio/ogg";
            }
            return "audio/mpeg";
        }
    }

    private interface OnPrepared {
        void ready(int index, Prepared prepared);
    }

    private static final class Session {
        private final Context appContext;
        private final SonosDevice device;
        private final List<Item> items;
        private final CompositeDisposable disposables = new CompositeDisposable();

        private int currentIndex;
        private String currentUrl;
        private String nextUrl;
        private Prepared nextPrepared;
        private int nextIndex;
        private int stoppedPolls;
        /** Bumped on skip; stale prepare callbacks (e.g. a superseded download) bail out. */
        private int generation;

        Session(final Context appContext, final SonosDevice device, final List<Item> items) {
            this.appContext = appContext;
            this.device = device;
            this.items = items;
        }

        void start(final Activity activity) {
            prepare(0, false, (index, prepared) -> {
                currentIndex = index;
                currentUrl = prepared.url;
                SonosPlayer.persistLast(appContext, device, prepared.title,
                        prepared.durationSeconds);
                soap(() -> device.playUri(prepared.url, prepared.title,
                        prepared.thumbnailUrl, prepared.durationSeconds,
                        prepared.mimeType), () -> {
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
         * Prepares item {@code index}; skips unpreparable items;
         * {@code onExhausted} fires when the end of the queue is reached.
         */
        private void prepare(final int index, final boolean quiet, final OnPrepared onReady,
                             final Runnable onExhausted) {
            final int gen = generation;
            if (index >= items.size()) {
                onExhausted.run();
                return;
            }
            final Item item = items.get(index);
            final Runnable skip = () -> {
                if (gen != generation) {
                    return;
                }
                Log.w(TAG, "skipping unplayable queue item: " + item.title());
                prepare(index + 1, quiet, onReady, onExhausted);
            };
            disposables.add(item.prepare(appContext, quiet,
                    prepared -> {
                        if (gen == generation) {
                            onReady.ready(index, prepared);
                        }
                    },
                    throwable -> skip.run()));
        }

        /** Jumps to item {@code index}: prepare it, play it, re-queue the following one. */
        void skipTo(final int index) {
            if (index < 0 || index >= items.size()) {
                return;
            }
            generation++;
            nextUrl = null;
            stoppedPolls = 0;
            final String oldUrl = currentUrl;
            prepare(index, false, (i, prepared) -> {
                currentIndex = i;
                currentUrl = prepared.url;
                SonosPlayer.persistLast(appContext, device, prepared.title,
                        prepared.durationSeconds);
                soap(() -> device.playUri(prepared.url, prepared.title,
                        prepared.thumbnailUrl, prepared.durationSeconds,
                        prepared.mimeType), () -> {
                    if (oldUrl != null && !oldUrl.equals(prepared.url)) {
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
            prepare(fromIndex, true, (index, prepared) -> soap(
                    () -> device.setNextUri(prepared.url, prepared.title,
                            prepared.thumbnailUrl, prepared.durationSeconds,
                            prepared.mimeType),
                    () -> {
                        nextIndex = index;
                        nextPrepared = prepared;
                        nextUrl = prepared.url;
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
                SonosPlayer.persistLast(appContext, device, nextPrepared.title,
                        nextPrepared.durationSeconds);
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
            generation++;
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
