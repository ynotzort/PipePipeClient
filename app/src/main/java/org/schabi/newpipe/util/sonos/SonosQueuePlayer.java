package org.schabi.newpipe.util.sonos;

import android.app.Activity;
import android.content.Context;
import android.content.Intent;
import android.net.Uri;
import android.os.Handler;
import android.os.Looper;
import android.util.Log;
import android.widget.Toast;

import androidx.annotation.Nullable;
import androidx.appcompat.app.AlertDialog;

import org.schabi.newpipe.R;
import org.schabi.newpipe.extractor.stream.StreamInfo;
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

        /** Duration in seconds for the queue list, 0 if unknown before preparing. */
        long durationSeconds();

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
        /** Serialized chapter markers (see {@link SonosPlayer#chaptersOf}), "" if none. */
        final String chapters;

        Prepared(final String url, final String mimeType, final String title,
                 final long durationSeconds, @Nullable final String thumbnailUrl,
                 final String chapters) {
            this.url = url;
            this.mimeType = mimeType;
            this.title = title;
            this.durationSeconds = durationSeconds;
            this.thumbnailUrl = thumbnailUrl;
            this.chapters = chapters;
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

    /** @return per-item durations in seconds (0 = unknown), or null if no session. */
    @Nullable
    public static List<Long> queueDurations() {
        if (session == null) {
            return null;
        }
        final List<Long> durations = new ArrayList<>(session.items.size());
        for (final Item item : session.items) {
            durations.add(item.durationSeconds());
        }
        return durations;
    }

    /** @return index of the item currently playing, or -1 if no queue session. */
    public static int queueIndex() {
        return session == null ? -1 : session.items.indexOf(session.currentItem);
    }

    /** Bumped on every queue edit; the control screen rebuilds its list on change. */
    public static int queueVersion() {
        return session == null ? -1 : session.version;
    }

    public static boolean hasSession() {
        return session != null;
    }

    /** Jumps the active queue session to the given item (no-op without a session). */
    public static void skipTo(final int index) {
        if (session != null) {
            session.skipTo(index);
        }
    }

    public static void next() {
        if (session != null) {
            session.skipTo(queueIndex() + 1);
        }
    }

    public static void previous() {
        if (session != null) {
            session.skipTo(queueIndex() - 1);
        }
    }

    /**
     * "Play on Sonos" for a single stream: with no queue running it starts a
     * fresh 1-item queue; with one it asks Play now / Add to queue.
     */
    public static void playOrEnqueue(final Activity activity, final StreamInfo info) {
        playOrEnqueueItem(activity, new StreamItem(info.getServiceId(), info.getUrl(),
                info.getName(), info.getDuration()));
    }

    /** Same, from a long-press context menu's {@link PlayQueueItem}. */
    public static void playOrEnqueue(final Activity activity, final PlayQueueItem queueItem) {
        playOrEnqueueItem(activity, new StreamItem(queueItem));
    }

    private static void playOrEnqueueItem(final Activity activity, final Item item) {
        if (session == null) {
            enqueueItem(activity, item);
            return;
        }
        new AlertDialog.Builder(activity)
                .setItems(new CharSequence[]{
                        activity.getString(R.string.sonos_play_now),
                        activity.getString(R.string.sonos_add_to_queue)},
                        (dialog, which) -> {
                            if (which == 0) {
                                stop(); // fresh 1-item queue replaces playback
                            }
                            enqueueItem(activity, item);
                        })
                .show();
    }

    /** Appends to the running queue, or starts a new queue playing the item. */
    private static void enqueueItem(final Activity activity, final Item item) {
        if (session == null) {
            final List<Item> single = new ArrayList<>();
            single.add(item);
            playItems(activity, single, null);
        } else {
            session.add(item);
            Toast.makeText(activity.getApplicationContext(),
                    R.string.sonos_added_to_queue, Toast.LENGTH_SHORT).show();
        }
    }

    /**
     * Removes the queue item at {@code index} and returns it (for undo);
     * null if it's the playing one or there is no session.
     */
    @Nullable
    public static Item removeAt(final int index) {
        return session == null ? null : session.removeAt(index);
    }

    /** Undo of {@link #removeAt}: puts the item back at (or near) its old spot. */
    public static void restore(final int index, final Item item) {
        if (session != null) {
            session.addAt(index, item);
        }
    }

    /** Moves the queue item at {@code from} to position {@code to}. */
    public static void move(final int from, final int to) {
        if (session != null) {
            session.move(from, to);
        }
    }

    /** A PipePipe stream: extract StreamInfo, then direct URL or download+serve. */
    private static final class StreamItem implements Item {
        private final int serviceId;
        private final String url;
        private final String title;
        private final long durationSeconds;

        StreamItem(final PlayQueueItem queueItem) {
            this(queueItem.getServiceId(), queueItem.getUrl(),
                    queueItem.getTitle(), queueItem.getDuration());
        }

        StreamItem(final int serviceId, final String url, final String title,
                   final long durationSeconds) {
            this.serviceId = serviceId;
            this.url = url;
            this.title = title;
            this.durationSeconds = durationSeconds;
        }

        @Override
        public String title() {
            return title;
        }

        @Override
        public long durationSeconds() {
            return durationSeconds;
        }

        @Override
        public Disposable prepare(final Context appContext, final boolean quiet,
                                  final Consumer<Prepared> onReady,
                                  final Consumer<Throwable> onError) {
            return ExtractorHelper
                    .getStreamInfo(serviceId, url, false)
                    .subscribeOn(Schedulers.io())
                    .observeOn(AndroidSchedulers.mainThread())
                    .subscribe(info -> SonosPlayer.resolve(appContext, info, quiet,
                                    (url, mimeType) -> onReady.accept(new Prepared(url,
                                            mimeType, info.getName(), info.getDuration(),
                                            info.getThumbnailUrl(),
                                            SonosPlayer.chaptersOf(info))),
                                    onError::accept),
                            onError::accept);
        }
    }

    /** A local audio file (content:// or file://): copy into cache, then serve. */
    static final class LocalFileItem implements Item {
        private final Uri uri;
        private final String displayTitle;
        private final long durationSeconds;

        LocalFileItem(final Uri uri, final String displayTitle, final long durationSeconds) {
            this.uri = uri;
            this.displayTitle = displayTitle;
            this.durationSeconds = durationSeconds;
        }

        @Override
        public String title() {
            return displayTitle;
        }

        @Override
        public long durationSeconds() {
            return durationSeconds;
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
                                            null, "")),
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
        public long durationSeconds() {
            return 0;
        }

        @Override
        public Disposable prepare(final Context appContext, final boolean quiet,
                                  final Consumer<Prepared> onReady,
                                  final Consumer<Throwable> onError) {
            onReady.accept(new Prepared(url, mimeFor(url), displayTitle, 0, null, ""));
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
        void ready(Item item, Prepared prepared);
    }

    private static final class Session {
        private final Context appContext;
        private final SonosDevice device;
        /** Owned mutable copy; edited in place by add/removeAt/move. */
        private final List<Item> items;
        private final CompositeDisposable disposables = new CompositeDisposable();

        // current/next are tracked by Item reference, not index — queue edits
        // shift indices under a playing session
        private Item currentItem;
        private String currentUrl;
        private Item nextItem;
        private String nextUrl;
        private Prepared nextPrepared;
        /** Last item a queueNext was issued for (dedups syncNext re-queues). */
        private Item wantedNext;
        private final Handler syncHandler = new Handler(Looper.getMainLooper());
        private int stoppedPolls;
        /** True while a skip target is being prepared — a STOPPED poll then isn't
         *  end-of-queue (the current track may end while the download runs). */
        private boolean preparing;
        /** Bumped on skip; stale prepare callbacks (e.g. a superseded download) bail out. */
        private int generation;
        /** Bumped on every queue edit; the control screen rebuilds on change. */
        private int version;

        Session(final Context appContext, final SonosDevice device, final List<Item> items) {
            this.appContext = appContext;
            this.device = device;
            this.items = new ArrayList<>(items);
        }

        void start(final Activity activity) {
            prepare(0, false, (item, prepared) -> {
                currentItem = item;
                currentUrl = prepared.url;
                SonosPlayer.persistLast(appContext, device, prepared.title,
                        prepared.durationSeconds, prepared.chapters);
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
                    queueNext(items.indexOf(item) + 1);
                    startPolling();
                });
            }, () -> Toast.makeText(appContext, R.string.sonos_no_compatible_stream,
                    Toast.LENGTH_LONG).show());
        }

        /**
         * Prepares the item at {@code index}; skips unpreparable items;
         * {@code onExhausted} fires when the end of the queue is reached.
         */
        private void prepare(final int index, final boolean quiet, final OnPrepared onReady,
                             final Runnable onExhausted) {
            final int gen = generation;
            if (index < 0 || index >= items.size()) {
                onExhausted.run();
                return;
            }
            final Item item = items.get(index);
            final Runnable skip = () -> {
                if (gen != generation) {
                    return;
                }
                Log.w(TAG, "skipping unplayable queue item: " + item.title());
                // re-derive the position: the queue may have been edited meanwhile
                final int at = items.indexOf(item);
                prepare(at < 0 ? items.size() : at + 1, quiet, onReady, onExhausted);
            };
            disposables.add(item.prepare(appContext, quiet,
                    prepared -> {
                        if (gen == generation) {
                            onReady.ready(item, prepared);
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
            if (nextUrl != null) {
                // un-queue the speaker's next NOW — if the current track ends while
                // the skip target downloads, it must not play the superseded track
                clearSpeakerNext();
            }
            wantedNext = null;
            stoppedPolls = 0;
            preparing = true;
            final String oldUrl = currentUrl;
            prepare(index, false, (item, prepared) -> {
                currentItem = item;
                currentUrl = prepared.url;
                SonosPlayer.persistLast(appContext, device, prepared.title,
                        prepared.durationSeconds, prepared.chapters);
                soap(() -> device.playUri(prepared.url, prepared.title,
                        prepared.thumbnailUrl, prepared.durationSeconds,
                        prepared.mimeType), () -> {
                    preparing = false;
                    if (oldUrl != null && !oldUrl.equals(prepared.url)) {
                        SonosStreamService.drop(oldUrl);
                    }
                    queueNext(items.indexOf(item) + 1);
                });
            }, () -> {
                preparing = false;
                Toast.makeText(appContext, R.string.sonos_no_compatible_stream,
                        Toast.LENGTH_LONG).show();
            });
        }

        void add(final Item item) {
            items.add(item);
            version++;
            syncNext();
        }

        void addAt(final int index, final Item item) {
            items.add(Math.max(0, Math.min(index, items.size())), item);
            version++;
            syncNext();
        }

        @Nullable
        Item removeAt(final int index) {
            if (index < 0 || index >= items.size() || items.get(index) == currentItem) {
                return null; // never the playing row
            }
            final Item removed = items.remove(index);
            version++;
            syncNext();
            return removed;
        }

        void move(final int from, final int to) {
            if (from < 0 || from >= items.size() || to < 0 || to >= items.size()
                    || from == to) {
                return;
            }
            items.add(to, items.remove(from));
            version++;
            syncNext();
        }

        /**
         * Debounced {@link #doSyncNext()}: rapid edits (each swap of a drag pass
         * lands here) coalesce into one re-queue check after the queue settles.
         */
        private void syncNext() {
            syncHandler.removeCallbacksAndMessages(null);
            syncHandler.postDelayed(() -> {
                if (session == this) {
                    doSyncNext();
                }
            }, 600);
        }

        /** After a queue edit: if a different item now follows the playing one, re-queue. */
        private void doSyncNext() {
            final int after = items.indexOf(currentItem) + 1;
            final Item desired = after > 0 && after < items.size() ? items.get(after) : null;
            if (desired == wantedNext) {
                // ponytail: dedup by intent only — overlapping re-queues race
                // last-write-wins on the speaker, self-heals on the next advance/skip
                return;
            }
            // Drop the stale queued next on the speaker RIGHT AWAY: its item may
            // just have been deleted, and preparing the replacement can take a
            // whole download — a track end in between would play the deleted one.
            if (nextUrl != null) {
                clearSpeakerNext();
            }
            if (desired == null) {
                wantedNext = null;
                return;
            }
            queueNext(after);
        }

        /** Best-effort immediate un-queue of the speaker's next track. */
        private void clearSpeakerNext() {
            nextItem = null;
            nextUrl = null;
            disposables.add(Completable.fromAction(device::clearNext)
                    .subscribeOn(Schedulers.io())
                    .subscribe(() -> { }, throwable -> { }));
        }

        private void queueNext(final int fromIndex) {
            wantedNext = fromIndex >= 0 && fromIndex < items.size()
                    ? items.get(fromIndex) : null;
            prepare(fromIndex, true, (item, prepared) -> soap(
                    () -> device.setNextUri(prepared.url, prepared.title,
                            prepared.thumbnailUrl, prepared.durationSeconds,
                            prepared.mimeType),
                    () -> {
                        nextItem = item;
                        nextPrepared = prepared;
                        nextUrl = prepared.url;
                    }), () -> {
                nextItem = null;
                nextUrl = null;
            });
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
                currentItem = nextItem;
                currentUrl = nextUrl;
                nextItem = null;
                nextUrl = null;
                SonosPlayer.persistLast(appContext, device, nextPrepared.title,
                        nextPrepared.durationSeconds, nextPrepared.chapters);
                queueNext(items.indexOf(currentItem) + 1);
                stoppedPolls = 0;
                return;
            }
            // two consecutive STOPPED polls = end of queue or user stop → tear down
            // (a single one could be a mid-advance race)
            if (preparing) {
                return; // mid-skip silence is expected, not end-of-queue
            }
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
            // Deliberately NOT shutting down the stream service: after a stop the
            // speaker keeps the track URI, and Play re-fetches it — killing the
            // server here made play-after-stop silently do nothing. The service's
            // own auto-stop timer (duration + slack) reclaims it.
            // ponytail: play-after-stop still dies once that timer fires; re-serving
            // the persisted last file from the control screen would fix that.
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
