package org.schabi.newpipe.util.sonos;

import android.content.SharedPreferences;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.drawable.ColorDrawable;
import android.graphics.drawable.Drawable;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.SeekBar;
import android.widget.TextView;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.appcompat.app.AlertDialog;
import androidx.appcompat.app.AppCompatActivity;
import androidx.appcompat.content.res.AppCompatResources;
import androidx.preference.PreferenceManager;
import androidx.recyclerview.widget.ItemTouchHelper;
import androidx.recyclerview.widget.LinearLayoutManager;
import androidx.recyclerview.widget.RecyclerView;

import com.google.android.material.snackbar.Snackbar;

import org.schabi.newpipe.R;
import org.schabi.newpipe.util.Localization;
import org.schabi.newpipe.util.ThemeHelper;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import io.reactivex.rxjava3.android.schedulers.AndroidSchedulers;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.schedulers.Schedulers;

/**
 * Shows what the last-used Sonos speaker is doing, with transport controls,
 * seeking, volume and cache cleanup. State is polled every 2 s while visible.
 */
public final class SonosControlActivity extends AppCompatActivity {
    private static final int MENU_CLEAR_CACHE = Menu.FIRST;
    private static final int MENU_CACHE_LIMIT = Menu.FIRST + 1;
    private static final long[] CACHE_MB_CHOICES = {256, 512, 1024, 2048, 4096};

    private final CompositeDisposable disposables = new CompositeDisposable();
    private SharedPreferences prefs;
    private SonosDevice device;
    private TextView titleView;
    private TextView stateView;
    private TextView timeLabel;
    private SeekBar positionBar;
    private SeekBar volumeBar;
    private RecyclerView queueList;
    private final List<String> queueRows = new ArrayList<>();
    private final QueueAdapter queueAdapter = new QueueAdapter();
    private Button prevButton;
    private Button nextButton;
    private Button chaptersButton;
    private String chapterData = "";
    private boolean draggingPosition;
    private boolean draggingVolume;
    private long knownDuration;
    private int shownQueueIndex = -2;
    private int shownQueueVersion = -2;

    @Override
    protected void onCreate(@Nullable final Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        Localization.assureCorrectAppLanguage(this);
        ThemeHelper.setTheme(this);
        setContentView(R.layout.activity_sonos_control);
        setSupportActionBar(findViewById(R.id.sonos_toolbar));
        setTitle(R.string.play_on_sonos_title);

        prefs = PreferenceManager.getDefaultSharedPreferences(this);
        final String ip = prefs.getString(SonosPlayer.PREF_LAST_IP, null);
        if (ip == null) {
            Toast.makeText(this, R.string.sonos_no_speaker_yet, Toast.LENGTH_LONG).show();
            finish();
            return;
        }
        device = new SonosDevice(ip, prefs.getString(SonosPlayer.PREF_LAST_NAME, ip));
        // Sonos reports TrackDuration from the file's (sometimes wrong) moov header;
        // prefer the real duration we knew when we started playback.
        knownDuration = prefs.getLong(SonosPlayer.PREF_LAST_DURATION, 0);

        ((TextView) findViewById(R.id.sonos_room_name)).setText(device.getRoomName());
        titleView = findViewById(R.id.sonos_track_title);
        titleView.setText(prefs.getString(SonosPlayer.PREF_LAST_TITLE, ""));
        stateView = findViewById(R.id.sonos_state);
        timeLabel = findViewById(R.id.sonos_time_label);
        positionBar = findViewById(R.id.sonos_position_bar);
        volumeBar = findViewById(R.id.sonos_volume_bar);
        queueList = findViewById(R.id.sonos_queue_list);

        findViewById(R.id.sonos_btn_play).setOnClickListener(v -> run(device::play));
        findViewById(R.id.sonos_btn_pause).setOnClickListener(v -> run(device::pause));
        findViewById(R.id.sonos_btn_stop).setOnClickListener(v -> run(device::stop));
        prevButton = findViewById(R.id.sonos_btn_prev);
        nextButton = findViewById(R.id.sonos_btn_next);
        prevButton.setOnClickListener(v -> SonosQueuePlayer.previous());
        nextButton.setOnClickListener(v -> SonosQueuePlayer.next());
        chaptersButton = findViewById(R.id.sonos_btn_chapters);
        chaptersButton.setOnClickListener(v -> showChaptersDialog());
        queueList.setLayoutManager(new LinearLayoutManager(this));
        queueList.setAdapter(queueAdapter);
        attachQueueTouchHelper();

        positionBar.setOnSeekBarChangeListener(new SeekBar.OnSeekBarChangeListener() {
            @Override
            public void onProgressChanged(final SeekBar seekBar, final int progress,
                                          final boolean fromUser) {
                if (fromUser) {
                    timeLabel.setText(formatTimes(progress, knownDuration));
                }
            }

            @Override
            public void onStartTrackingTouch(final SeekBar seekBar) {
                draggingPosition = true;
            }

            @Override
            public void onStopTrackingTouch(final SeekBar seekBar) {
                draggingPosition = false;
                run(() -> device.seek(seekBar.getProgress()));
            }
        });
        volumeBar.setOnSeekBarChangeListener(new SeekBar.OnSeekBarChangeListener() {
            @Override
            public void onProgressChanged(final SeekBar seekBar, final int progress,
                                          final boolean fromUser) {
            }

            @Override
            public void onStartTrackingTouch(final SeekBar seekBar) {
                draggingVolume = true;
            }

            @Override
            public void onStopTrackingTouch(final SeekBar seekBar) {
                draggingVolume = false;
                run(() -> device.setVolume(seekBar.getProgress()));
            }
        });

    }

    @Override
    public boolean onCreateOptionsMenu(final Menu menu) {
        menu.add(Menu.NONE, MENU_CLEAR_CACHE, 0, "");
        menu.add(Menu.NONE, MENU_CACHE_LIMIT, 1, R.string.sonos_cache_limit);
        return true;
    }

    @Override
    public boolean onPrepareOptionsMenu(final Menu menu) {
        long bytes = 0;
        final File[] files = cacheDir().listFiles();
        if (files != null) {
            for (final File f : files) {
                bytes += f.length();
            }
        }
        menu.findItem(MENU_CLEAR_CACHE).setTitle(getString(R.string.sonos_clear_cache,
                String.format(Locale.getDefault(), "%.1f MB", bytes / 1048576.0)));
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(final MenuItem item) {
        if (item.getItemId() == MENU_CLEAR_CACHE) {
            final File[] files = cacheDir().listFiles();
            if (files != null) {
                for (final File f : files) {
                    // don't delete a file the speaker is currently streaming
                    final File base = f.getName().endsWith(".done")
                            ? new File(f.getPath().substring(0, f.getPath().length() - 5))
                            : f;
                    if (!SonosStreamService.isServing(base)) {
                        //noinspection ResultOfMethodCallIgnored
                        f.delete();
                    }
                }
            }
            Toast.makeText(this, R.string.sonos_cache_cleared, Toast.LENGTH_SHORT).show();
            return true;
        } else if (item.getItemId() == MENU_CACHE_LIMIT) {
            showCacheLimitDialog();
            return true;
        }
        return super.onOptionsItemSelected(item);
    }

    private void showCacheLimitDialog() {
        final long current = prefs.getLong(SonosPlayer.PREF_CACHE_MAX_MB,
                SonosPlayer.DEFAULT_CACHE_MAX_MB);
        final String[] labels = new String[CACHE_MB_CHOICES.length];
        int checked = -1;
        for (int i = 0; i < CACHE_MB_CHOICES.length; i++) {
            labels[i] = CACHE_MB_CHOICES[i] >= 1024
                    ? (CACHE_MB_CHOICES[i] / 1024) + " GB" : CACHE_MB_CHOICES[i] + " MB";
            if (CACHE_MB_CHOICES[i] == current) {
                checked = i;
            }
        }
        new AlertDialog.Builder(this)
                .setTitle(R.string.sonos_cache_limit)
                .setSingleChoiceItems(labels, checked, (dialog, which) -> {
                    prefs.edit().putLong(SonosPlayer.PREF_CACHE_MAX_MB,
                            CACHE_MB_CHOICES[which]).apply();
                    dialog.dismiss();
                })
                .show();
    }

    @Override
    protected void onStart() {
        super.onStart();
        disposables.add(Flowable.interval(0, 2, TimeUnit.SECONDS)
                .onBackpressureDrop()
                .observeOn(Schedulers.io(), false, 1)
                .map(tick -> new Object[]{
                        device.getTransportState(),
                        device.getPositionInfo(),
                        device.getVolume(),
                })
                .observeOn(AndroidSchedulers.mainThread())
                .subscribe(this::applyStatus, throwable ->
                        stateView.setText(String.valueOf(throwable.getMessage()))));
    }

    @Override
    protected void onStop() {
        super.onStop();
        disposables.clear();
    }

    private void applyStatus(final Object[] status) {
        final String state = (String) status[0];
        final long[] position = (long[]) status[1];
        final int volume = (int) status[2];
        // Re-read per tick: the queue player updates these prefs on track advance.
        titleView.setText(prefs.getString(SonosPlayer.PREF_LAST_TITLE, ""));
        knownDuration = prefs.getLong(SonosPlayer.PREF_LAST_DURATION, 0);
        final boolean live = prefs.getBoolean(SonosPlayer.PREF_LAST_LIVE, false);
        positionBar.setVisibility(live ? View.GONE : View.VISIBLE);
        chapterData = prefs.getString(SonosPlayer.PREF_LAST_CHAPTERS, "");
        chaptersButton.setVisibility(chapterData.isEmpty() || live
                ? View.GONE : View.VISIBLE);
        updateQueue();
        switch (state) {
            case "PLAYING":
            case "TRANSITIONING":
                stateView.setText(R.string.sonos_state_playing);
                break;
            case "PAUSED_PLAYBACK":
                stateView.setText(R.string.sonos_state_paused);
                break;
            case "STOPPED":
                stateView.setText(R.string.sonos_state_stopped);
                break;
            default:
                stateView.setText(state);
        }
        if (live) {
            timeLabel.setText(getString(R.string.duration_live) + " · "
                    + SonosDevice.formatTime(position[0]));
        } else {
            // Prefer the real duration (from StreamInfo); fall back to the speaker's
            // report only if we never stored one.
            if (knownDuration <= 0) {
                knownDuration = position[1];
            }
            positionBar.setMax((int) Math.max(1, knownDuration));
            if (!draggingPosition) {
                positionBar.setProgress((int) Math.min(position[0], knownDuration));
                timeLabel.setText(formatTimes(position[0], knownDuration));
            }
        }
        if (!draggingVolume) {
            volumeBar.setProgress(volume);
        }
    }

    /** One-line queue rows: tap to play. */
    private final class QueueAdapter extends RecyclerView.Adapter<QueueAdapter.Holder> {
        final class Holder extends RecyclerView.ViewHolder {
            private final TextView text;

            Holder(final View view) {
                super(view);
                text = view.findViewById(android.R.id.text1);
                view.setOnClickListener(v -> {
                    final int position = getBindingAdapterPosition();
                    if (position != RecyclerView.NO_POSITION) {
                        SonosQueuePlayer.skipTo(position);
                    }
                });
            }
        }

        @NonNull
        @Override
        public Holder onCreateViewHolder(@NonNull final ViewGroup parent, final int viewType) {
            return new Holder(LayoutInflater.from(parent.getContext())
                    .inflate(android.R.layout.simple_list_item_1, parent, false));
        }

        @Override
        public void onBindViewHolder(@NonNull final Holder holder, final int position) {
            holder.text.setText(queueRows.get(position));
        }

        @Override
        public int getItemCount() {
            return queueRows.size();
        }
    }

    /** Long-press-drag to reorder, swipe to remove (red + bin reveal, undo snackbar). */
    private void attachQueueTouchHelper() {
        final ColorDrawable swipeBackground = new ColorDrawable(0xFFC62828);
        final Drawable deleteIcon =
                AppCompatResources.getDrawable(this, R.drawable.ic_delete);
        if (deleteIcon != null) {
            deleteIcon.setTint(Color.WHITE);
        }
        new ItemTouchHelper(new ItemTouchHelper.SimpleCallback(
                ItemTouchHelper.UP | ItemTouchHelper.DOWN,
                ItemTouchHelper.LEFT | ItemTouchHelper.RIGHT) {
            @Override
            public boolean onMove(@NonNull final RecyclerView recyclerView,
                                  @NonNull final RecyclerView.ViewHolder viewHolder,
                                  @NonNull final RecyclerView.ViewHolder target) {
                final int from = viewHolder.getBindingAdapterPosition();
                final int to = target.getBindingAdapterPosition();
                if (from == RecyclerView.NO_POSITION || to == RecyclerView.NO_POSITION) {
                    return false;
                }
                SonosQueuePlayer.move(from, to);
                queueRows.add(to, queueRows.remove(from));
                queueAdapter.notifyItemMoved(from, to);
                // keep the poll's rebuild check in sync or it would reset the drag
                shownQueueVersion = SonosQueuePlayer.queueVersion();
                return true;
            }

            @Override
            public int getSwipeDirs(@NonNull final RecyclerView recyclerView,
                                    @NonNull final RecyclerView.ViewHolder viewHolder) {
                // the playing row can't be removed — don't tease the red bin
                return viewHolder.getBindingAdapterPosition()
                        == SonosQueuePlayer.queueIndex()
                        ? 0 : super.getSwipeDirs(recyclerView, viewHolder);
            }

            @Override
            public void onSwiped(@NonNull final RecyclerView.ViewHolder viewHolder,
                                 final int direction) {
                final int position = viewHolder.getBindingAdapterPosition();
                final SonosQueuePlayer.Item removed = position == RecyclerView.NO_POSITION
                        ? null : SonosQueuePlayer.removeAt(position);
                if (removed == null) {
                    Toast.makeText(SonosControlActivity.this,
                            R.string.sonos_remove_playing_denied, Toast.LENGTH_SHORT).show();
                    queueAdapter.notifyItemChanged(position); // snap the row back
                    return;
                }
                queueRows.remove(position);
                queueAdapter.notifyItemRemoved(position);
                shownQueueVersion = SonosQueuePlayer.queueVersion();
                Snackbar.make(queueList, R.string.sonos_removed_from_queue,
                                Snackbar.LENGTH_LONG)
                        .setAction(R.string.undo, v -> {
                            SonosQueuePlayer.restore(position, removed);
                            shownQueueIndex = -2; // force list rebuild
                            updateQueue();
                        })
                        .show();
            }

            @Override
            public void onChildDraw(@NonNull final Canvas canvas,
                                    @NonNull final RecyclerView recyclerView,
                                    @NonNull final RecyclerView.ViewHolder viewHolder,
                                    final float dX, final float dY, final int actionState,
                                    final boolean isCurrentlyActive) {
                if (actionState == ItemTouchHelper.ACTION_STATE_SWIPE && dX != 0) {
                    final View row = viewHolder.itemView;
                    if (dX > 0) {
                        swipeBackground.setBounds(row.getLeft(), row.getTop(),
                                row.getLeft() + (int) dX, row.getBottom());
                    } else {
                        swipeBackground.setBounds(row.getRight() + (int) dX, row.getTop(),
                                row.getRight(), row.getBottom());
                    }
                    swipeBackground.draw(canvas);
                    if (deleteIcon != null) {
                        final int size = deleteIcon.getIntrinsicHeight();
                        final int top = row.getTop() + (row.getHeight() - size) / 2;
                        final int margin = size;
                        final int left = dX > 0
                                ? row.getLeft() + margin
                                : row.getRight() - margin - size;
                        deleteIcon.setBounds(left, top, left + size, top + size);
                        deleteIcon.draw(canvas);
                    }
                }
                super.onChildDraw(canvas, recyclerView, viewHolder, dX, dY,
                        actionState, isCurrentlyActive);
            }

            @Override
            public void clearView(@NonNull final RecyclerView recyclerView,
                                  @NonNull final RecyclerView.ViewHolder viewHolder) {
                super.clearView(recyclerView, viewHolder);
                // drag finished — rebuild so the ▶ marker lands where it belongs.
                // posted: clearView can fire mid-layout (e.g. rapid swipes), and
                // notifyDataSetChanged during layout throws IllegalStateException
                queueList.post(() -> {
                    shownQueueIndex = -2;
                    updateQueue();
                });
            }
        }).attachToRecyclerView(queueList);
    }

    /** Renders the queue-player playlist; rebuilds when the playing index or queue edits change. */
    private void updateQueue() {
        final List<String> titles = SonosQueuePlayer.queueTitles();
        final List<Long> durations = SonosQueuePlayer.queueDurations();
        final int index = SonosQueuePlayer.queueIndex();
        final int version = SonosQueuePlayer.queueVersion();
        prevButton.setVisibility(titles == null ? View.GONE : View.VISIBLE);
        nextButton.setVisibility(titles == null ? View.GONE : View.VISIBLE);
        if (titles == null || durations == null) {
            if (!queueRows.isEmpty()) {
                queueRows.clear();
                queueAdapter.notifyDataSetChanged();
            }
            shownQueueIndex = -2;
            return;
        }
        if (index == shownQueueIndex && version == shownQueueVersion) {
            return;
        }
        final boolean indexMoved = index != shownQueueIndex;
        shownQueueIndex = index;
        shownQueueVersion = version;
        queueRows.clear();
        for (int i = 0; i < titles.size(); i++) {
            final long duration = durations.get(i);
            queueRows.add((i == index ? "▶ " : "") + titles.get(i)
                    + (duration > 0 ? "  ·  " + shortTime(duration) : ""));
        }
        queueAdapter.notifyDataSetChanged();
        if (indexMoved) {
            queueList.scrollToPosition(Math.max(0, index - 1));
        }
    }

    /** Chapter list (from PREF_LAST_CHAPTERS, "seconds|title" lines): tap to seek. */
    private void showChaptersDialog() {
        final String[] lines = chapterData.split("\n");
        final CharSequence[] labels = new CharSequence[lines.length];
        final int[] starts = new int[lines.length];
        for (int i = 0; i < lines.length; i++) {
            final int separator = lines[i].indexOf('|');
            starts[i] = Integer.parseInt(lines[i].substring(0, separator));
            labels[i] = shortTime(starts[i]) + "  " + lines[i].substring(separator + 1);
        }
        new AlertDialog.Builder(this)
                .setTitle(R.string.sonos_chapters)
                .setItems(labels, (dialog, which) -> run(() -> device.seek(starts[which])))
                .show();
    }

    /** "3:45" for tracks under an hour, "1:03:45" above. */
    private static String shortTime(final long seconds) {
        return seconds >= 3600
                ? String.format(Locale.US, "%d:%02d:%02d",
                        seconds / 3600, (seconds / 60) % 60, seconds % 60)
                : String.format(Locale.US, "%d:%02d", seconds / 60, seconds % 60);
    }

    private static String formatTimes(final long position, final long duration) {
        return SonosDevice.formatTime(position) + " / " + SonosDevice.formatTime(duration);
    }

    private File cacheDir() {
        return new File(getCacheDir(), "sonos");
    }

    private void run(final io.reactivex.rxjava3.functions.Action action) {
        disposables.add(Completable.fromAction(action)
                .subscribeOn(Schedulers.io())
                .observeOn(AndroidSchedulers.mainThread())
                .subscribe(() -> { }, throwable ->
                        Toast.makeText(this,
                                getString(R.string.sonos_error,
                                        String.valueOf(throwable.getMessage())),
                                Toast.LENGTH_LONG).show()));
    }
}
