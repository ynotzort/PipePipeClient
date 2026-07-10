package org.schabi.newpipe.util.sonos;

import android.content.SharedPreferences;
import android.os.Bundle;
import android.view.View;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.ListView;
import android.widget.SeekBar;
import android.widget.TextView;
import android.widget.Toast;

import androidx.annotation.Nullable;
import androidx.appcompat.app.AppCompatActivity;
import androidx.preference.PreferenceManager;

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
    private final CompositeDisposable disposables = new CompositeDisposable();
    private SharedPreferences prefs;
    private SonosDevice device;
    private TextView titleView;
    private TextView stateView;
    private TextView timeLabel;
    private SeekBar positionBar;
    private SeekBar volumeBar;
    private ListView queueList;
    private Button prevButton;
    private Button nextButton;
    private Button clearCacheButton;
    private boolean draggingPosition;
    private boolean draggingVolume;
    private long knownDuration;
    private int shownQueueIndex = -2;

    @Override
    protected void onCreate(@Nullable final Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        Localization.assureCorrectAppLanguage(this);
        ThemeHelper.setTheme(this);
        setContentView(R.layout.activity_sonos_control);
        setTitle(R.string.play_on_sonos_title);

        prefs = PreferenceManager.getDefaultSharedPreferences(this);
        final String ip = prefs.getString("sonos_last_ip", null);
        if (ip == null) {
            Toast.makeText(this, R.string.sonos_no_speaker_yet, Toast.LENGTH_LONG).show();
            finish();
            return;
        }
        device = new SonosDevice(ip, prefs.getString("sonos_last_name", ip));
        // Sonos reports TrackDuration from the file's (sometimes wrong) moov header;
        // prefer the real duration we knew when we started playback.
        knownDuration = prefs.getLong("sonos_last_duration", 0);

        ((TextView) findViewById(R.id.sonos_room_name)).setText(device.getRoomName());
        titleView = findViewById(R.id.sonos_track_title);
        titleView.setText(prefs.getString("sonos_last_title", ""));
        stateView = findViewById(R.id.sonos_state);
        timeLabel = findViewById(R.id.sonos_time_label);
        positionBar = findViewById(R.id.sonos_position_bar);
        volumeBar = findViewById(R.id.sonos_volume_bar);
        queueList = findViewById(R.id.sonos_queue_list);
        clearCacheButton = findViewById(R.id.sonos_btn_clear_cache);

        findViewById(R.id.sonos_btn_play).setOnClickListener(v -> run(device::play));
        findViewById(R.id.sonos_btn_pause).setOnClickListener(v -> run(device::pause));
        findViewById(R.id.sonos_btn_stop).setOnClickListener(v -> run(device::stop));
        prevButton = findViewById(R.id.sonos_btn_prev);
        nextButton = findViewById(R.id.sonos_btn_next);
        prevButton.setOnClickListener(v -> SonosQueuePlayer.previous());
        nextButton.setOnClickListener(v -> SonosQueuePlayer.next());
        queueList.setOnItemClickListener((parent, view, position, id) ->
                SonosQueuePlayer.skipTo(position));

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

        clearCacheButton.setOnClickListener(v -> {
            final File[] files = cacheDir().listFiles();
            if (files != null) {
                for (final File f : files) {
                    //noinspection ResultOfMethodCallIgnored
                    f.delete();
                }
            }
            Toast.makeText(this, R.string.sonos_cache_cleared, Toast.LENGTH_SHORT).show();
            updateCacheButton();
        });
        updateCacheButton();
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
        titleView.setText(prefs.getString("sonos_last_title", ""));
        knownDuration = prefs.getLong("sonos_last_duration", 0);
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
        if (!draggingVolume) {
            volumeBar.setProgress(volume);
        }
    }

    /** Renders the queue-player playlist; rebuilds only when the playing index moves. */
    private void updateQueue() {
        final List<String> titles = SonosQueuePlayer.queueTitles();
        final int index = SonosQueuePlayer.queueIndex();
        prevButton.setVisibility(titles == null ? View.GONE : View.VISIBLE);
        nextButton.setVisibility(titles == null ? View.GONE : View.VISIBLE);
        if (titles == null) {
            if (queueList.getAdapter() != null) {
                queueList.setAdapter(null);
            }
            shownQueueIndex = -2;
            return;
        }
        if (index == shownQueueIndex) {
            return;
        }
        shownQueueIndex = index;
        final List<String> rows = new ArrayList<>(titles.size());
        for (int i = 0; i < titles.size(); i++) {
            rows.add((i == index ? "▶ " : "") + titles.get(i));
        }
        queueList.setAdapter(new ArrayAdapter<>(this,
                android.R.layout.simple_list_item_1, rows));
        queueList.setSelection(Math.max(0, index - 1));
    }

    private static String formatTimes(final long position, final long duration) {
        return SonosDevice.formatTime(position) + " / " + SonosDevice.formatTime(duration);
    }

    private File cacheDir() {
        return new File(getCacheDir(), "sonos");
    }

    private void updateCacheButton() {
        long bytes = 0;
        final File[] files = cacheDir().listFiles();
        if (files != null) {
            for (final File f : files) {
                bytes += f.length();
            }
        }
        clearCacheButton.setText(getString(R.string.sonos_clear_cache,
                String.format(Locale.getDefault(), "%.1f MB", bytes / 1048576.0)));
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
