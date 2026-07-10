package org.schabi.newpipe.util.sonos;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.net.Uri;
import android.net.wifi.WifiManager;
import android.os.Build;
import android.os.Handler;
import android.os.IBinder;
import android.os.PowerManager;
import android.util.Log;

import androidx.annotation.Nullable;
import androidx.core.app.NotificationCompat;
import androidx.preference.PreferenceManager;

import com.arthenica.ffmpegkit.FFmpegKit;
import com.arthenica.ffmpegkit.FFmpegKitConfig;
import com.arthenica.ffmpegkit.FFmpegSession;

import org.schabi.newpipe.R;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Foreground service that serves local audio files over HTTP so a Sonos
 * speaker on the LAN can stream them (used when the source has no direct URL,
 * e.g. YouTube SABR — the audio is first downloaded to cache, then served).
 * Serves the current track plus, in queue mode, the prefetched next one.
 */
public final class SonosStreamService extends Service {
    private static final String TAG = "SonosStreamService";
    public static final int PORT = 8987;
    private static final String ACTION_STOP = "org.schabi.newpipe.sonos.STOP";
    private static final String EXTRA_DURATION = "duration";
    private static final String EXTRA_LIVE = "live";
    private static final long LIVE_IDLE_STOP_MS = 1800 * 1000L;
    private static final int NOTIFICATION_ID = 64719;

    /**
     * Path → file currently servable. Static so callers can register/drop entries
     * without binding; the service instance only owns the socket and lifecycle.
     * Multiple entries allow a queued "next" track to be prefetched by the speaker
     * while the current one is still served.
     */
    private static final Map<String, File> FILES = new ConcurrentHashMap<>();
    /** Path → HLS variant URL for endless live relays (ffmpeg remux, no file). */
    private static final Map<String, String> LIVE = new ConcurrentHashMap<>();
    private static final AtomicLong SEQUENCE = new AtomicLong();

    /** Running service instance, for notification refreshes on track change. */
    private static volatile SonosStreamService instance;

    private ServerSocket serverSocket;
    private WifiManager.WifiLock wifiLock;
    private PowerManager.WakeLock wakeLock;
    private final Handler autoStopHandler = new Handler();
    private final AtomicInteger liveClients = new AtomicInteger();
    /** Running live-relay ffmpeg session ids, cancelled on destroy. */
    private final Set<Long> liveSessions = ConcurrentHashMap.newKeySet();

    /**
     * Starts serving the given file and returns the URL a Sonos speaker can fetch it from.
     *
     * <p>The URL path carries a fresh token every call: Sonos caches track metadata
     * (including duration) keyed by resource URL, so reusing a fixed path makes each
     * new track inherit the previous track's duration. The token also keys the
     * path→file map.</p>
     */
    public static String start(final Context context, final File file, final String title,
                               final long durationSeconds) throws IOException {
        final String ip = getLocalIpAddress();
        final String name = file.getName();
        final String extension = name.contains(".")
                ? name.substring(name.lastIndexOf('.')) : ".m4a";
        final String path = "/audio-" + System.currentTimeMillis()
                + "-" + SEQUENCE.incrementAndGet() + extension;
        FILES.put(path, file);
        final Intent intent = new Intent(context, SonosStreamService.class)
                .putExtra(EXTRA_DURATION, durationSeconds);
        context.startService(intent);
        return "http://" + ip + ":" + PORT + path;
    }

    /**
     * Starts an endless live relay for the given HLS variant and returns the URL
     * the speaker can stream from. Each GET on it spawns its own ffmpeg pulling
     * the variant, dropping video and remuxing the AAC to ADTS (no re-encode).
     * Unique path per call for the same metadata-cache reason as {@link #start}.
     */
    public static String startLive(final Context context, final String hlsVariantUrl,
                                   final String title) throws IOException {
        final String ip = getLocalIpAddress();
        final String path = "/live-" + System.currentTimeMillis()
                + "-" + SEQUENCE.incrementAndGet() + ".aac";
        LIVE.put(path, hlsVariantUrl);
        context.startService(new Intent(context, SonosStreamService.class)
                .putExtra(EXTRA_LIVE, true));
        return "http://" + ip + ":" + PORT + path;
    }

    /** Whether the file is registered for serving (guards cache purges). */
    public static boolean isServing(final File file) {
        return FILES.containsValue(file);
    }

    /**
     * Stops serving the given URL's path. The file stays in cache for replays;
     * the size-capped LRU trim in SonosPlayer reclaims it eventually.
     * No-op for foreign URLs.
     */
    public static void drop(final String url) {
        FILES.remove(Uri.parse(url).getPath());
        LIVE.remove(Uri.parse(url).getPath());
    }

    /** Stops the service (and with it, via onDestroy, all serving and cached files). */
    public static void shutdown(final Context context) {
        context.stopService(new Intent(context, SonosStreamService.class));
    }

    /** Re-renders the notification from the persisted current track (no-op if not running). */
    public static void refreshNotification() {
        final SonosStreamService service = instance;
        if (service != null) {
            service.getSystemService(NotificationManager.class)
                    .notify(NOTIFICATION_ID, service.buildNotification());
        }
    }

    private static String getLocalIpAddress() throws IOException {
        final Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            final Enumeration<InetAddress> addresses = interfaces.nextElement().getInetAddresses();
            while (addresses.hasMoreElements()) {
                final InetAddress address = addresses.nextElement();
                if (address instanceof Inet4Address && address.isSiteLocalAddress()) {
                    return address.getHostAddress();
                }
            }
        }
        throw new IOException("No LAN IP address found — is WiFi connected?");
    }

    @Override
    public int onStartCommand(final Intent intent, final int flags, final int startId) {
        if (intent == null || ACTION_STOP.equals(intent.getAction())) {
            stopSelf();
            return START_NOT_STICKY;
        }
        final long duration = intent.getLongExtra(EXTRA_DURATION, 0);

        instance = this;
        startForeground(NOTIFICATION_ID, buildNotification());
        acquireWifiLock();
        startServer();

        // ponytail: no end-of-playback detection — stop serving after duration + generous
        // slack (covers pauses); poll GetTransportInfo instead if this ever bites.
        // Live is endless: the timer here only covers "speaker never connected";
        // while a live client streams it's cancelled, and re-armed on disconnect.
        autoStopHandler.removeCallbacksAndMessages(null);
        autoStopHandler.postDelayed(this::stopSelf,
                intent.getBooleanExtra(EXTRA_LIVE, false)
                        ? LIVE_IDLE_STOP_MS : (duration + 1800) * 1000L);
        return START_NOT_STICKY;
    }

    /**
     * The notification always shows the persisted current track, not whatever
     * was served last: in queue mode the prefetched NEXT item is served while
     * the current one still plays, so a served-title notification is wrong.
     */
    private Notification buildNotification() {
        final String title = PreferenceManager
                .getDefaultSharedPreferences(this)
                .getString(SonosPlayer.PREF_LAST_TITLE, "");
        // dedicated channel so Sonos playback is configurable separately from
        // the app's other (downloads etc.) notifications
        final String channelId = "sonos_playback";
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            getSystemService(NotificationManager.class).createNotificationChannel(
                    new NotificationChannel(channelId,
                            getString(R.string.sonos_notification_channel),
                            NotificationManager.IMPORTANCE_LOW));
        }
        final PendingIntent stopIntent = PendingIntent.getService(this, 0,
                new Intent(this, SonosStreamService.class).setAction(ACTION_STOP),
                PendingIntent.FLAG_IMMUTABLE);
        final PendingIntent openIntent = PendingIntent.getActivity(this, 0,
                new Intent(this, SonosControlActivity.class),
                PendingIntent.FLAG_IMMUTABLE);
        return new NotificationCompat.Builder(this, channelId)
                .setContentTitle(getString(R.string.play_on_sonos_title))
                .setContentText(title)
                .setSmallIcon(R.drawable.ic_speaker)
                .setOngoing(true)
                .setContentIntent(openIntent)
                .addAction(0, getString(R.string.stop), stopIntent)
                .build();
    }

    private void acquireWifiLock() {
        if (wifiLock == null) {
            final WifiManager wifi =
                    (WifiManager) getApplicationContext().getSystemService(Context.WIFI_SERVICE);
            wifiLock = wifi.createWifiLock(WifiManager.WIFI_MODE_FULL_HIGH_PERF, TAG);
            wifiLock.acquire();
        }
        // The WifiLock keeps the radio up but NOT the CPU: with the screen off the
        // CPU dozes, the live-relay ffmpeg gets suspended and the speaker's buffer
        // drains. Hold the CPU for the service's (auto-stop-bounded) lifetime.
        if (wakeLock == null) {
            final PowerManager power = (PowerManager)
                    getApplicationContext().getSystemService(Context.POWER_SERVICE);
            wakeLock = power.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "pipepipe:" + TAG);
            wakeLock.acquire();
        }
    }

    private void startServer() {
        if (serverSocket != null && !serverSocket.isClosed()) {
            return; // already serving; FILES was updated by start()
        }
        final Thread thread = new Thread(() -> {
            try (ServerSocket server = new ServerSocket(PORT)) {
                serverSocket = server;
                while (!server.isClosed()) {
                    final Socket client = server.accept();
                    new Thread(() -> handleClient(client), TAG + "-client").start();
                }
            } catch (final IOException e) {
                Log.w(TAG, "server stopped", e);
            }
        }, TAG + "-server");
        thread.setDaemon(true);
        thread.start();
    }

    /** Cache files keep their source extension (local-file imports vary in format). */
    private static String contentTypeFor(final String name) {
        if (name.endsWith(".mp3")) {
            return "audio/mpeg";
        }
        if (name.endsWith(".flac")) {
            return "audio/flac";
        }
        if (name.endsWith(".wav")) {
            return "audio/wav";
        }
        if (name.endsWith(".ogg")) {
            return "audio/ogg";
        }
        return "audio/mp4";
    }

    private void handleClient(final Socket client) {
        try (Socket socket = client) {
            final BufferedReader reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));
            final String requestLine = reader.readLine();
            final String[] requestParts = requestLine == null
                    ? null : requestLine.split(" ");
            final String path = requestParts != null && requestParts.length > 1
                    ? requestParts[1] : null;
            final File file = path != null ? FILES.get(path) : null;
            final String liveUrl = path != null ? LIVE.get(path) : null;
            long rangeStart = 0;
            long rangeEnd = -1;
            String line;
            while ((line = reader.readLine()) != null && !line.isEmpty()) {
                final String lower = line.toLowerCase(Locale.US);
                if (lower.startsWith("range:") && lower.contains("bytes=")) {
                    final String[] parts =
                            lower.substring(lower.indexOf("bytes=") + 6).trim().split("-", 2);
                    rangeStart = parts[0].isEmpty() ? 0 : Long.parseLong(parts[0]);
                    if (parts.length > 1 && !parts[1].isEmpty()) {
                        rangeEnd = Long.parseLong(parts[1]);
                    }
                }
            }
            final OutputStream out = socket.getOutputStream();
            if (liveUrl != null) {
                serveLive(out, liveUrl, requestLine.startsWith("HEAD"));
                return;
            }
            if (requestLine == null || file == null || !file.isFile()) {
                out.write("HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n"
                        .getBytes(StandardCharsets.US_ASCII));
                return;
            }
            final long length = file.length();
            if (rangeEnd < 0 || rangeEnd >= length) {
                rangeEnd = length - 1;
            }
            final boolean partial = rangeStart > 0 || rangeEnd < length - 1;
            final long contentLength = rangeEnd - rangeStart + 1;
            final StringBuilder header = new StringBuilder()
                    .append(partial ? "HTTP/1.1 206 Partial Content\r\n" : "HTTP/1.1 200 OK\r\n")
                    .append("Content-Type: ").append(contentTypeFor(file.getName()))
                    .append("\r\n")
                    .append("Accept-Ranges: bytes\r\n")
                    .append("Content-Length: ").append(contentLength).append("\r\n");
            if (partial) {
                header.append("Content-Range: bytes ").append(rangeStart).append('-')
                        .append(rangeEnd).append('/').append(length).append("\r\n");
            }
            header.append("Connection: close\r\n\r\n");
            out.write(header.toString().getBytes(StandardCharsets.US_ASCII));
            if (!requestLine.startsWith("HEAD")) {
                try (FileInputStream in = new FileInputStream(file)) {
                    long skipped = 0;
                    while (skipped < rangeStart) {
                        skipped += in.skip(rangeStart - skipped);
                    }
                    final byte[] buffer = new byte[64 * 1024];
                    long remaining = contentLength;
                    int read;
                    while (remaining > 0
                            && (read = in.read(buffer, 0,
                                    (int) Math.min(buffer.length, remaining))) != -1) {
                        out.write(buffer, 0, read);
                        remaining -= read;
                    }
                }
            }
            out.flush();
        } catch (final IOException e) {
            Log.d(TAG, "client connection ended: " + e.getMessage());
        }
    }

    /**
     * Endless live relay (proven end-to-end via live_relay.py against the real
     * speaker): ffmpeg pulls the muxed HLS variant, drops video and remuxes the
     * AAC-LC track to ADTS into a named pipe; the pipe is streamed to the client
     * as a web-radio-style response — no Content-Length, no Range, connection
     * open until either side quits. Sonos double-connects (probe GET dropped
     * instantly, then the real one); each GET gets its own ffmpeg, so that's fine.
     */
    private void serveLive(final OutputStream out, final String hlsUrl, final boolean headOnly)
            throws IOException {
        out.write(("HTTP/1.0 200 OK\r\n"
                + "Content-Type: audio/aac\r\n"
                + "icy-name: PipePipe Live\r\n"
                + "Connection: close\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
        if (headOnly) {
            return;
        }
        // synchronized: concurrent first-use calls race on creating the pipes dir
        // inside ffmpeg-kit (mkdirs loser gets null) — Sonos double-connects
        final String pipe;
        synchronized (LIVE) {
            pipe = FFmpegKitConfig.registerNewFFmpegPipe(this);
        }
        if (pipe == null) {
            throw new IOException("ffmpeg pipe creation failed");
        }
        liveClients.incrementAndGet();
        autoStopHandler.removeCallbacksAndMessages(null);
        final AtomicBoolean readerOpened = new AtomicBoolean();
        // -y is required: the output "file" (the just-created FIFO) already exists,
        // and without it ffmpeg asks to overwrite and exits — empty stream, STOPPED
        final FFmpegSession session = FFmpegKit.executeWithArgumentsAsync(new String[]{
                "-y", "-loglevel", "error", "-i", hlsUrl, "-vn", "-c:a", "copy",
                "-f", "adts", pipe}, completed -> {
            Log.i(TAG, "live ffmpeg exited: " + completed.getReturnCode()
                    + " " + completed.getOutput());
            // FIFO open() for reading blocks until a writer appears — if ffmpeg
            // died before ever opening its output (bad/expired URL), pair the
            // stuck reader below with a throwaway writer so it sees instant EOF
            if (!readerOpened.get() && new File(pipe).exists()) {
                try {
                    new FileOutputStream(pipe).close();
                } catch (final IOException ignored) {
                }
            }
        });
        liveSessions.add(session.getSessionId());
        try (FileInputStream in = new FileInputStream(pipe)) {
            readerOpened.set(true);
            final byte[] buffer = new byte[8192];
            int read;
            while ((read = in.read(buffer)) != -1) {
                out.write(buffer, 0, read);
            }
        } finally {
            liveSessions.remove(session.getSessionId());
            FFmpegKit.cancel(session.getSessionId());
            FFmpegKitConfig.closeFFmpegPipe(pipe);
            Log.d(TAG, "live client gone, ffmpeg cancelled");
            if (liveClients.decrementAndGet() == 0) {
                // ponytail: no STOPPED-poll teardown — stop 30 min after the
                // speaker lets go (covers reconnects; user Stop is immediate)
                autoStopHandler.postDelayed(this::stopSelf, LIVE_IDLE_STOP_MS);
            }
        }
    }

    @Override
    public void onDestroy() {
        instance = null;
        autoStopHandler.removeCallbacksAndMessages(null);
        // ffmpeg exit → pipe EOF → relay thread closes its socket and cleans up
        for (final long sessionId : liveSessions) {
            FFmpegKit.cancel(sessionId);
        }
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (final IOException ignored) {
            }
        }
        if (wifiLock != null && wifiLock.isHeld()) {
            wifiLock.release();
        }
        if (wakeLock != null && wakeLock.isHeld()) {
            wakeLock.release();
        }
        // Files stay in cache for instant replays; the size-capped LRU trim in
        // SonosPlayer (and Android's cache-dir eviction) bounds disk usage.
        FILES.clear();
        LIVE.clear();
        super.onDestroy();
    }

    @Nullable
    @Override
    public IBinder onBind(final Intent intent) {
        return null;
    }
}
