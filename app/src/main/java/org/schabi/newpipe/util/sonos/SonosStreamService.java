package org.schabi.newpipe.util.sonos;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.net.wifi.WifiManager;
import android.os.Build;
import android.os.Handler;
import android.os.IBinder;
import android.util.Log;

import androidx.annotation.Nullable;
import androidx.core.app.NotificationCompat;

import org.schabi.newpipe.R;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
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

/**
 * Foreground service that serves one local audio file over HTTP so a Sonos
 * speaker on the LAN can stream it (used when the source has no direct URL,
 * e.g. YouTube SABR — the audio is first downloaded to cache, then served).
 */
public final class SonosStreamService extends Service {
    private static final String TAG = "SonosStreamService";
    public static final int PORT = 8987;
    private static final String ACTION_STOP = "org.schabi.newpipe.sonos.STOP";
    private static final String EXTRA_FILE = "file";
    private static final String EXTRA_TITLE = "title";
    private static final String EXTRA_DURATION = "duration";
    private static final int NOTIFICATION_ID = 64719;

    private ServerSocket serverSocket;
    private File servedFile;
    private WifiManager.WifiLock wifiLock;
    private final Handler autoStopHandler = new Handler();

    /**
     * Starts serving the given file and returns the URL a Sonos speaker can fetch it from.
     *
     * <p>The URL path carries a fresh token every call: Sonos caches track metadata
     * (including duration) keyed by resource URL, so reusing a fixed path makes each
     * new track inherit the previous track's duration. The server ignores the path and
     * always serves the current file — fine for our single-active-playback model.</p>
     */
    public static String start(final Context context, final File file, final String title,
                               final long durationSeconds) throws IOException {
        final String ip = getLocalIpAddress();
        final Intent intent = new Intent(context, SonosStreamService.class)
                .putExtra(EXTRA_FILE, file.getAbsolutePath())
                .putExtra(EXTRA_TITLE, title)
                .putExtra(EXTRA_DURATION, durationSeconds);
        context.startService(intent);
        // ponytail: path ignored by the server; token only defeats Sonos's per-URL cache
        return "http://" + ip + ":" + PORT + "/audio-" + System.currentTimeMillis() + ".m4a";
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
        servedFile = new File(intent.getStringExtra(EXTRA_FILE));
        final String title = intent.getStringExtra(EXTRA_TITLE);
        final long duration = intent.getLongExtra(EXTRA_DURATION, 0);

        startForeground(NOTIFICATION_ID, buildNotification(title));
        acquireWifiLock();
        startServer();

        // ponytail: no end-of-playback detection — stop serving after duration + generous
        // slack (covers pauses); poll GetTransportInfo instead if this ever bites
        autoStopHandler.removeCallbacksAndMessages(null);
        autoStopHandler.postDelayed(this::stopSelf, (duration + 1800) * 1000L);
        return START_NOT_STICKY;
    }

    private Notification buildNotification(final String title) {
        final String channelId = getString(R.string.notification_channel_id);
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            getSystemService(NotificationManager.class).createNotificationChannel(
                    new NotificationChannel(channelId, getString(R.string.play_on_sonos_title),
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
    }

    private void startServer() {
        if (serverSocket != null && !serverSocket.isClosed()) {
            return; // already serving; servedFile was updated above
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

    private void handleClient(final Socket client) {
        final File file = servedFile;
        try (Socket socket = client) {
            final BufferedReader reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));
            final String requestLine = reader.readLine();
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
                    .append("Content-Type: audio/mp4\r\n")
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

    @Override
    public void onDestroy() {
        autoStopHandler.removeCallbacksAndMessages(null);
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (final IOException ignored) {
            }
        }
        if (wifiLock != null && wifiLock.isHeld()) {
            wifiLock.release();
        }
        super.onDestroy();
    }

    @Nullable
    @Override
    public IBinder onBind(final Intent intent) {
        return null;
    }
}
