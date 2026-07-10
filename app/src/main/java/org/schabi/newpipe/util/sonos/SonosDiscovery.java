package org.schabi.newpipe.util.sonos;

import android.content.Context;
import android.net.wifi.WifiManager;

import androidx.annotation.NonNull;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

/**
 * Finds Sonos speakers on the LAN via SSDP M-SEARCH
 * (ST urn:schemas-upnp-org:device:ZonePlayer:1 — answered by both S1 and S2).
 */
public final class SonosDiscovery {
    private static final String SSDP_ADDRESS = "239.255.255.250";
    private static final int SSDP_PORT = 1900;
    private static final String SEARCH_TARGET = "urn:schemas-upnp-org:device:ZonePlayer:1";
    private static final int RESPONSE_WINDOW_MS = 2500;

    private static final Pattern LOCATION = Pattern.compile("(?im)^LOCATION:\\s*http://([^:/]+)");
    private static final Pattern ROOM_NAME = Pattern.compile("<roomName>([^<]*)</roomName>");

    private static final OkHttpClient CLIENT = new OkHttpClient.Builder()
            .connectTimeout(3, TimeUnit.SECONDS)
            .readTimeout(3, TimeUnit.SECONDS)
            .build();

    private SonosDiscovery() {
    }

    /** Blocking, takes ~{@value RESPONSE_WINDOW_MS} ms. Call from a background thread. */
    @NonNull
    public static List<SonosDevice> discover(final Context context) throws IOException {
        final WifiManager wifi =
                (WifiManager) context.getApplicationContext().getSystemService(Context.WIFI_SERVICE);
        final WifiManager.MulticastLock lock = wifi.createMulticastLock("pipepipe-sonos");
        lock.acquire();
        try {
            return search();
        } finally {
            lock.release();
        }
    }

    private static List<SonosDevice> search() throws IOException {
        final String msearch = "M-SEARCH * HTTP/1.1\r\n"
                + "HOST: " + SSDP_ADDRESS + ":" + SSDP_PORT + "\r\n"
                + "MAN: \"ssdp:discover\"\r\n"
                + "MX: 1\r\n"
                + "ST: " + SEARCH_TARGET + "\r\n\r\n";
        final byte[] payload = msearch.getBytes(StandardCharsets.UTF_8);
        final InetAddress group = InetAddress.getByName(SSDP_ADDRESS);

        // ip -> room name; LinkedHashMap keeps discovery order and dedupes multi-NIC replies
        final Map<String, String> found = new LinkedHashMap<>();
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.setSoTimeout(500);
            // send twice — SSDP is UDP, first packet is easily lost
            socket.send(new DatagramPacket(payload, payload.length, group, SSDP_PORT));
            socket.send(new DatagramPacket(payload, payload.length, group, SSDP_PORT));

            final byte[] buffer = new byte[2048];
            final long deadline = System.currentTimeMillis() + RESPONSE_WINDOW_MS;
            while (System.currentTimeMillis() < deadline) {
                final DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                try {
                    socket.receive(packet);
                } catch (final SocketTimeoutException e) {
                    continue;
                }
                final String response =
                        new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                final Matcher m = LOCATION.matcher(response);
                if (m.find()) {
                    found.putIfAbsent(m.group(1), null);
                }
            }
        }

        final List<SonosDevice> devices = new ArrayList<>();
        for (final String ip : found.keySet()) {
            devices.add(new SonosDevice(ip, fetchRoomName(ip)));
        }
        return devices;
    }

    private static String fetchRoomName(final String ip) {
        final Request request = new Request.Builder()
                .url("http://" + ip + ":1400/xml/device_description.xml")
                .build();
        try (Response response = CLIENT.newCall(request).execute()) {
            if (response.isSuccessful() && response.body() != null) {
                final Matcher m = ROOM_NAME.matcher(response.body().string());
                if (m.find() && !m.group(1).isEmpty()) {
                    return m.group(1);
                }
            }
        } catch (final IOException ignored) {
            // fall through to IP as name
        }
        return ip;
    }
}
