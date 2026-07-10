package org.schabi.newpipe.util.sonos;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/**
 * A single Sonos speaker, controlled via UPnP SOAP on port 1400.
 * Works on both S1 and S2 speakers. All methods do blocking network I/O.
 */
public final class SonosDevice implements Serializable {
    private static final MediaType TEXT_XML = MediaType.parse("text/xml; charset=\"utf-8\"");
    private static final String AV_TRANSPORT_URN = "urn:schemas-upnp-org:service:AVTransport:1";
    private static final String RENDERING_URN = "urn:schemas-upnp-org:service:RenderingControl:1";

    private static final OkHttpClient CLIENT = new OkHttpClient.Builder()
            .connectTimeout(5, TimeUnit.SECONDS)
            .readTimeout(10, TimeUnit.SECONDS)
            .build();

    private final String ip;
    private final String roomName;

    public SonosDevice(final String ip, final String roomName) {
        this.ip = ip;
        this.roomName = roomName;
    }

    public String getIp() {
        return ip;
    }

    public String getRoomName() {
        return roomName;
    }

    public void playUri(final String uri, final String title, @Nullable final String thumbnailUrl,
                        final long durationSeconds, final String mimeType) throws IOException {
        final String didl = buildDidl(uri, title, thumbnailUrl, durationSeconds, mimeType);
        soap("AVTransport", AV_TRANSPORT_URN, "SetAVTransportURI",
                "<CurrentURI>" + xmlEscape(uri) + "</CurrentURI>"
                        + "<CurrentURIMetaData>" + xmlEscape(didl) + "</CurrentURIMetaData>");
        play();
    }

    public void play() throws IOException {
        soap("AVTransport", AV_TRANSPORT_URN, "Play", "<Speed>1</Speed>");
    }

    public void pause() throws IOException {
        soap("AVTransport", AV_TRANSPORT_URN, "Pause", "");
    }

    public void stop() throws IOException {
        soap("AVTransport", AV_TRANSPORT_URN, "Stop", "");
    }

    /** @return e.g. "PLAYING", "PAUSED_PLAYBACK", "STOPPED" */
    public String getTransportState() throws IOException {
        final String body = soap("AVTransport", AV_TRANSPORT_URN, "GetTransportInfo", "");
        return extractTag(body, "CurrentTransportState");
    }

    public void seek(final long positionSeconds) throws IOException {
        soap("AVTransport", AV_TRANSPORT_URN, "Seek",
                "<Unit>REL_TIME</Unit><Target>" + formatTime(positionSeconds) + "</Target>");
    }

    /** @return {position, duration} in seconds (0 if unknown). */
    public long[] getPositionInfo() throws IOException {
        final String body = soap("AVTransport", AV_TRANSPORT_URN, "GetPositionInfo", "");
        return new long[]{
                parseTime(extractTag(body, "RelTime")),
                parseTime(extractTag(body, "TrackDuration")),
        };
    }

    public static String formatTime(final long seconds) {
        return String.format(Locale.US, "%d:%02d:%02d",
                seconds / 3600, (seconds / 60) % 60, seconds % 60);
    }

    private static long parseTime(final String time) {
        try {
            final String[] parts = time.split(":");
            return Long.parseLong(parts[0]) * 3600
                    + Long.parseLong(parts[1]) * 60
                    + (long) Double.parseDouble(parts[2]);
        } catch (final RuntimeException e) {
            return 0;
        }
    }

    /** @return 0-100 */
    public int getVolume() throws IOException {
        final String body = soap("RenderingControl", RENDERING_URN, "GetVolume",
                "<Channel>Master</Channel>");
        try {
            return Integer.parseInt(extractTag(body, "CurrentVolume"));
        } catch (final NumberFormatException e) {
            throw new IOException("Unparseable volume response", e);
        }
    }

    public void setVolume(final int volume) throws IOException {
        soap("RenderingControl", RENDERING_URN, "SetVolume",
                "<Channel>Master</Channel><DesiredVolume>"
                        + Math.max(0, Math.min(100, volume)) + "</DesiredVolume>");
    }

    private String soap(final String service, final String urn, final String action,
                        final String args) throws IOException {
        final String envelope = "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
                + "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" "
                + "s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">"
                + "<s:Body><u:" + action + " xmlns:u=\"" + urn + "\">"
                + "<InstanceID>0</InstanceID>" + args
                + "</u:" + action + "></s:Body></s:Envelope>";
        final Request request = new Request.Builder()
                .url("http://" + ip + ":1400/MediaRenderer/" + service + "/Control")
                .header("SOAPACTION", "\"" + urn + "#" + action + "\"")
                .post(RequestBody.create(envelope, TEXT_XML))
                .build();
        try (Response response = CLIENT.newCall(request).execute()) {
            final String body = response.body() != null ? response.body().string() : "";
            if (!response.isSuccessful()) {
                throw new IOException("Sonos " + action + " failed: HTTP "
                        + response.code() + " " + extractTag(body, "errorCode"));
            }
            return body;
        }
    }

    private static String buildDidl(final String uri, final String title,
                                    @Nullable final String thumbnailUrl,
                                    final long durationSeconds, final String mimeType) {
        final String duration = formatTime(durationSeconds);
        return "<DIDL-Lite xmlns=\"urn:schemas-upnp-org:metadata-1-0/DIDL-Lite/\" "
                + "xmlns:dc=\"http://purl.org/dc/elements/1.1/\" "
                + "xmlns:upnp=\"urn:schemas-upnp-org:metadata-1-0/upnp/\">"
                + "<item id=\"pipepipe\" parentID=\"-1\" restricted=\"true\">"
                + "<dc:title>" + xmlEscape(title) + "</dc:title>"
                + (thumbnailUrl != null
                        ? "<upnp:albumArtURI>" + xmlEscape(thumbnailUrl) + "</upnp:albumArtURI>"
                        : "")
                + "<upnp:class>object.item.audioItem.musicTrack</upnp:class>"
                + "<res duration=\"" + duration + "\" protocolInfo=\"http-get:*:"
                + mimeType + ":*\">" + xmlEscape(uri) + "</res>"
                + "</item></DIDL-Lite>";
    }

    // ponytail: regex over a full XML parser — responses are tiny, fixed-shape Sonos SOAP
    @NonNull
    private static String extractTag(final String xml, final String tag) {
        final Matcher m = Pattern.compile("<" + tag + ">([^<]*)</" + tag + ">").matcher(xml);
        return m.find() ? m.group(1) : "";
    }

    private static String xmlEscape(final String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                .replace("\"", "&quot;").replace("'", "&apos;");
    }

    @NonNull
    @Override
    public String toString() {
        return roomName + " (" + ip + ")";
    }
}
