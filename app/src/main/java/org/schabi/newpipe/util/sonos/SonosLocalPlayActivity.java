package org.schabi.newpipe.util.sonos;

import android.Manifest;
import android.content.ContentUris;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.database.Cursor;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.os.Environment;
import android.provider.DocumentsContract;
import android.provider.MediaStore;
import android.util.Log;
import android.widget.Toast;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.content.ContextCompat;

import org.schabi.newpipe.R;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import io.reactivex.rxjava3.android.schedulers.AndroidSchedulers;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.schedulers.Schedulers;

/**
 * Transparent entry point for playing local audio on a Sonos speaker: shows up
 * as "Play on Sonos" in file managers ({@code ACTION_VIEW}) and share sheets
 * ({@code ACTION_SEND}). Single audio files play directly; m3u/m3u8 playlists
 * are parsed and played as a queue (auto-advancing). Finishes as soon as the
 * speaker is picked; everything after runs on the application context.
 */
public final class SonosLocalPlayActivity extends AppCompatActivity {
    private static final String TAG = "SonosLocalPlay";
    private static final int PERMISSION_REQUEST = 1;
    private Uri uri;

    @Override
    protected void onCreate(@Nullable final Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        uri = Intent.ACTION_SEND.equals(getIntent().getAction())
                ? getIntent().getParcelableExtra(Intent.EXTRA_STREAM)
                : getIntent().getData();
        if (uri == null) {
            finish();
            return;
        }
        if (isM3u()) {
            // m3u entries are raw file paths → reading them needs the media permission
            final String permission = Build.VERSION.SDK_INT >= 33
                    ? Manifest.permission.READ_MEDIA_AUDIO
                    : Manifest.permission.READ_EXTERNAL_STORAGE;
            if (ContextCompat.checkSelfPermission(this, permission)
                    != PackageManager.PERMISSION_GRANTED) {
                requestPermissions(new String[]{permission}, PERMISSION_REQUEST);
                return;
            }
            playM3u();
        } else {
            SonosPlayer.playLocalFile(this, uri, this::finish);
        }
    }

    @Override
    public void onRequestPermissionsResult(final int requestCode,
                                           @NonNull final String[] permissions,
                                           @NonNull final int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        // if denied, file entries fail item-by-item and get skipped with a toast
        playM3u();
    }

    private boolean isM3u() {
        final String mime = getContentResolver().getType(uri);
        if (mime != null && mime.toLowerCase(Locale.US).contains("mpegurl")) {
            return true;
        }
        final String name = SonosPlayer.displayName(this, uri).toLowerCase(Locale.US);
        return name.endsWith(".m3u") || name.endsWith(".m3u8");
    }

    private void playM3u() {
        //noinspection ResultOfMethodCallIgnored
        Single.fromCallable(this::parseM3u)
                .subscribeOn(Schedulers.io())
                .observeOn(AndroidSchedulers.mainThread())
                .subscribe(items -> {
                    if (items.isEmpty()) {
                        Toast.makeText(getApplicationContext(),
                                R.string.sonos_no_compatible_stream,
                                Toast.LENGTH_LONG).show();
                        finish();
                    } else {
                        SonosQueuePlayer.playItems(this, items, this::finish);
                    }
                }, throwable -> {
                    Toast.makeText(getApplicationContext(),
                            getString(R.string.sonos_error,
                                    String.valueOf(throwable.getMessage())),
                            Toast.LENGTH_LONG).show();
                    finish();
                });
    }

    private List<SonosQueuePlayer.Item> parseM3u() throws IOException {
        final List<SonosQueuePlayer.Item> items = new ArrayList<>();
        final File baseDir = playlistDirectory();
        String pendingTitle = null;
        try (InputStream in = getContentResolver().openInputStream(uri)) {
            if (in == null) {
                throw new IOException("Cannot open " + uri);
            }
            final BufferedReader reader =
                    new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }
                if (line.startsWith("#")) {
                    // #EXTINF:duration,Artist - Title
                    if (line.startsWith("#EXTINF:")) {
                        final int comma = line.indexOf(',');
                        if (comma >= 0 && comma + 1 < line.length()) {
                            pendingTitle = line.substring(comma + 1).trim();
                        }
                    }
                    continue;
                }
                final SonosQueuePlayer.Item item =
                        toItem(line.replace('\\', '/'), pendingTitle, baseDir);
                if (item != null) {
                    items.add(item);
                }
                pendingTitle = null;
            }
        }
        Log.i(TAG, "m3u parsed: " + items.size() + " items, baseDir=" + baseDir
                + ", uri=" + uri);
        return items;
    }

    @Nullable
    private SonosQueuePlayer.Item toItem(final String location, @Nullable final String title,
                                         @Nullable final File baseDir) {
        if (location.startsWith("http://") || location.startsWith("https://")) {
            return new SonosQueuePlayer.HttpItem(location,
                    title != null ? title : location);
        }
        final String fileName = location.substring(location.lastIndexOf('/') + 1);
        final String displayTitle = title != null ? title : fileName;
        File file = null;
        if (location.startsWith("file://")) {
            file = new File(Uri.parse(location).getPath());
        } else if (location.startsWith("/")) {
            file = new File(location);
        } else if (baseDir != null) {
            file = new File(baseDir, location);
        }
        if (file != null && file.isFile()) {
            return new SonosQueuePlayer.LocalFileItem(Uri.fromFile(file), displayTitle);
        }
        // Path unresolvable (opaque file-manager URI, moved file, …) — find the
        // entry by filename in the media index instead.
        final Uri media = findInMediaStore(fileName, location);
        if (media != null) {
            return new SonosQueuePlayer.LocalFileItem(media, displayTitle);
        }
        Log.w(TAG, "m3u entry not found: " + location);
        return null;
    }

    /**
     * Looks the file up in MediaStore audio by display name; if several files
     * share the name, prefers the one whose path ends with the m3u entry's
     * relative path.
     */
    @Nullable
    private Uri findInMediaStore(final String fileName, final String location) {
        try (Cursor cursor = getContentResolver().query(
                MediaStore.Audio.Media.EXTERNAL_CONTENT_URI,
                new String[]{MediaStore.Audio.Media._ID, MediaStore.Audio.Media.DATA},
                MediaStore.Audio.Media.DISPLAY_NAME + " = ?",
                new String[]{fileName}, null)) {
            long firstId = -1;
            while (cursor != null && cursor.moveToNext()) {
                if (firstId < 0) {
                    firstId = cursor.getLong(0);
                }
                final String path = cursor.getString(1);
                if (path != null && path.endsWith("/" + location)) {
                    firstId = cursor.getLong(0);
                    break;
                }
            }
            if (firstId >= 0) {
                return ContentUris.withAppendedId(
                        MediaStore.Audio.Media.EXTERNAL_CONTENT_URI, firstId);
            }
        } catch (final RuntimeException ignored) {
        }
        return null;
    }

    /**
     * Best-effort folder of the playlist file, for resolving relative entries.
     * Many file managers (MiXplorer, …) embed the real filesystem path in their
     * content URIs, so first try the URI path directly; then the SAF document id
     * ("primary:Music/x.m3u" / "raw:/storage/…"). Null if nothing matches —
     * relative entries are then skipped.
     */
    @Nullable
    private File playlistDirectory() {
        final File direct = existingFile(uri.getPath());
        if (direct != null) {
            return direct.getParentFile();
        }
        try {
            final String docId = DocumentsContract.getDocumentId(uri);
            if (docId != null && docId.startsWith("primary:")) {
                return new File(Environment.getExternalStorageDirectory(),
                        docId.substring("primary:".length())).getParentFile();
            }
            if (docId != null && docId.startsWith("raw:")) {
                return new File(docId.substring("raw:".length())).getParentFile();
            }
        } catch (final RuntimeException ignored) {
        }
        return null;
    }

    /** The path itself, or its "/storage/…" suffix, if it names a readable file. */
    @Nullable
    private static File existingFile(@Nullable final String path) {
        if (path == null) {
            return null;
        }
        File file = new File(path);
        if (file.isFile()) {
            return file;
        }
        final int idx = path.indexOf("/storage/");
        if (idx > 0) {
            file = new File(path.substring(idx));
            if (file.isFile()) {
                return file;
            }
        }
        return null;
    }
}
