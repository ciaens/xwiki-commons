/*
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.xwiki.store.blob.internal;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.io.IOUtils;
import org.xwiki.store.blob.Blob;
import org.xwiki.store.blob.BlobPath;
import org.xwiki.store.blob.BlobStoreException;
import org.xwiki.store.blob.UncheckedBlobStoreException;

/**
 * Lazily walks a directory of a {@link FileSystemBlobStore}, depth first, and returns its regular files as blobs.
 *
 * <p>Files and directories can be deleted during the walk, by another thread or by another cluster member sharing the
 * directory: the entries that disappear are skipped, as is the walked directory itself when it doesn't exist. Any
 * other failure is thrown as an {@link UncheckedBlobStoreException}. The iterator must be closed to release the open
 * directories.</p>
 *
 * <p>Symbolic links found during the walk are not followed: a link to a regular file is a blob, any other link (to a
 * directory, broken, looping or unreadable) is skipped. The walked directory itself can be a link to a directory, in
 * which case its target is walked, like {@link DeletingFileVisitor#deleteTree(Path)} deletes it.</p>
 *
 * @version $Id$
 * @since 18.10.0RC1
 */
final class FileSystemBlobIterator implements Iterator<Blob>, Closeable
{
    private final FileSystemBlobStore store;

    private final Path directory;

    private final BlobPath directoryPath;

    private final Deque<OpenDirectory> openDirectories = new ArrayDeque<>();

    private Blob nextBlob;

    private record OpenDirectory(DirectoryStream<Path> stream, Iterator<Path> entries)
    {
    }

    private FileSystemBlobIterator(FileSystemBlobStore store, Path directory, BlobPath directoryPath)
    {
        this.store = store;
        this.directory = directory;
        this.directoryPath = directoryPath;
    }

    /**
     * Start walking the given directory.
     *
     * @param store the store the directory belongs to
     * @param directory the absolute and normalized file system path of the directory to walk
     * @param directoryPath the blob path corresponding to the directory
     * @return the iterator over the regular files of the directory, at every depth
     * @throws BlobStoreException if the directory exists but cannot be read
     */
    static FileSystemBlobIterator walk(FileSystemBlobStore store, Path directory, BlobPath directoryPath)
        throws BlobStoreException
    {
        FileSystemBlobIterator iterator = new FileSystemBlobIterator(store, directory, directoryPath);
        try {
            iterator.openDirectory(directory);
        } catch (UncheckedBlobStoreException e) {
            throw e.getCause();
        }
        return iterator;
    }

    /**
     * Start walking the given directory and return the blobs as a stream, which closes the iterator when closed.
     *
     * @param store the store the directory belongs to
     * @param directory the absolute and normalized file system path of the directory to walk
     * @param directoryPath the blob path corresponding to the directory
     * @return the regular files of the directory, at every depth
     * @throws BlobStoreException if the directory exists but cannot be read
     */
    static Stream<Blob> stream(FileSystemBlobStore store, Path directory, BlobPath directoryPath)
        throws BlobStoreException
    {
        FileSystemBlobIterator iterator = walk(store, directory, directoryPath);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
            .onClose(iterator::close);
    }

    @Override
    public boolean hasNext()
    {
        if (this.nextBlob == null) {
            this.nextBlob = computeNext();
        }

        return this.nextBlob != null;
    }

    @Override
    public Blob next()
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        Blob result = this.nextBlob;
        this.nextBlob = null;
        return result;
    }

    @Override
    public void close()
    {
        while (!this.openDirectories.isEmpty()) {
            IOUtils.closeQuietly(this.openDirectories.pop().stream());
        }
    }

    private Blob computeNext()
    {
        while (!this.openDirectories.isEmpty()) {
            Path entry = nextEntry(this.openDirectories.peek());
            if (entry == null) {
                IOUtils.closeQuietly(this.openDirectories.pop().stream());
            } else {
                Blob blob = visit(entry);
                if (blob != null) {
                    return blob;
                }
            }
        }

        return null;
    }

    private Path nextEntry(OpenDirectory openDirectory)
    {
        try {
            return openDirectory.entries().hasNext() ? openDirectory.entries().next() : null;
        } catch (DirectoryIteratorException e) {
            if (e.getCause() instanceof NoSuchFileException) {
                // The directory was deleted while being read: there are no more entries.
                return null;
            }
            throw failure(e.getCause());
        }
    }

    private Blob visit(Path entry)
    {
        BasicFileAttributes attributes = readAttributes(entry);
        if (attributes != null && attributes.isSymbolicLink()) {
            // A link to a regular file is a blob. Links to directories are not followed.
            BasicFileAttributes targetAttributes = readLinkTargetAttributes(entry);
            return targetAttributes != null && targetAttributes.isRegularFile() ? toBlob(entry) : null;
        } else if (attributes != null && attributes.isDirectory()) {
            openDirectory(entry);
        } else if (attributes != null && attributes.isRegularFile()) {
            return toBlob(entry);
        }

        // The entry was deleted since the directory was read, or it's neither a file nor a directory.
        return null;
    }

    private BasicFileAttributes readAttributes(Path entry)
    {
        try {
            return Files.readAttributes(entry, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (NoSuchFileException e) {
            return null;
        } catch (IOException e) {
            throw failure(e);
        }
    }

    private BasicFileAttributes readLinkTargetAttributes(Path link)
    {
        try {
            return Files.readAttributes(link, BasicFileAttributes.class);
        } catch (IOException e) {
            // A broken link, a link loop or a target that cannot be read: the link isn't a blob, as Files.walk with
            // Files::isRegularFile considers it.
            return null;
        }
    }

    private void openDirectory(Path path)
    {
        try {
            DirectoryStream<Path> stream = Files.newDirectoryStream(path);
            this.openDirectories.push(new OpenDirectory(stream, stream.iterator()));
        } catch (NoSuchFileException | NotDirectoryException e) {
            // The directory doesn't exist (anymore): there is nothing to list in it.
        } catch (IOException e) {
            throw failure(e);
        }
    }

    private Blob toBlob(Path file)
    {
        List<String> segments = new ArrayList<>(this.directoryPath.getNames());
        for (Path segment : this.directory.relativize(file)) {
            segments.add(segment.toString());
        }

        return new FileSystemBlob(BlobPath.absolute(segments), file, this.store);
    }

    private UncheckedBlobStoreException failure(IOException cause)
    {
        return new UncheckedBlobStoreException(
            new BlobStoreException("Failed to list blobs in directory: " + this.directory, cause));
    }
}
