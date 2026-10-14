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
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.xwiki.store.blob.Blob;
import org.xwiki.store.blob.BlobPath;
import org.xwiki.store.blob.BlobStoreException;
import org.xwiki.store.blob.UncheckedBlobStoreException;

/**
 * Lazily walks a directory of a {@link FileSystemBlobStore} and returns its regular files as blobs. A failure during
 * the walk is thrown as an {@link UncheckedBlobStoreException}. The iterator must be closed to release the open
 * directories.
 *
 * @version $Id$
 * @since 18.10.0RC1
 */
final class FileSystemBlobIterator implements Iterator<Blob>, Closeable
{
    private final FileSystemBlobStore store;

    private final Path directory;

    private final BlobPath directoryPath;

    private final Stream<Path> walk;

    private final Iterator<Path> files;

    private FileSystemBlobIterator(FileSystemBlobStore store, Path directory, BlobPath directoryPath,
        Stream<Path> walk)
    {
        this.store = store;
        this.directory = directory;
        this.directoryPath = directoryPath;
        this.walk = walk;
        this.files = walk.filter(Files::isRegularFile).iterator();
    }

    /**
     * Start walking the given directory.
     *
     * @param store the store the directory belongs to
     * @param directory the absolute and normalized file system path of the directory to walk
     * @param directoryPath the blob path corresponding to the directory
     * @return the iterator over the regular files of the directory, at every depth
     * @throws BlobStoreException if the directory cannot be read
     */
    static FileSystemBlobIterator walk(FileSystemBlobStore store, Path directory, BlobPath directoryPath)
        throws BlobStoreException
    {
        try {
            return new FileSystemBlobIterator(store, directory, directoryPath, Files.walk(directory));
        } catch (IOException e) {
            throw new BlobStoreException(getErrorMessage(directory), e);
        }
    }

    /**
     * Start walking the given directory and return the blobs as a stream, which closes the iterator when closed.
     *
     * @param store the store the directory belongs to
     * @param directory the absolute and normalized file system path of the directory to walk
     * @param directoryPath the blob path corresponding to the directory
     * @return the regular files of the directory, at every depth
     * @throws BlobStoreException if the directory cannot be read
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
        try {
            return this.files.hasNext();
        } catch (UncheckedIOException e) {
            throw failure(e.getCause());
        }
    }

    @Override
    public Blob next()
    {
        try {
            return toBlob(this.files.next());
        } catch (UncheckedIOException e) {
            throw failure(e.getCause());
        }
    }

    @Override
    public void close()
    {
        this.walk.close();
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
        return new UncheckedBlobStoreException(new BlobStoreException(getErrorMessage(this.directory), cause));
    }

    private static String getErrorMessage(Path directory)
    {
        return "Failed to list blobs in directory: " + directory;
    }
}
