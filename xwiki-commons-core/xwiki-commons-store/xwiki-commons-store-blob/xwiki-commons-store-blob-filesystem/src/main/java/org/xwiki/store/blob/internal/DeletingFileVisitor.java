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

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Deletes a file tree of a {@link FileSystemBlobStore}. The files and directories that are deleted by someone else
 * during the walk, another thread or another cluster member sharing the directory, are skipped, so that concurrent
 * deletions of the same tree all succeed.
 *
 * @version $Id$
 * @since 18.10.0RC1
 */
final class DeletingFileVisitor extends SimpleFileVisitor<Path>
{
    private DeletingFileVisitor()
    {
    }

    /**
     * Delete the given directory with all its content. When the directory is a symbolic link to a directory, the
     * content of its target is deleted, as {@link FileSystemBlobIterator} lists it, and the link and its target
     * directory are kept so that the link stays usable. Links found inside the tree are deleted, not followed.
     *
     * @param directory the directory to delete
     * @throws IOException if a file or directory cannot be deleted for another reason than being already deleted
     */
    static void deleteTree(Path directory) throws IOException
    {
        DeletingFileVisitor visitor = new DeletingFileVisitor();
        if (Files.isSymbolicLink(directory)) {
            try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
                for (Path entry : entries) {
                    Files.walkFileTree(entry, visitor);
                }
            } catch (NoSuchFileException | NotDirectoryException e) {
                // The link or its target was deleted in the meantime: there is nothing to delete.
            }
        } else {
            Files.walkFileTree(directory, visitor);
        }
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException
    {
        Files.deleteIfExists(file);
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exception) throws IOException
    {
        if (exception instanceof NoSuchFileException) {
            return FileVisitResult.CONTINUE;
        }
        throw exception;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path directory, IOException exception) throws IOException
    {
        if (exception != null && !(exception instanceof NoSuchFileException)) {
            throw exception;
        }
        Files.deleteIfExists(directory);
        return FileVisitResult.CONTINUE;
    }
}
