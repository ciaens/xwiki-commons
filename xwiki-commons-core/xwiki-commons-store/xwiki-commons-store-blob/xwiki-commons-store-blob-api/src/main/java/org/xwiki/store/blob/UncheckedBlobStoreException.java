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
package org.xwiki.store.blob;

import java.io.Serial;
import java.util.Objects;

import org.xwiki.stability.Unstable;

/**
 * Wraps a {@link BlobStoreException} with an unchecked exception. It is thrown where a checked exception cannot be,
 * for example while iterating the stream returned by {@link BlobStore#listDescendants(BlobPath)}.
 *
 * @version $Id$
 * @since 18.10.0RC1
 */
@Unstable
public class UncheckedBlobStoreException extends RuntimeException
{
    /**
     * Provides an id for serialization.
     */
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new exception with the specified cause.
     *
     * @param cause the {@link BlobStoreException} to wrap, not {@code null}
     */
    public UncheckedBlobStoreException(BlobStoreException cause)
    {
        super(Objects.requireNonNull(cause));
    }

    /**
     * Constructs a new exception with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause the {@link BlobStoreException} to wrap, not {@code null}
     */
    public UncheckedBlobStoreException(String message, BlobStoreException cause)
    {
        super(message, Objects.requireNonNull(cause));
    }

    /**
     * @return the wrapped {@link BlobStoreException}
     */
    @Override
    public synchronized BlobStoreException getCause()
    {
        return (BlobStoreException) super.getCause();
    }
}
