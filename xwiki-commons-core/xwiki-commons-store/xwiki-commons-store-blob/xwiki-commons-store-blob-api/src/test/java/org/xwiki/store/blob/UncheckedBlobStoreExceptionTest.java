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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link UncheckedBlobStoreException}.
 *
 * @version $Id$
 */
class UncheckedBlobStoreExceptionTest
{
    @Test
    void wrapsCause()
    {
        BlobStoreException cause = new BlobStoreException("listing failed");

        UncheckedBlobStoreException exception = new UncheckedBlobStoreException(cause);

        assertSame(cause, exception.getCause());
        assertEquals(BlobStoreException.class.getName() + ": listing failed", exception.getMessage());
    }

    @Test
    void wrapsCauseWithMessage()
    {
        BlobStoreException cause = new BlobStoreException("listing failed");

        UncheckedBlobStoreException exception = new UncheckedBlobStoreException("message", cause);

        assertSame(cause, exception.getCause());
        assertEquals("message", exception.getMessage());
    }

    @Test
    void rejectsNullCause()
    {
        assertThrows(NullPointerException.class, () -> new UncheckedBlobStoreException(null));
        assertThrows(NullPointerException.class, () -> new UncheckedBlobStoreException("message", null));
    }
}
