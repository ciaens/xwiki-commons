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
package org.xwiki.job.internal;

import java.util.HashMap;
import java.util.Map;

import org.xwiki.cache.Cache;
import org.xwiki.cache.event.CacheEntryListener;

/**
 * In-memory {@link Cache} for tests, with entries expiring after a lifespan measured on a clock controlled by the test.
 *
 * @param <T> the type of the cached values
 * @version $Id$
 */
class ExpiringMapCache<T> implements Cache<T>
{
    private record Entry<T>(T value, long expiration)
    {
    }

    private final Map<String, Entry<T>> entries = new HashMap<>();

    private final int lifespan;

    private long now;

    /**
     * @param lifespan the number of seconds after which an entry expires, {@code 0} or less for no expiration
     */
    ExpiringMapCache(int lifespan)
    {
        this.lifespan = lifespan;
    }

    /**
     * @param seconds the number of seconds to move the clock of the cache forward
     */
    void advance(int seconds)
    {
        this.now += seconds * 1000L;
    }

    @Override
    public void set(String key, T value)
    {
        long expiration = this.lifespan > 0 ? this.now + this.lifespan * 1000L : Long.MAX_VALUE;
        this.entries.put(key, new Entry<>(value, expiration));
    }

    @Override
    public T get(String key)
    {
        Entry<T> entry = this.entries.get(key);
        if (entry == null) {
            return null;
        }
        if (this.now >= entry.expiration()) {
            this.entries.remove(key);
            return null;
        }

        return entry.value();
    }

    @Override
    public void remove(String key)
    {
        this.entries.remove(key);
    }

    @Override
    public void removeAll()
    {
        this.entries.clear();
    }

    @Override
    public void addCacheEntryListener(CacheEntryListener<T> listener)
    {
        // Not needed by the tests.
    }

    @Override
    public void removeCacheEntryListener(CacheEntryListener<T> listener)
    {
        // Not needed by the tests.
    }

    @Override
    public void dispose()
    {
        this.entries.clear();
    }
}
