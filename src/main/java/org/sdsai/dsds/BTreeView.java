package org.sdsai.dsds;

import java.util.SortedMap;
import java.util.AbstractSet;
import java.util.AbstractCollection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.Collection;
import java.util.Iterator;
import java.lang.Iterable;
import java.util.NoSuchElementException;

/**
 * A view into an existing {@link BTree}.
 */
public class BTreeView<K extends Comparable<? super K>, STOREKEY, V> implements SortedMap<K,V>
{
    private BTree<K, STOREKEY, V> btree;

    /**
     * The lowest key in this set, inclusive.
     */
    private K minKey;

    /**
     * The highest key in this set, inclusive.
     */
    private K maxKey;

    public BTreeView(final BTree<K, STOREKEY, V> btree, final K minKey, final K maxKey)
    {
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.btree  = btree;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Comparator<? super K> comparator()
    {
        return btree.comparator();
    }

    /**
     * Return a {@link BTreeLocation} wrapped in an iterator.
     * This will call {@link BTreeLocation#hasNext()} and
     * {@link BTreeLocation#next()} to iterate through this BTree.
     */
    public Iterator<K> getIterator()
    {
        return new Iterator<K>()
        {
            private BTreeLocation<K, STOREKEY> nextState = btree.getLocation(minKey);

            public boolean hasNext()
            {
                return nextState != null && nextState.hasNext();
            }

            public K next()
            {
                nextState = nextState.next();

                if ( nextState == null )
                    throw new NoSuchElementException();

                return nextState.getKey();
            }

            /**
             * Unsupported operation.
             * @throws UnsupportedOperationException
             */
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Return a {@link BTreeLocation} wrapped in an iterator.
     * This will call {@link BTreeLocation#prev()} and
     * {@link BTreeLocation#hasPrev()} to iterator through the BTree
     * in reverse.
     */
    public Iterator<K> getReverseIterator()
    {
        return new Iterator<K>()
        {
            private BTreeLocation<K, STOREKEY> prevState = btree.getLocation(maxKey);

            public boolean hasNext()
            {
                return prevState != null && prevState.hasPrev();
            }

            public K next()
            {
                prevState = prevState.prev();

                if ( prevState == null )
                    throw new NoSuchElementException();

                return prevState.getKey();
            }

            /**
             * Unsupported operation.
             * @throws UnsupportedOperationException
             */
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Map.Entry<K,V>> entrySet()
    {
        /* FIXME - size is wrong. */
        return btree.entrySet(BTreeView.this.getIterator());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public K firstKey()
    {
        /* FIXME - validate range. */
        return btree.firstKey();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SortedMap<K,V> headMap(K toKey)
    {
        /* FIXME - validate range. */
        return btree.headMap(toKey);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public K lastKey()
    {
        /* FIXME - validate range. */
        return btree.lastKey();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SortedMap<K,V> subMap(K fromKey, K toKey)
    {
        /* FIXME - validate range. */
        return btree.subMap(fromKey, toKey);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SortedMap<K,V> tailMap(K fromKey)
    {
        /* FIXME - validate value. */
        return btree.tailMap(fromKey);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<V> values()
    {
        /* FIXME - size is wrong. */
        return btree.values(getIterator());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear()
    {
        for (final Iterator<K> itr = getIterator(); itr.hasNext(); )
        {
            btree.remove(itr.next());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsKey(Object key)
    {
        /* FIXME - in range. */
        return btree.containsKey(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsValue(Object value)
    {
        /* FIXME - in range. */
        return btree.containsValue(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o)
    {
        if ( o == null )
        {
            return false;
        }

        if ( ! ( o instanceof BTreeView ) )
        {
            return false;
        }

        @SuppressWarnings("unchecked")
        final BTreeView<? extends K, ? extends STOREKEY, ? extends V> that =
            (BTreeView<? extends K, ? extends STOREKEY, ? extends V>) o;

        return this.minKey.equals(that.minKey) &&
               this.maxKey.equals(that.maxKey) &&
               this.btree.equals(that.btree);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V get(Object key)
    {
        /* FIXME - check range */
        return btree.get(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return btree.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty()
    {
        return size() == 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<K> keySet()
    {
        return new AbstractSet<K>()
        {
            @Override
            public Iterator<K> iterator()
            {
                return BTreeView.this.getIterator();
            }

            @Override
            public int size()
            {
                return BTreeView.this.size();
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V put(final K key, final V value)
    {
        return btree.put(key, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putAll(final Map<? extends K,? extends V> m)
    {
        btree.putAll(m);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V remove(Object key)
    {
        /* FIXME - check if key is in range. */
        return btree.remove(key);
    }

    public boolean inRange(final Object o)
    {
        if (o == null)
        {
            return false;
        }

        if (o == minKey)
        {
            return true;
        }

        if (o == maxKey)
        {
            return true;
        }

        if (minKey.getClass().isInstance(o))
        {
            @SuppressWarnings("unchecked")
            final K k = (K)o;

            return inRange(k);
        }

        return false;
    }

    public boolean inRange(final K key)
    {
        return minKey.compareTo(key) <= 0 && maxKey.compareTo(key) >= 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size()
    {
        int i = 0;

        for (final Iterator<K> itr = getIterator(); itr.hasNext(); itr.next())
        {
            ++i;
        }

        return i;
    }
}