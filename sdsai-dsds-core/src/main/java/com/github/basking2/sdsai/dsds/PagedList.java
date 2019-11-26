/**
 * Copyright (c) 2011, Samuel R. Baskinger <basking2@yahoo.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a 
 * copy  of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation 
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, 
 * and/or sell copies of the Software, and to permit persons to whom the 
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included 
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS 
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
 * DEALINGS IN THE SOFTWARE.
 */
package com.github.basking2.sdsai.dsds;

import com.github.basking2.sdsai.dsds.node.Node;
import com.github.basking2.sdsai.dsds.node.NodeFunction;
import com.github.basking2.sdsai.dsds.node.NodeStore;
import com.github.basking2.sdsai.dsds.node.NodeStoreNodeNotFoundException;

import java.util.List;
import java.util.Collection;
import java.util.Iterator;
import java.util.ListIterator;

import static java.util.Arrays.fill;

/**
 * A circularly linked list in which each list element many data elements.
 * This list does very well for in-order aggregation but
 * quickly fragments if elements are inserted at arbitrary indexes.
 */
public class PagedList<STOREKEY, V>
implements List<V>
{
    /**
     */
    private STOREKEY headKey;

    /**
     */
    private NodeStore<STOREKEY, STOREKEY, V> nodeStore;
    
    /**
     */
    private int pageSize;
    
    public PagedList(final STOREKEY headKey,
                     final NodeStore<STOREKEY, STOREKEY, V> nodeStore,
                     final int pageSize)
    {
        this.pageSize = pageSize;
        this.nodeStore = nodeStore;
        this.headKey = headKey;
        
        // Force a loading/creation of the node.
        getHead();
    }
    
    public PagedList(final STOREKEY headKey,
                     final NodeStore<STOREKEY, STOREKEY, V> nodeStore)
    {
        this(headKey, nodeStore, 100);
    }
    
    private Node<STOREKEY, STOREKEY> getHead()
    {
        Node<STOREKEY, STOREKEY> root;
        
        try
        {
            root = nodeStore.loadNode(headKey);
            pageSize = root.getDataCap();
        }
        catch ( final NodeStoreNodeNotFoundException e)
        {
            root = newNode(headKey, headKey);
            nodeStore.store(headKey, root);
        }
        
        return root;
    }

    private Node<STOREKEY, STOREKEY> 
    newNode(final STOREKEY prev, final STOREKEY next)
    {
        // child, data, ancestors
        final Node<STOREKEY, STOREKEY> n =
            new Node<STOREKEY, STOREKEY>(1, pageSize, 1);
        n.getAncestors().add(prev);
        n.getChildren().add(next);
        return n;
    }
    
    private boolean isHead(final STOREKEY storeKey)
    {
        return headKey.equals(storeKey);
    }
    
    private STOREKEY nextKey(final Node<STOREKEY, STOREKEY> node)
    {
        return node.getChildren().get(0);
    }
    
    private STOREKEY prevKey(final Node<STOREKEY, STOREKEY> node)
    {
        return node.getAncestors().get(0);
    }
    
    private int pageFill(final Node<STOREKEY, STOREKEY> node)
    {
        return node.getData().size();
    }
    
    public void eachPage(final NodeFunction<STOREKEY, STOREKEY> nodeFunction)
    {
        eachPage(nodeFunction, headKey);
    }
    
    public void eachPage(final NodeFunction<STOREKEY, STOREKEY> nodeFunction,
                         final STOREKEY startKey)
    {
        STOREKEY nextKey = startKey;

        do
        {
            final Node<STOREKEY, STOREKEY> node = nodeStore.loadNode(nextKey);
            
            if ( !nodeFunction.call(node) )
                return;
            
            nextKey = nextKey(node);
        }            
        while ( ! startKey.equals(nextKey) );
    }
    
    public Iterator<Node<STOREKEY, STOREKEY>> reversePageIterator()
    {
        return new Iterator<Node<STOREKEY, STOREKEY>>()
        {
            STOREKEY nextKey = prevKey(getHead());
            final STOREKEY stopKey = nextKey;
            
            public boolean hasNext()
            {
                return nextKey != null;
            }
            
            public Node<STOREKEY, STOREKEY> next()
            {
                final Node<STOREKEY, STOREKEY> n = nodeStore.loadNode(nextKey);
                
                nextKey = PagedList.this.prevKey(n);
                
                if ( stopKey.equals(nextKey) )
                    nextKey = null;
                
                return n;
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
    
    public Iterator<Node<STOREKEY, STOREKEY>> pageIterator()
    {
        return new Iterator<Node<STOREKEY, STOREKEY>>()
        {
            STOREKEY nextKey = headKey;
            
            public boolean hasNext()
            {
                return nextKey != null;
            }
            
            public Node<STOREKEY, STOREKEY> next()
            {
                final Node<STOREKEY, STOREKEY> n = nodeStore.loadNode(nextKey);
                
                nextKey = PagedList.this.nextKey(n);
                
                if ( headKey.equals(nextKey) )
                    nextKey = null;
                
                return n;
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
     * Used when appending data. This will
     * create an empty list page with the assumption that future
     * inserts will populate it. Splitting may result in a list
     * that is at 1/2 capacity.
     */
    private PagedListLocation<STOREKEY> insertPage(final STOREKEY prevKey,
                                                   final Node<STOREKEY, STOREKEY> prevNode,
                                                   final STOREKEY nextKey,
                                                   final Node<STOREKEY, STOREKEY> nextNode)
    {
        final Node<STOREKEY, STOREKEY> node = newNode(prevKey, nextKey);
        final STOREKEY key = nodeStore.generateKey(node, null);
        
        if ( prevKey.equals(nextKey) )
        {
            // When a list is small, prev and next can be the same.
            // We update both objects in memory but only store one.
            prevNode.getChildren().set(0, key);
            nextNode.getChildren().set(0, key);
            prevNode.getAncestors().set(0, key);
            nextNode.getAncestors().set(0, key);
            
            nodeStore.store(prevKey, prevNode);
        }
        else
        {
            prevNode.getChildren().set(0, key);
            nextNode.getAncestors().set(0, key);
            
            nodeStore.store(prevKey, prevNode);
            nodeStore.store(nextKey, nextNode);
        }
        
        nodeStore.store(key, node);
        
        return new PagedListLocation<STOREKEY>(nodeStore, key, node);
    }
    
    /**
     * Splits the given list node and inserts a
     * new page after it. That new PagedListLocation is returned.
     */
    private PagedListLocation<STOREKEY> split(final STOREKEY prevKey,
                                              final Node<STOREKEY, STOREKEY> prevNode,
                                              final STOREKEY nextKey,
                                              final Node<STOREKEY, STOREKEY> nextNode    )
    {
        final Node<STOREKEY, STOREKEY> node = newNode(prevKey, nextKey);

        final STOREKEY key = nodeStore.generateKey(node, null);
        
        final List<STOREKEY> l = 
            prevNode.getData().subList(pageSize/2, prevNode.getData().size());

        // Move data into new node
        node.getData().addAll(l);

        // Unlink data from old node.
        l.clear();

        // Relink the outer nodes and store them.
        if ( prevKey.equals(nextKey) )
        {
            // When a list is small, prev and next can be the same.
            // We update both objects in memory but only store one.
            prevNode.getChildren().set(0, key);
            nextNode.getChildren().set(0, key);
            prevNode.getAncestors().set(0, key);
            nextNode.getAncestors().set(0, key);
            
            nodeStore.store(prevKey, prevNode);
        }
        else
        {
            prevNode.getChildren().set(0, key);
            nextNode.getAncestors().set(0, key);
            
            nodeStore.store(prevKey, prevNode);
            nodeStore.store(nextKey, nextNode);
        }
        
        nodeStore.store(key, node);
        
        return new PagedListLocation<STOREKEY>(nodeStore, key, node);
    }
    
    /**
     * <p>Merge {@code loc} into {@code prevLoc}.</p>
     *
     * <p>If {@link PagedListLocation#getKey()} returns equal values
     *    from both {@code loc} and {@code prevLoc}, then no merge is 
     *    performed.</p>
     *
     * <p>If {@code loc}'s key is the head key ({@code loc} is the head node
     *    of the list) then {@code loc}'s key is swapped with {@code prevLoc}'s
     *    key.</p>
     *
     * <p>Upon exit, {@code prevLoc} is left in a consistent state.
     *    The other arguments have no such garuntee and should be discarded.
     * </p>
     */
    private void merge( final PagedListLocation<STOREKEY> prevLoc,
                        final PagedListLocation<STOREKEY> loc,
                        PagedListLocation<STOREKEY> nextLoc)
    {
        // Don't merge the same node into itself.
        // This also catches lists of page-size=1.
        if ( prevLoc.getKey().equals(loc.getKey()) )
            return;
        
        if ( loc.size() > 0 )
            prevLoc.getNode().getData().addAll(loc.getNode().getData());

        // If next and prev are equals, we are producing a single node list.
        // This changes how we updated and store nodes.
        // NOTE: Because we cannot merge loc into prevLoc 
        //       (previous if check), merges to a single node will always
        //       have prevLoc and nextLoc share a key.
        if ( prevLoc.getKey().equals(nextLoc.getKey()) )
        {
            prevLoc.getNode().getChildren().set(0, prevLoc.getKey());
            prevLoc.getNode().getAncestors().set(0, prevLoc.getKey());
        }
        else
        {
            prevLoc.getNode().getChildren().set(0, nextLoc.getKey());
            nextLoc.getNode().getAncestors().set(0, prevLoc.getKey());
            nextLoc.store();
        }
        
        prevLoc.store();
        loc.remove();
    }

    /**
     * Return a {@link PagedListLocation} that is the last page in the
     * list with it's index pointed at {@link PagedListLocation#size()}.
     */
    private PagedListLocation<STOREKEY> findLastInsertionPoint()
    {
        final PagedListLocation<STOREKEY> ctx = 
            new PagedListLocation<STOREKEY>(nodeStore, headKey).prev();
        
        return ctx.index(ctx.size());
    }

    /**
     * Walk along each {@link PagedListLocation} until
     * the given index is found.
     * @throws ArrayIndexOutOfBoundsException if this index exceeds
     *         the size of this list. That is, the insertionPoint
     *         may be list.size() which indicates that the item 
     *         will be inserted at the end of the list as the last element.
     */
    private PagedListLocation<STOREKEY> findInsertionPoint(int index)
    {
        PagedListLocation<STOREKEY> ctx = 
            new PagedListLocation<STOREKEY>(nodeStore, headKey);
        
        // NOTE: > not >=.
        while (index > ctx.size())
        {
            index -= ctx.size();
            ctx = ctx.next();

            if ( headKey.equals(ctx.getKey()) )
            {
                // NOTE: Last index, at this point, will be the size of the list.
                throw new ArrayIndexOutOfBoundsException(
                    "Index: "+index+" Size: "+size());
            }
        }
        
        return ctx.index(index);
    }
    
    
    /**
     * Seek to a given location and return that {@link PagedListLocation}.
     * This is different than {@link #findInsertionPoint(int)} and
     * {@link #findLastInsertionPoint()} in that this returns a
     * {@link PagedListLocation} that points at a valid index where as
     * insertion points may point at an element beyond the end of a list.
     */
    private PagedListLocation<STOREKEY> seek(int index)
    {
        PagedListLocation<STOREKEY> ctx = 
            new PagedListLocation<STOREKEY>(nodeStore, headKey);
        
        // NOTE: >= not >.
        while (index >= ctx.size())
        {
            index -= ctx.size();
            ctx = ctx.next();

            if ( headKey.equals(ctx.getKey()) )
            {
                // NOTE: Last index, at this point, will be the size of the list.
                throw new ArrayIndexOutOfBoundsException(
                    "Index: "+index+" Size: "+size());
            }
        }
        
        return ctx.index(index);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean add(final V value)
    {
        PagedListLocation<STOREKEY> ctx = findLastInsertionPoint();
        
        add(ctx, value);
        
        return true;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void add(final int index, final V value)
    {
        PagedListLocation<STOREKEY> ctx = findInsertionPoint(index);
        
        add(ctx, value);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean addAll(final Collection<? extends V> c)
    {
        if ( c.isEmpty() )
            return false;
    
        return addAll(findLastInsertionPoint(), c);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean addAll(final int index, final Collection<? extends V> c)
    {
        if ( c.isEmpty() )
            return false;
            
        return addAll(findInsertionPoint(index), c);
    }
    
    /**
     * This assumes that ctx.size() >= pageSize and will
     * return a new PagedListLocation with its index positioned at the next
     * insertion point.
     */
    private PagedListLocation<STOREKEY> nextInsertionPoint(final PagedListLocation<STOREKEY> ctx)
    {
        return nextInsertionPoint( ctx, ctx.next() );
    }
    
    /**
     * This assumes that ctx.size() >= pageSize and will
     * return a new PagedListLocation with its index positioned at the next
     * insertion point.
     */
    private PagedListLocation<STOREKEY> nextInsertionPoint(
        final PagedListLocation<STOREKEY> ctx, 
        final PagedListLocation<STOREKEY> nxt)
    {
        // We want to insert at the end of a node but can't.
        // Instead of prepending to the next node we make an empty
        // node to continue appending to.
        if ( ctx.getIndex() == ctx.size() )
        {
            // Similar to appending, when bulk inserting we
            // append to the end of an empty page instead
            // of inserting into the beginning of a new one.
            return insertPage(ctx.getKey(),
                             ctx.getNode(),
                             nxt.getKey(),
                             nxt.getNode());
        }
        else 
        {
            final int insertionPoint = ctx.getIndex();
            
            // We've run out of space inserting into the middle
            // of a node. Split that node.
            final PagedListLocation<STOREKEY> ctx2 = split(ctx.getKey(),
                                                           ctx.getNode(),
                                                           nxt.getKey(),
                                                           nxt.getNode());
            if ( insertionPoint <= ctx.size() )
            {
                return ctx.index(insertionPoint);
            }
            else
            {
                return ctx2.index(insertionPoint - ctx.size());
            }
        }
    }
    
    private boolean addAll(PagedListLocation<STOREKEY> ctx,
                           final Collection<? extends V>  c)
    {
        for (final V v : c)
        {
            final STOREKEY vkey = nodeStore.generateKey(null, v);
            
            // If there is no room we must insert an empty page
            // replacing ctx with it.
            if ( ctx.size() >= pageSize )
            {
                ctx = nextInsertionPoint(ctx);
            }

            ctx.getNode().getData().add(ctx.getIndex(), vkey);

            ctx.index(ctx.getIndex()+1);
            
            nodeStore.store(vkey, v);
        }
        
        // Save our work.
        nodeStore.store(ctx.getKey(), ctx.getNode());
        
        return true;
    }
    
    private boolean add(PagedListLocation<STOREKEY> ctx, final V v)
    {
        final STOREKEY vkey = nodeStore.generateKey(null, v);
        
        // If there is no room we must insert an empty page
        // replacing ctx with it.
        if ( ctx.size() >= pageSize )
        {
            final PagedListLocation<STOREKEY> nxt = ctx.next();
            
            // An optimization for inserting single items.
            // When inserting a single item at the end of a full node,
            // we may insert the next value at the beginning of the next
            // node if that node has room. Otherwise we do our normal 
            // new page construction.
            if ( nxt.size() < pageSize && ctx.getIndex() == ctx.size())
            {
                ctx = nxt.index(0);
            }
            else
            {
                ctx = nextInsertionPoint(ctx, nxt);
            }
        }

        ctx.getNode().getData().add(ctx.getIndex(), vkey);

        ctx.index(ctx.getIndex()+1);
        
        nodeStore.store(vkey, v);

        // Save our work.
        nodeStore.store(ctx.getKey(), ctx.getNode());
        
        return true;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void clear()
    {
        Node<STOREKEY, STOREKEY> head = getHead();
        STOREKEY nextKey = nextKey(head);
        while ( ! headKey.equals(nextKey) )
        {
            Node<STOREKEY, STOREKEY> node = nodeStore.loadNode(nextKey);

            for ( final STOREKEY k : node.getData() )
                nodeStore.removeData(k);
                
            nodeStore.removeNode(nextKey);
            
            nextKey = nextKey(node);
        }
        
        // When here, we've walked around the loop and are back at the head.

        // Clear all data.
        for ( final STOREKEY k : head.getData() )
            nodeStore.removeData(k);

        head.getData().clear();
        head.getChildren().set(0, headKey);
        head.getAncestors().set(0, headKey);
        nodeStore.store(headKey, head);
    }
    
    /**
     * Clear this list with {@link #clear()} and then delete the 
     * empty head node.
     */
    public void destroy()
    {
        clear();
        
        nodeStore.removeNode(headKey);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(final Object o)
    {
        final Iterator<Node<STOREKEY, STOREKEY>> pages = pageIterator();
        
        while (pages.hasNext())
        {
            final Node<STOREKEY, STOREKEY> node = pages.next();
            
            for (final STOREKEY k : node.getData())
            {
                if ( o.equals(nodeStore.loadData(k)) )
                    return true;
            }
        }
        
        return false;
    }
   
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsAll(final Collection<?> c)
    {
        final boolean[] foundVector = new boolean[c.size()];
        final Object[]  findVector = c.toArray();
        
        fill(foundVector, false);
        
        final Iterator<Node<STOREKEY, STOREKEY>> pages = pageIterator();
        
        while (pages.hasNext())
        {
            final Node<STOREKEY, STOREKEY> node = pages.next();
            
            for (final STOREKEY k : node.getData())
            {
                for (int i = 0; i < findVector.length; i++)
                {
                    if ( findVector[i].equals(nodeStore.loadData(k)) )
                    {
                        foundVector[i] = true;
                    }
                }
            }
        }
        
        for ( final boolean b : foundVector )
            if ( ! b )
                return false;
        
        return true;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object o)
    {
        if ( o == null )
            return false;
            
        if ( ! ( o instanceof PagedList ) )
            return false;
            
        return ((PagedList)o).headKey.equals(headKey);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public V get(int index)
    {
        final int originalSize = index;
        final Iterator<Node<STOREKEY, STOREKEY>> pages = pageIterator();
        
        while (pages.hasNext())
        {
            final Node<STOREKEY, STOREKEY> node = pages.next();
            
            if ( index < node.getData().size() )
            {
                return nodeStore.loadData(node.getData().get(index));
            }
            
            index -= node.getData().size();
        }
        
        throw new IndexOutOfBoundsException(
            "Index "+originalSize+" in list of size "+size());
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty()
    {
        return getHead().getData().isEmpty();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int indexOf(Object o)
    {
        final Iterator<Node<STOREKEY, STOREKEY>> pages = pageIterator();

        int index = 0;
        
        while (pages.hasNext())
        {
            final Node<STOREKEY, STOREKEY> node = pages.next();
            
            for (int i = 0; i < node.getData().size(); i++)
            {
                if (o.equals(nodeStore.loadData(node.getData().get(i))))
                {
                    return index + i;
                }
            }
            
            index += node.getData().size();
        }
        
        return -1;
    }

    /**
     * {@inheritDoc}
     */    
    @Override
    public Iterator<V> iterator()
    {
        return new Iterator<V>()
        {
            private PagedListLocation<STOREKEY> location = 
                new PagedListLocation<STOREKEY>(nodeStore, headKey);
            
            public boolean hasNext()
            {
                if ( location == null )
                    return false;
                    
                // There is more data.
                if ( location.getIndex() < location.size() )
                    return true;
                    
                // There are more nodes (with data).
                if ( ! headKey.equals(location.nextKey()) )
                    return true;
                    
                return false;
            }
            
            public V next()
            {
                final V v = nodeStore.loadData(
                    location.getNode().getData().get(location.getIndex()));
                
                // Advance the location.
                location.index(location.getIndex()+1);
                
                // Did we run out of data.
                if ( location.getIndex() >= location.size() )
                {
                    // Next index 0.
                    location = location.next().index(0);
                    
                    // If we are back at the head, we are done.
                    if ( location.getKey().equals(headKey) )
                        location = null;
                }
                
                return v;
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
    public int lastIndexOf(final Object o)
    {
        final Iterator<Node<STOREKEY, STOREKEY>> pages = reversePageIterator();

        int index = size();
        
        while (pages.hasNext())
        {
            final Node<STOREKEY, STOREKEY> node = pages.next();
            
            index -= node.getData().size();
            
            for (int i = node.getData().size()-1; i >= 0; i--)
            {
                if (o.equals(nodeStore.loadData(node.getData().get(i))))
                {
                    return index + i;
                }
            }
        }
        
        return -1;
    }
    
    // FIXME
    @Override
    public ListIterator<V> listIterator()
    {
                throw new UnsupportedOperationException();
    }
    
    // FIXME
    @Override
    public ListIterator<V> listIterator(final int index)
    {
                throw new UnsupportedOperationException();
    }
    
    
    /**
     * Used by the remove family of methods to
     * remove the data from the {@link NodeStore}.
     * See {@link #removeFrom(PagedListLocation)} for removing the
     * data from the list structure.
     */
    private V removeData(final PagedListLocation<STOREKEY> loc)
    {
        final STOREKEY valKey = loc.getNode().getData().get(loc.getIndex());

        final V val = nodeStore.loadData(valKey);

        nodeStore.removeData(valKey);

        return val;
    }
    
    /**
     * Remove the given location and returns a location
     * positioned at the same global list index that was removed.
     * The actual object may have a different result for 
     * {@link PagedListLocation#getIndex()} or may be a different object.
     */
    private PagedListLocation<STOREKEY>
    removeFrom(final PagedListLocation<STOREKEY> loc)
    {
        // Actually do the removal.
        loc.getNode().getData().remove(loc.getIndex());
        
        final PagedListLocation<STOREKEY> prev = loc.prev();
        
        final PagedListLocation<STOREKEY> next = loc.next();
        
        // Can we merge with prev?
        if ( !headKey.equals(loc.getKey())
          && loc.size() + prev.size() < pageSize 
          && ! loc.getKey().equals(prev.getKey()))
        {
        
            int i = prev.getNode().getData().size() + loc.getIndex();

            // This will cause a write of the nodes.
            merge(prev, loc, next);

            if ( i < prev.size()) {
                // If i < prev.size() we did not delete the last element. 
                return prev.index(i);
            } else {
                // We did delete the last element. Our index is in the next node.
                return prev.next().index(0);
            }
            
        } // Can we merge with next?
        else if ( !headKey.equals(next.getKey())
               && loc.size() + next.size() < pageSize
               && ! loc.getKey().equals(prev.getKey()))
        {
            final PagedListLocation<STOREKEY> nextNext = next.next();
            
            // This will cause a write of the nodes.
            merge(loc, next, nextNext);

            return loc;
        }
        else
        {
            nodeStore.store(loc.getKey(), loc.getNode());
            
            return loc;
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public V remove(final int index)
    {
        final PagedListLocation<STOREKEY> l = seek(index);
        final V value = removeData(l);
        removeFrom(l);
        return value;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(final Object o)
    {
        // FIXME - could be faster without the index call and then remove.
        final int i = indexOf(o);
        
        if ( i < 0 )
            return false;
        
        remove(i);
        
        return true;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean removeAll(final Collection<?> c)
    {
        if ( c.isEmpty() )
            return false;
            
        boolean altered = false;

        for ( Object o : c )
        {
            altered = altered || remove(o);
        }
        
        return altered;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean retainAll(final Collection<?> c)
    {
        if ( c.isEmpty() )
            return false;
            
        boolean altered = false;
            
        PagedListLocation<STOREKEY> loc =
            new PagedListLocation<STOREKEY>(nodeStore, headKey);

        do
        {
            for ( int i = 0; i < loc.size(); i++ )
            {
                final V v = nodeStore.loadData(loc.getNode().getData().get(i));
                
                if (!c.contains(v))
                {
                    removeData(loc);
                    loc = removeFrom(loc);
                    altered = true;
                }
            }
            
            loc = loc.next();
        }
        while( ! loc.getKey().equals(headKey) );
        
        return altered;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public V set(final int index, final V value)
    {
        final PagedListLocation<STOREKEY> ctx = seek(index);
        
        final STOREKEY k = ctx.getNode().getData().get(ctx.getIndex());

        final V v = nodeStore.loadData(k);
        
        nodeStore.store(k, value);
        
        return v;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int size()
    {
        final Iterator<Node<STOREKEY, STOREKEY>> pages = pageIterator();

        int index = 0;;
        
        while (pages.hasNext())
        {
            index += pages.next().getData().size();
        }
        
        return index;
    }
    
    // FIXME
    @Override
    public List<V> subList(final int fromIndex, final int toIndex)
    {
                throw new UnsupportedOperationException();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Object[] toArray()
    {
        int size = size();
        
        Object[] o = new Object[size];
        
        int i = 0;
        
        for( final V v : this )
        {
            o[i++] = v;
        }
        
        return o;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] t)
    {
        int size = size();
        
        if ( t == null || t.length < size )
        {
            t = (T[]) new Object[size];
        }
        
        int i = 0;
        
        for( final V v : this )
        {
            t[i++] = (T) v;
        }
        
        return t;
    }
}

