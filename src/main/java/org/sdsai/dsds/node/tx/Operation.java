package org.sdsai.dsds.node.tx;

import org.sdsai.dsds.node.Node;
import org.sdsai.dsds.node.NodeStore;
import org.sdsai.dsds.node.NodeStoreException;
import org.sdsai.dsds.node.NodeStoreNodeNotFoundException;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.HashMap;

/**
 * An Operation is write, read, or other activity on a {@link NodeStore}
 * which is included in a {@link Transaction}.
 */
public abstract class Operation<KEY, VALUE>
{
    private boolean deleted;
    private KEY key;
    private VALUE value;

    /**
     * Create a new operation with key and value set to null.
     */
    public Operation()
    {
        this(null, null);
    }

    /**
     * Create a new operation with key and value set to the given parameters.
     */
    public Operation(final KEY key, final VALUE value)
    {
        this.deleted = false;
        this.key = key;
        this.value = value;
    }

    /**
     * Return the value of this operation if it is successful.
     *
     * The null value is appropriate if this is a write operation where there is no data returned.
     *
     * @return VALUE of operation or null.
     */
    public VALUE getValue()
    {
        return value;
    }

    /**
     * Return a unique identifier that allows for tracking of this operation.
     */
    public KEY getKey()
    {
        return key;
    }

    /**
     * Delete this operation.
     *
     * An operation structure typically has a lot of data associated with it. When a future operation
     * obviates a previous operation that previous operation is not removed from the {@link Transaction}
     * structure. Rather, its {@link #delete()} method is called to release all unecessary data
     * and mark the Operation as deleted.
     *
     * Deleted operations will not be executed when the {@link Transaction} is committed.
     */
    public void delete()
    {
        deleted = true;
        value = null;
    }

    /**
     * Return if this operation is deleted.
     */
    public boolean isDeleted()
    {
        return deleted;
    }

    /**
     * Execute this operation if {@link #delete()} has not been called on it.
     *
     * @throws RuntimeException or other unchecked exceptions to communicate failure.
     */
    public void execute()
    {
        if ( ! deleted )
        {
            doExecute();
        }
    }

    /**
     * Implementing classes should implement this method.
     *
     * This is conditionally called by {@link #execute()} if {@link #delete()} 
     * has not been previously called.
     *
     * @throws RuntimeException or other unchecked exceptions to communicate failure.
     */
    protected abstract void doExecute();

}
