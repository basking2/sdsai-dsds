package com.github.basking2.sdsai.dsds.node.tx;

/**
 * This operation holds a result. Its doExecute operation is empty.
 */
public class CachedReadOperation<KEY, VALUE> extends Operation<KEY, VALUE>
{
    public CachedReadOperation(final KEY key, final VALUE value)
    {
        super(key, value);
    }

    protected void doExecute()
    {
    }
}
