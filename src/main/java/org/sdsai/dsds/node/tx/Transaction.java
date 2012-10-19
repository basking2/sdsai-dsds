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
 * This represents a transaction against a {@link NodeStore}.
 *
 * Operations are stored cached internally until {@link #commit()} is called or {@link #rollback()} causes 
 * the data to be abandoned.
 *
 * A transaction. This is implemented as a NodeStore in which methods not related
 * to the transaction are directly proxied to the {@link NodeStore} used to
 * initialize this object.
 */
public class Transaction<USERKEY, STOREKEY, VALUE>
    implements NodeStore<USERKEY, STOREKEY, VALUE>
{
    private NodeStore<USERKEY, STOREKEY, VALUE> nodeStore;

    private List<Operation<?, ?>> operations;

    private Map<STOREKEY, Operation<STOREKEY, VALUE>> valueReads;

    private Map<STOREKEY, Operation<STOREKEY, Node<USERKEY, STOREKEY>>> nodeReads;

    private Map<STOREKEY, Operation<STOREKEY, VALUE>> valueWrites;

    private Map<STOREKEY, Operation<STOREKEY, Node<USERKEY, STOREKEY>>> nodeWrites;

    /**
     * Create a new transaction that operates against the given {@link NodeStore}.
     */
    public Transaction(final NodeStore<USERKEY, STOREKEY, VALUE> nodeStore)
    {
        this.nodeStore = nodeStore;
        this.operations = new LinkedList<Operation<?,?>>();
        this.valueReads = new HashMap<STOREKEY, Operation<STOREKEY, VALUE>>();
        this.valueWrites = new HashMap<STOREKEY, Operation<STOREKEY, VALUE>>();
        this.nodeReads = new HashMap<STOREKEY, Operation<STOREKEY, Node<USERKEY, STOREKEY>>>();
        this.nodeWrites = new HashMap<STOREKEY, Operation<STOREKEY, Node<USERKEY, STOREKEY>>>();
    }

    public static interface SubTx<USERKEY, STOREKEY, VALUE>
    {
        void buildTx(Transaction<USERKEY, STOREKEY, VALUE> tx);
    }

    /**
     * Give the user a sub-transaction which is merged into this transaction when {@link SubTx#buildTx(Transaction)} returns.
     */
    public void subTransaction(final SubTx<USERKEY, STOREKEY, VALUE> subTx)
    {
        final Transaction<USERKEY, STOREKEY, VALUE> tx = new Transaction<USERKEY, STOREKEY, VALUE>(nodeStore);

        subTx.buildTx(tx);

        operations.addAll(tx.operations);
        valueReads.putAll(tx.valueReads);
        nodeReads.putAll(tx.nodeReads);
        valueWrites.putAll(tx.valueWrites);
        nodeWrites.putAll(tx.nodeWrites);
    }

    /**
     * Rollback and clear all internal structures of this transaction.
     */
    public void rollback()
    {
        operations.clear();
        valueReads.clear();
        valueWrites.clear();
        nodeReads.clear();
        nodeWrites.clear();
    }

    /**
     * Execute all operations in this transaction.
     */
    public void commit()
    {
        for (final Operation<?, ?> o : operations)
        {
            o.execute();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public STOREKEY convert(final USERKEY key)
    {
        return nodeStore.convert(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public STOREKEY generateKey(final Node<USERKEY,STOREKEY> node, final VALUE value)
    {
        return nodeStore.generateKey(node, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public VALUE loadData(final STOREKEY key)
    {
        final Operation<STOREKEY, VALUE> readOperation = valueReads.get(key);

        if ( readOperation == null ) 
        {
            final VALUE value = nodeStore.loadData(key);

            valueReads.put(key, new CachedReadOperation<STOREKEY, VALUE>(key, value));

            return value;
        }
        else
        {
            return readOperation.getValue();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Node<USERKEY, STOREKEY> loadNode(final STOREKEY key)
    {
        final Operation<STOREKEY, Node<USERKEY, STOREKEY>> readOperation = nodeReads.get(key);

        if ( readOperation == null ) 
        {
            final Node<USERKEY, STOREKEY> value = nodeStore.loadNode(key);

            nodeReads.put(key, new CachedReadOperation<STOREKEY, Node<USERKEY, STOREKEY>>(key, value));

            return value;
        }
        else
        {
            return readOperation.getValue();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeNode(final STOREKEY key)
    {
        final Operation<STOREKEY, Node<USERKEY, STOREKEY>> o = new Operation<STOREKEY, Node<USERKEY, STOREKEY>>()
        {
            @Override
            protected void doExecute()
            {
                nodeStore.removeNode(key);
            }

            @Override
            public Node<USERKEY, STOREKEY> getValue()
            {
                throw new NodeStoreNodeNotFoundException();
            }   
        };

        if ( nodeWrites.containsKey(key) ) {
            nodeWrites.get(key).delete();
        }
        operations.add(o);
        nodeReads.put(key, o);
        nodeWrites.put(key, o);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeData(final STOREKEY key)
    {
        final Operation<STOREKEY, VALUE> o = new Operation<STOREKEY, VALUE>(key, null)
        {
            @Override
            protected void doExecute()
            {
                nodeStore.removeData(key);
            }
        };

        if ( valueWrites.containsKey(key) ) {
            valueWrites.get(key).delete();
        }
        operations.add(o);
        valueReads.put(key, o);
        valueWrites.put(key, o);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void store(final STOREKEY key, final Node<USERKEY, STOREKEY> node)
    {
        final Operation<STOREKEY, Node<USERKEY, STOREKEY>> o =
            new Operation<STOREKEY, Node<USERKEY, STOREKEY>>(key, node)
            {
                @Override
                protected void doExecute()
                {
                    nodeStore.store(key, node);
                }
            };

        if ( nodeReads.containsKey(key) ) {
            nodeReads.get(key).delete();
        }
        operations.add(o);
        nodeReads.put(key, o);
        nodeWrites.put(key, o);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void store(final STOREKEY key, final VALUE value)
    {
        final Operation<STOREKEY, VALUE> o =
            new Operation<STOREKEY, VALUE>(key, value)
            {
                @Override
                protected void doExecute()
                {
                    nodeStore.store(key, value);
                }
            };

        if ( valueWrites.containsKey(key) ) {
            valueWrites.get(key).delete();
        }
        operations.add(o);
        valueReads.put(key, o);
        valueWrites.put(key, o);
    }
}
