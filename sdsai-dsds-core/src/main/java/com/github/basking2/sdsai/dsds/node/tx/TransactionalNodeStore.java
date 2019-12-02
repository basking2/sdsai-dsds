package com.github.basking2.sdsai.dsds.node.tx;

import com.github.basking2.sdsai.dsds.node.Node;
import com.github.basking2.sdsai.dsds.node.NodeStore;
import com.github.basking2.sdsai.dsds.node.NodeStoreNodeNotFoundException;

import java.util.List;
import java.util.Map;
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
 *
 * @param <USERKEY> The user key type.
 * @param <STOREKEY> The data store key type.
 * @param <VALUE> The value being stored.
 */
public class TransactionalNodeStore<USERKEY, STOREKEY, VALUE>
    implements NodeStore<USERKEY, STOREKEY, VALUE>
{
    private final NodeStore<USERKEY, STOREKEY, VALUE> nodeStore;

    /**
     * The list of all operations. Some may be marked as deleted, in which case they will not be executed at commit time.
     *
     * This preserves ordering.
     */
    private final List<Operation<?, ?>> operations;

    /**
     * Operations added to {@link #operations} are indexed here so the value can be read or the operation can be obviated.
     */
    private final Map<STOREKEY, Operation<STOREKEY, VALUE>> values;

    /**
     * Operations added to {@link #operations} are indexed here so the value can be read or the operation can be obviated.
     */
    private final Map<STOREKEY, Operation<STOREKEY, Node<USERKEY, STOREKEY>>> nodes;

    private final TransactionalNodeStore<USERKEY, STOREKEY, VALUE> parentTransaction;

    public TransactionalNodeStore(final NodeStore<USERKEY, STOREKEY, VALUE> nodeStore)
    {
        this(nodeStore, null);
    }

    /**
     * Create a new transaction that operates against the given {@link NodeStore}.
     */
    public TransactionalNodeStore(final NodeStore<USERKEY, STOREKEY, VALUE> nodeStore, final TransactionalNodeStore<USERKEY, STOREKEY, VALUE> parentTransaction)
    {
        this.nodeStore = nodeStore;
        this.operations = new LinkedList<>();
        this.values = new HashMap<>();
        this.nodes = new HashMap<>();
        this.parentTransaction = parentTransaction;
    }

    /**
     * Give the user a sub-transaction which is merged into this transaction when {@link SubTx#buildTx(TransactionalNodeStore)} returns.
     */
    public void subTransaction(final SubTx<USERKEY, STOREKEY, VALUE> subTx)
    {
        final TransactionalNodeStore<USERKEY, STOREKEY, VALUE> tx =
            new TransactionalNodeStore<>(nodeStore, this);

        subTx.buildTx(tx);

        operations.addAll(tx.operations);
        values.putAll(tx.values);
        nodes.putAll(tx.nodes);
    }

    /**
     * Rollback and clear all internal structures of this transaction.
     */
    public void rollback()
    {
        operations.clear();
        values.clear();
        nodes.clear();
    }

    /**
     * Execute all operations in this transaction.
     */
    public void commit()
    {
        for (final Operation<?, ?> o : operations)
        {
            if (!o.isDeleted()) {
                o.execute();
            }
        }

        operations.clear();
        values.clear();
        nodes.clear();
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
     * Attempt to find a read operation in this transaction or any parent transaction.
     */
    private Operation<STOREKEY, VALUE> loadDataReadOperation(final STOREKEY key)
    {
        Operation<STOREKEY, VALUE> readOperation = values.get(key);

        if (readOperation == null && parentTransaction != null)
        {
            readOperation = parentTransaction.loadDataReadOperation(key);
        }

        return readOperation;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public VALUE loadData(final STOREKEY key)
    {
        final Operation<STOREKEY, VALUE> readOperation = loadDataReadOperation(key);

        if ( readOperation == null ) 
        {
            final VALUE value = nodeStore.loadData(key);

            values.put(key, new CachedReadOperation<>(key, value));

            return value;
        }
        else
        {
            return readOperation.getValue();
        }
    }

    /**
     * Attempt to find a read operation in this transaction or any parent transaction.
     */
    private Operation<STOREKEY, Node<USERKEY, STOREKEY>> loadNodeReadOperation(final STOREKEY key)
    {
        Operation<STOREKEY, Node<USERKEY, STOREKEY>> readOperation = nodes.get(key);

        if (readOperation == null && parentTransaction != null)
        {
            readOperation = parentTransaction.loadNodeReadOperation(key);
        }

        return readOperation;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public Node<USERKEY, STOREKEY> loadNode(final STOREKEY key)
    {
        final Operation<STOREKEY, Node<USERKEY, STOREKEY>> readOperation = loadNodeReadOperation(key);

        if ( readOperation == null ) 
        {
            final Node<USERKEY, STOREKEY> value = nodeStore.loadNode(key);

            nodes.put(key, new CachedReadOperation<>(key, value));

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

        if ( nodes.containsKey(key) ) {
            nodes.get(key).delete();
        }

        operations.add(o);
        nodes.put(key, o);
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

        if ( values.containsKey(key) ) {
            values.get(key).delete();
        }

        operations.add(o);
        values.put(key, o);
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

        if ( nodes.containsKey(key) ) {
            nodes.get(key).delete();
        }

        operations.add(o);
        nodes.put(key, o);
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

        if ( values.containsKey(key) ) {
            values.get(key).delete();
        }

        operations.add(o);
        values.put(key, o);
    }

    /**
     * An interface to provide a callback for doing operations on sub-{@link TransactionalNodeStore}s.
     */
    public interface SubTx<USERKEY, STOREKEY, VALUE>
    {
        /**
         * The callback routine that builds up a subtransaction.
         *
         * When this method returns the tx parameter is merged into this {@link TransactionalNodeStore}.
         *
         * @param tx An empty transaction that will be merged into the parent transaction.
         */
        void buildTx(TransactionalNodeStore<USERKEY, STOREKEY, VALUE> tx);
    }
}
