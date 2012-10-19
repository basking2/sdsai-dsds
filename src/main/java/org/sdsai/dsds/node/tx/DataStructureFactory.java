package org.sdsai.dsds.node.tx;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

import org.sdsai.dsds.BTree;
import org.sdsai.dsds.PagedList;
import org.sdsai.dsds.node.NodeStore;


/**
 * Build transactional datastructures using {@link Proxy}.
 */
public class DataStructureFactory<USERKEY, STOREKEY, VALUE>
{
    private NodeStore<USERKEY, STOREKEY, VALUE> nodeStore;

    /**
     * Build a new DataStructureFactory.
     *
     * @throws RuntimeException If a TransactionalNodeStore is passed as the nodeStore parameter.
     */
    public DataStructureFactory(final NodeStore<USERKEY, STOREKEY, VALUE> nodeStore)
    {
        if ( nodeStore instanceof TransactionalNodeStore )
        {
            throw new RuntimeException("DataStructureFactory is incompatible with TransactionalNodeStore.");
        }

        this.nodeStore = nodeStore;
    }

    private InvocationHandler buildTxCommittingInvocationHandler()
    {
        final TransactionalNodeStore<USERKEY, STOREKEY, VALUE> transactionalNodeStore =
            new TransactionalNodeStore<USERKEY, STOREKEY, VALUE>(nodeStore);

        final InvocationHandler ivh = new InvocationHandler()
        {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args)
            {
                try
                {
                    final Object returnObject = method.invoke(proxy, args);

                    transactionalNodeStore.commit();

                    return returnObject;
                }
                catch (final InvocationTargetException ite)
                {
                    throw new RuntimeException("Illegal access to method.", ite);
                }
                catch (final IllegalAccessException iae)
                {
                    throw new RuntimeException("Illegal access to method.", iae);
                }
            }
        };

        return ivh;
    }

    public PagedList<STOREKEY, VALUE> pagedList()
    {
        @SuppressWarnings("unchecked")
        final PagedList<STOREKEY, VALUE> pagedList = (PagedList<STOREKEY, VALUE>)
            Proxy.newProxyInstance(
                PagedList.class.getClassLoader(),
                new Class<?>[]{ PagedList.class },
                buildTxCommittingInvocationHandler());

        return pagedList;
    }

    public BTree<USERKEY, STOREKEY, VALUE> bTree()
    {
        @SuppressWarnings("unchecked")
        final BTree<USERKEY, STOREKEY, VALUE> bTree = (BTree<USERKEY, STOREKEY, VALUE>)
            Proxy.newProxyInstance(
                BTree.class.getClassLoader(),
                new Class<?>[]{ BTree.class },
                buildTxCommittingInvocationHandler());

        return bTree;
    }
}
