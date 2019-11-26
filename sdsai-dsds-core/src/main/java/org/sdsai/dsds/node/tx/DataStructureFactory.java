package org.sdsai.dsds.node.tx;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;

import java.util.List;
import java.util.Map;

import org.sdsai.dsds.BTree;
import org.sdsai.dsds.PagedList;
import org.sdsai.dsds.node.NodeStore;


/**
 * Build transactional datastructures using {@link Proxy}.
 */
public class DataStructureFactory
{
    /**
     * Build a new DataStructureFactory.
     *
     * @throws RuntimeException If a TransactionalNodeStore is passed as the nodeStore parameter.
     */
    private DataStructureFactory()
    {
    }

    private static <USERKEY, STOREKEY, VALUE> TransactionalNodeStore<USERKEY, STOREKEY, VALUE> txNodeStore(
        final NodeStore<USERKEY, STOREKEY, VALUE> nodeStore)
    {
        if ( nodeStore instanceof TransactionalNodeStore )
        {
            throw new RuntimeException("DataStructureFactory is incompatible with TransactionalNodeStore.");
        }

        return new TransactionalNodeStore<USERKEY, STOREKEY, VALUE>(nodeStore);
    }

    private static <USERKEY, STOREKEY, VALUE> InvocationHandler buildTxCommittingInvocationHandler(
        final NodeStore<USERKEY, STOREKEY, VALUE> nodeStore,
        final Object dispatchObject)
    {

        final TransactionalNodeStore<USERKEY, STOREKEY, VALUE> transactionalNodeStore = txNodeStore(nodeStore);
        final InvocationHandler ivh = new InvocationHandler()
        {
            @Override
            public Object invoke(final Object proxy, final Method method, final Object[] args)
            {
                try
                {
                    final Object returnObject = method.invoke(dispatchObject, args);

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

    public static <STOREKEY, VALUE> List<VALUE> pagedList(
        final STOREKEY root,
        final NodeStore<STOREKEY, STOREKEY, VALUE> nodeStore,
        final int size)
    {
        final PagedList<STOREKEY, VALUE> pagedList = new PagedList<STOREKEY, VALUE>(root, nodeStore, size);

        @SuppressWarnings("unchecked")
        final List<VALUE> list = (List<VALUE>)
            Proxy.newProxyInstance(
                PagedList.class.getClassLoader(),
                new Class<?>[]{ List.class },
                buildTxCommittingInvocationHandler(nodeStore, pagedList));

        return list;
    }

    public static <STOREKEY, VALUE> List<VALUE> pagedList(
        final STOREKEY root,
        final NodeStore<STOREKEY, STOREKEY, VALUE> nodeStore)
    {
        final PagedList<STOREKEY, VALUE> pagedList = new PagedList<STOREKEY, VALUE>(root, nodeStore);

        @SuppressWarnings("unchecked")
        final List<VALUE> list = (List<VALUE>)
            Proxy.newProxyInstance(
                PagedList.class.getClassLoader(),
                new Class<?>[]{ List.class },
                buildTxCommittingInvocationHandler(nodeStore, pagedList));

        return list;
    }

    public static <USERKEY, STOREKEY, VALUE> Map<USERKEY, VALUE> bTree(
        final USERKEY root,
        final NodeStore<USERKEY, STOREKEY, VALUE> nodeStore,
        final int size)
    {
        final BTree<USERKEY, STOREKEY, VALUE> bTree = new BTree<USERKEY, STOREKEY, VALUE>(root, nodeStore, size);

        @SuppressWarnings("unchecked")
        final Map<USERKEY, VALUE> map = (Map<USERKEY, VALUE>)
            Proxy.newProxyInstance(
                BTree.class.getClassLoader(),
                new Class<?>[]{ Map.class },
                buildTxCommittingInvocationHandler(nodeStore, bTree));

        return map;
    }

    public static <USERKEY, STOREKEY, VALUE> Map<USERKEY, VALUE> bTree(
        final USERKEY root,
        final NodeStore<USERKEY, STOREKEY, VALUE> nodeStore)
    {
        final BTree<USERKEY, STOREKEY, VALUE> bTree = new BTree<USERKEY, STOREKEY, VALUE>(root, nodeStore);

        @SuppressWarnings("unchecked")
        final Map<USERKEY, VALUE> map = (Map<USERKEY, VALUE>)
            Proxy.newProxyInstance(
                BTree.class.getClassLoader(),
                new Class<?>[]{ Map.class },
                buildTxCommittingInvocationHandler(nodeStore, bTree));

        return map;
    }
}
