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
package org.sdsai.dsds.mongo;

import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBList;

import java.util.Collection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoUtils
{

    private static final Logger logger =
        LoggerFactory.getLogger(MongoUtils.class);
        
    private static MongoObjectFactory objectFactory = new MongoObjectFactory();

    /**
     * @return the base name of the method (minus "get" or "is")
     *         or {@code null} if this is not an accessor.
     */
    private static String isAccessor(final Method m)
    {
        if (m.getParameterTypes().length > 0)
            return null;
            
        final String name = m.getName();
        
        if ( name.startsWith("get") )
            return name.substring(3);

        return null;
    }

    /**
     * Convert {@code o} to a {@link DBObject} or, if o is 
     * a primitive class ({@link Class#isPrimitive()} is true),
     * a new, empty, {@link BasicDBObject} is created and the
     * key {@code value} is set to this object and that is returned.
     * The {@code _id} field is never set unless there is an object
     * field that causes it to be set.
     *
     * @return this java object wrapped in a DBObject.
     */
    public static DBObject toDBObject(Object o)
    {
        o = toDBObjectHelper(o, o.getClass());
        
        if (o instanceof DBObject)
            return (DBObject) o;
            
        return new BasicDBObject("Value", o);
    }
    
    private static boolean isValueOfAble(final Class<?> c)
    {
        return
                Boolean.class.isAssignableFrom(c)
             || Character.class.isAssignableFrom(c)
             || Byte.class.isAssignableFrom(c)
             || Short.class.isAssignableFrom(c)
             || Integer.class.isAssignableFrom(c)
             || Long.class.isAssignableFrom(c)
             || Float.class.isAssignableFrom(c)
             || Double.class.isAssignableFrom(c)
             || Void.class.isAssignableFrom(c)
             ;
    }

    /**
     * Return a DBObject wrapping {@code o} <em>or</em> if o is
     * a primitive class, just return o.
     *
     * @return a DBObject wrapping {@code o} <em>or</em> if o is
     * a primitive class, just return o.
     */
    private static Object toDBObjectHelper(final Object o, final Class<?> clazz)
    {
        try
        {
            if ( clazz.isPrimitive() ) 
            {
                return o;
            }

            if ( isValueOfAble(clazz) )
            {
                return o;
            }
            
            if ( o instanceof String )
            {
                return o;
            }
            
            if ( clazz.isArray() )
            {
                final BasicDBList dblist = new BasicDBList();
                final BasicDBObject dbo = new BasicDBObject();
                        
                for (int i = 0; i<Array.getLength(o); i++) {
                    dblist.add(
                        toDBObjectHelper(
                            Array.get(o, i),
                            Array.get(o, i).getClass()));
                }
 
                dbo.put("Class", o.getClass().getName());
                dbo.put("list", dblist);
 
                return dbo;
            }
            
            if ( o instanceof Collection ) 
            {
                final BasicDBObject dbo = new BasicDBObject();
                final BasicDBList dblist = new BasicDBList();
                        
                for (final Object tmpo : (Collection) o) {
                    dblist.add(toDBObjectHelper(tmpo, tmpo.getClass()));
                }
                
                dbo.put("Class", o.getClass().getName());
                dbo.put("list", dblist);
 
                return dbo;
            }
            
            if ( o instanceof Map )
            {
                final BasicDBObject dbo = new BasicDBObject();
                
                final BasicDBObject dbmap = new BasicDBObject();
                        
                for (final Object key : ((Map) o).keySet())
                {
                    Object o2 = ((Map)o).get(key);
                    dbmap.put(
                        key.toString(), 
                        toDBObjectHelper(o2, o2.getClass()));
                }
                
                dbo.put("map", dbmap);
                dbo.put("Class", o.getClass().getName());
 
                return dbo;
            }
            
            final DBObject dbo = new BasicDBObject();
            
            // Complex user object.
            for( final Method m : o.getClass().getMethods() ) {
            
                final String getterName = isAccessor(m);
                
                if (getterName != null) {
                    if (getterName.equals("Class")) {
                        dbo.put("Class", o.getClass().getName());
                    } else {
                        dbo.put(getterName,
                            toDBObjectHelper(
                                m.invoke(o),
                                m.getReturnType()));
                    }
                }
            }
        
            return dbo;
        }
        catch (IllegalAccessException e)
        {
            throw new RuntimeException("Unexpected prohibition.", e);
        }
        catch (InvocationTargetException e)
        {
            throw new RuntimeException("Unexpected error.", e);
        }
    }
    
    public static Object fromDBObject(final DBObject dbo)
    {
        try {
            return fromDBObjectHelper(dbo);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public static <T> T fromDBObjectHelper(final DBObject dbo)
        throws Exception
    {
            final Class<?> returnType = Class.forName(dbo.get("Class").toString());
            final Object newInstance = objectFactory.newInstance(returnType);
            
            logger.debug("Converting id={} to {}", 
                dbo.get("_id"),
                returnType.getName());

            for (final Method method : returnType.getMethods() )
            {
                if ( method.getName().startsWith("set") &&
                     method.getParameterTypes().length == 1 )
                {
                    final String fieldName = method.getName().substring(3);
                    
                    final Object o = fromDBObjectHelper(
                        dbo,
                        fieldName,
                        method.getParameterTypes()[0]);
                    method.invoke(newInstance, o);
                }
            }
        
            @SuppressWarnings("unchecked")
            final T t = (T) newInstance;
            return t;
    }
    
    public static Object getPrimitive(final BasicDBObject dbo, 
                                      final String fieldName,
                                      final Class<?> returnType)
    throws Exception
    {
        final boolean contained = dbo.containsField(fieldName);
        
        if ( Boolean.TYPE.equals(returnType) )
            return contained ? dbo.getBoolean(fieldName) : Boolean.FALSE;
        
        if ( Short.TYPE.equals(returnType)
          || Byte.TYPE.equals(returnType)
          || Integer.TYPE.equals(returnType) )
          return (Integer) (contained? dbo.getInt(fieldName) : 0);
        
        if ( Character.TYPE.equals(returnType) ) 
            return (Character) ((dbo.get(fieldName)+"").charAt(0));

        if ( Long.TYPE.equals(returnType) )
            return (Long) (contained? dbo.getLong(fieldName) : 0L);

        if ( Float.TYPE.equals(returnType) )
            return (Float) (contained? Float.valueOf(dbo.get(fieldName)+"") : 0F);

        if ( Double.TYPE.equals(returnType) )
            return (Double) (contained? dbo.getDouble(fieldName) : 0D);

        return null;
    }
    
    public static <T> T fromDBObjectHelper(
        DBObject dbo,
        final String fieldName,
        final Class<T> returnType) throws Exception
    {
        logger.debug("Converting {}:{}", fieldName, returnType.getName());
        
        //logger.debug("-->{}", dbo);
        
        if (returnType.isPrimitive())
        {
            @SuppressWarnings("unchecked")
            final T t = (T) getPrimitive((BasicDBObject)dbo, fieldName, returnType);
            return t;
        }
        
        if (isValueOfAble(returnType))
        {
            @SuppressWarnings("unchecked")
            final T t = (T) returnType
                .getMethod("valueOf", String.class)
                .invoke(null, dbo.get(fieldName).toString());
            return t;
        }
        
        if (returnType.isArray())
        {
            final BasicDBList dblist = (BasicDBList) dbo.get("list");
            // FIXME
            
            return null;
        }
        
        if (Collection.class.isAssignableFrom(returnType))
        {
            dbo = (DBObject) dbo.get(fieldName);
            final BasicDBList dblist = (BasicDBList) dbo.get("list");

            @SuppressWarnings("unchecked")
            final Collection<Object> c = (Collection<Object>)
                objectFactory.newInstance(dbo.get("Class").toString());
            
            for (int i = 0; i < dblist.size(); i++ ) {
                Object tmpo = dblist.get(i);
                
                if ( tmpo instanceof DBObject )
                    tmpo = fromDBObjectHelper((DBObject)tmpo);;

                c.add(tmpo);
            }
            
            @SuppressWarnings("unchecked")
            final T t = (T) c;
            return t;
        }

        if (Map.class.isAssignableFrom(returnType))
        {
            dbo = (DBObject) dbo.get(fieldName);
            final BasicDBObject dbmap = (BasicDBObject) dbo.get("map");
            
            @SuppressWarnings("unchecked")
            final Map<String, Object> map = (Map<String, Object>)
                objectFactory.newInstance(dbo.get("Class").toString());
            
            for ( final String tmpFieldName : dbmap.keySet() )
            {
                Object tmpo = dbmap.get(tmpFieldName);
                
                if ( tmpo instanceof DBObject )
                    tmpo = fromDBObjectHelper((DBObject)tmpo);

                map.put(fieldName, tmpo);
            }
            
            @SuppressWarnings("unchecked")
            final T t = (T) map;
            return t;
        }
        
        // user object.
        
        @SuppressWarnings("unchecked")
        final T t = (T) fromDBObjectHelper((DBObject)dbo.get(fieldName));
        return t;
    }
}
