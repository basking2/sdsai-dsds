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
package com.github.basking2.sdsai.dsds.mongo;

import java.lang.reflect.InvocationTargetException;

public class MongoObjectFactory
{
    public Object newInstance(final String className)
    {
        try
        {
            return newInstance(Class.forName(className));
        }
        catch (final ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }
        
    public <T> T newInstance(final Class<T> clazz)
    {
        try
        {
          return clazz.getConstructor().newInstance();
        }
        catch (final NoSuchMethodException e)
        {
            throw new RuntimeException(e);
        }
        catch (final InstantiationException e)
        {
            throw new RuntimeException(e);
        }
        catch (final IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
        catch (final InvocationTargetException e)
        {
            throw new RuntimeException(e);
        }
    }
}
