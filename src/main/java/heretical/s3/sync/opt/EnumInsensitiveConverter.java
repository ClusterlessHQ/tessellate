/*
 * Copyright (c) 2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package heretical.s3.sync.opt;

import java.lang.reflect.Method;

import joptsimple.ValueConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class EnumInsensitiveConverter<T extends Enum> implements ValueConverter<T>
  {
  private static final Logger LOG = LoggerFactory.getLogger( EnumInsensitiveConverter.class );

  Class<T> type;
  boolean toUpper;

  public EnumInsensitiveConverter( Class<T> type, boolean toUpper )
    {
    this.type = type;
    this.toUpper = toUpper;
    }

  @Override
  public T convert( String string )
    {
    if( toUpper )
      return (T) Enum.valueOf( type, string.toUpperCase() );
    else
      return (T) Enum.valueOf( type, string.toLowerCase() );
    }

  @Override
  public Class<T> valueType()
    {
    return type;
    }

  @Override
  public String valuePattern()
    {
    String[] values = invokeStaticMethodIfExists( type, "values" );
    return values == null ? null : String.join( ",", values );
    }

  public static <R> R invokeStaticMethodIfExists( Class target, String methodName )
    {
    return invokeStaticMethodIfExists( target, methodName, null, null );
    }

  public static <R> R invokeStaticMethodIfExists( Class target, String methodName, Object[] parameters, Class[] parameterTypes )
    {
    if( target == null )
      return null;

    try
      {
      Method method = target.getMethod( methodName, parameterTypes );

      method.setAccessible( true );

      return (R) method.invoke( null, parameters );
      }
    catch( NoSuchMethodException exception )
      {
      if( LOG.isTraceEnabled() )
        LOG.trace( "did not find {} method on {}", methodName, target.getName() );

      return null;
      }
    catch( NoClassDefFoundError exception )
      {
      if( LOG.isTraceEnabled() )
        LOG.trace( "did not find class argument on method {} on {}", methodName, target.getName() );

      return null;
      }
    catch( Exception exception )
      {
      throw new RuntimeException( "unable to invoke static method: " + target.getName() + "." + methodName, exception );
      }
    }
  }