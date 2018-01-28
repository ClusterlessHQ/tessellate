/*
 * Copyright (c) 2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package heretical.s3.sync;

import java.lang.reflect.Type;
import java.util.function.Function;

import cascading.tuple.type.CoercibleType;

/**
 *
 */
public class CleanCoercibleType<Canonical> implements CoercibleType<Canonical>
  {
  CoercibleType<Canonical> coercibleType;
  Function<Object, Object> function;

  public CleanCoercibleType( CoercibleType<Canonical> coercibleType, Function<Object, Object> function )
    {
    this.coercibleType = coercibleType;
    this.function = function;
    }

  @Override
  public Class<Canonical> getCanonicalType()
    {
    return coercibleType.getCanonicalType();
    }

  @Override
  public Canonical canonical( Object value )
    {
    return coercibleType.canonical( function.apply( value ) );
    }

  @Override
  public <Coerce> Coerce coerce( Object value, Type to )
    {
    return coercibleType.coerce( value, to );
    }
  }
