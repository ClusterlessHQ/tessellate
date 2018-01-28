/*
 * Copyright (c) 2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package heretical.s3.sync.opt;

/**
 *
 */
public class EnumDashedConverter<T extends Enum> extends EnumInsensitiveConverter<T>
  {
  public EnumDashedConverter( Class<T> type, boolean toUpper )
    {
    super( type, toUpper );
    }

  @Override
  public T convert( String string )
    {
    return super.convert( string.replace( '-', '_' ) );
    }
  }
