/*
 * Copyright (c) 2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package heretical.s3.sync.factory;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class TapFactories
  {
  static Map<String, SinkFactory> sinkFactories = new HashMap<>();

  static
    {
    sinkFactories.put( PartitionSinkFactory.INSTANCE.getProtocol(), PartitionSinkFactory.INSTANCE );
    sinkFactories.put( ESSinkFactory.INSTANCE.getProtocol(), ESSinkFactory.INSTANCE );
    }

  public static SinkFactory getSinkFactory( String url )
    {
    URI uri = URI.create( url );

    if( uri.getScheme() == null )
      return PartitionSinkFactory.INSTANCE;

    return sinkFactories.get( uri.getScheme() );
    }
  }
