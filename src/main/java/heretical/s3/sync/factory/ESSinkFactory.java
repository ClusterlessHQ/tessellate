/*
 * Copyright (c) 2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package heretical.s3.sync.factory;

import java.net.URI;
import java.util.Properties;

import cascading.tap.Tap;
import cascading.tuple.Fields;
import heretical.s3.sync.Format;
import heretical.s3.sync.S3Logs;
import org.elasticsearch.hadoop.cascading.EsTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ESSinkFactory extends SinkFactory
  {
  private static final Logger LOG = LoggerFactory.getLogger( ESSinkFactory.class );

  public static final ESSinkFactory INSTANCE = new ESSinkFactory();

  @Override
  public String getProtocol()
    {
    return "es";
    }

  @Override
  public Tap getSink( String outputPath, Format format, Fields partitionKey, Fields sinkFields )
    {
    URI uri = URI.create( outputPath );

    Properties settings = new Properties();

    // maps the json value id to es id and allows a single field json payload
    settings.setProperty( "es.mapping.id", S3Logs.REQUEST_ID.get( 0 ).toString() );
    settings.setProperty( "es.input.json", "true" );

    // required for aws es
//    settings.setProperty( "es.nodes.wan.only", "true" );  // disallows es.nodes.discovery=true

    if( uri.getPort() == 443 )
      {
      if( LOG.isDebugEnabled() )
        LOG.debug( "enabling SSL for port: {}", uri.getPort() );

      settings.setProperty( "es.net.ssl", "true" );
      settings.setProperty( "es.net.ssl.cert.allow.self.signed", "true" );
      }

    return new EsTap( uri.getHost(), uri.getPort(), uri.getPath(), null, sinkFields, settings );
    }
  }
