/*
 * Copyright (c) 2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package heretical.s3.sync.factory;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import cascading.nested.json.local.JSONTextLine;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.DirTap;
import cascading.tap.local.PartitionTap;
import cascading.tap.partition.DelimitedPartition;
import cascading.tuple.Fields;
import heretical.s3.sync.Format;

/**
 *
 */
public class PartitionSinkFactory extends SinkFactory
  {
  public static SinkFactory INSTANCE = new PartitionSinkFactory();

  @Override
  public String getProtocol()
    {
    return "file";
    }

  @Override
  public Tap getSink( String outputPath, Format format, Fields partitionKey, Fields sinkFields )
    {
    if( format == null )
      format = Format.csv;

    DelimitedPartition partitioner = new DelimitedPartition( partitionKey, "/", "logs." + format.name() );

    Scheme<Properties, InputStream, OutputStream, ?, ?> scheme;

    switch( format )
      {
      default:
      case csv:
        scheme = new TextDelimited( sinkFields, true, ",", "\"" );
        break;
      case tsv:
        scheme = new TextDelimited( sinkFields, true, "\t", "\"" );
        break;
      case json:
        scheme = new JSONTextLine( sinkFields );
        break;
      }

    return new PartitionTap(
      new DirTap( scheme, outputPath, SinkMode.UPDATE ), partitioner
    );
    }
  }
