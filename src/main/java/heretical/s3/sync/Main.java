/*
 * Copyright (c) 2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package heretical.s3.sync;

import java.net.URI;
import java.util.TimeZone;

import cascading.flow.Flow;
import cascading.flow.local.LocalFlowConnector;
import cascading.local.tap.aws.s3.S3FileCheckpointer;
import cascading.local.tap.aws.s3.S3Tap;
import cascading.operation.Debug;
import cascading.operation.regex.RegexParser;
import cascading.operation.text.DateFormatter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.DirTap;
import cascading.tap.local.PartitionTap;
import cascading.tap.partition.DelimitedPartition;
import cascading.tuple.Fields;
import cascading.tuple.type.DateType;

import static cascading.flow.FlowDef.flowDef;

/**
 */
public class Main
  {
  public static final String DD_MMM_YYYY = "dd-MMM-yyyy";
  public static final TimeZone UTC = TimeZone.getTimeZone( "UTC" );
  public static final DateType DMY = new DateType( DD_MMM_YYYY, UTC );
  public static final Fields KEY = new Fields( "date", DMY );
  public static final Fields LINE = new Fields( "line", String.class );
  public static final Fields KEY_LINE = KEY.append( LINE );

  public static void main( String[] args )
    {
    Options options = new Options();

    if( !options.parse( args ) )
      return;

    System.out.println( "source s3 uri = " + options.getInput() );
    System.out.println( "sink path = " + options.getOutput() );

    if( options.hasInputCheckpoint() )
      System.out.println( "checkpoint file path = " + options.getInputCheckpoint() );

    // read from an S3 bucket
    // optionally restart where a previous run left off
    S3FileCheckpointer checkpointer = options.hasInputCheckpoint() ? new S3FileCheckpointer( options.getInputCheckpoint() ) : new S3FileCheckpointer();
    Tap inputTap = new S3Tap( new TextLine(), checkpointer, URI.create( options.getInput() ) );

    // write to disk, using log data to create the directory structure
    // if file exists, append to it -- we aren't duplicating s3 reads so this is safe
    DelimitedPartition partitioner = new DelimitedPartition( KEY.append( S3Logs.OPERATION ), "/", "logs.csv" );
    Tap outputTap = new PartitionTap(
      new DirTap( new TextDelimited( true, ",", "\"" ), options.getOutput(), SinkMode.UPDATE ), partitioner
    );

    Pipe pipe = new Pipe( "head" );

    // extract the log timestamp and reduce to day/month/year for use as the queue key
    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( S3Logs.TIME, S3Logs.REGEX, 3 ), new Fields( "time", "line" ) );
    pipe = new Each( pipe, S3Logs.TIME, new DateFormatter( KEY, DD_MMM_YYYY, UTC ), KEY_LINE );

    // watch the progress on the console
    pipe = new Each( pipe, new Debug( true ) );

    // parse the full log into its fields and primitive values -- S3Logs.FIELDS declard field names and field types
    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( S3Logs.FIELDS, S3Logs.REGEX ), KEY.append( S3Logs.FIELDS ) );

    // watch the progress on the console
    pipe = new Each( pipe, new Debug( true ) );

    Flow syncFlow = new LocalFlowConnector().connect( flowDef()
      .setName( "egress" )
      .addSource( pipe, inputTap )
      .addSink( pipe, outputTap )
      .addTail( pipe )
    );

    syncFlow.start();

    syncFlow.complete();
    System.out.println( "completed" );
    }
  }
