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
import cascading.nested.json.JSONCoercibleType;
import cascading.nested.json.JSONCreateFunction;
import cascading.operation.Debug;
import cascading.operation.regex.RegexParser;
import cascading.operation.regex.RegexReplace;
import cascading.operation.text.DateFormatter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Coerce;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.Tap;
import cascading.tap.local.DirTap;
import cascading.tuple.Fields;
import cascading.tuple.type.DateType;
import heretical.s3.sync.factory.TapFactories;
import heretical.s3.sync.operation.QueryParamsParserFunction;
import heretical.s3.sync.operation.SourcePathFunction;

import static cascading.flow.FlowDef.flowDef;

/**
 */
public class Main
  {
  public static final String DD = "dd";
  public static final String MMM = "MM";
  public static final String YYYY = "yyyy";
  public static final TimeZone UTC = TimeZone.getTimeZone( "UTC" );

  public static final DateType Y_TYPE = new DateType( YYYY, UTC );
  public static final DateType M_TYPE = new DateType( MMM, UTC );
  public static final DateType D_TYPE = new DateType( DD, UTC );

  public static final Fields DAY = new Fields( "day", D_TYPE );
  public static final Fields MONTH = new Fields( "month", M_TYPE );
  public static final Fields YEAR = new Fields( "year", Y_TYPE );
  public static final Fields KEY_CLEAN = new Fields( "keyClean", String.class );
  public static final Fields JSON = new Fields( "json", JSONCoercibleType.TYPE );
  public static final Fields LOG_KEY = new Fields( "logKey", String.class );
  public static final Fields QUERY_STRING = new Fields( "queryString", JSONCoercibleType.TYPE );
  public static Fields STANDARD_TIME = new Fields( "time", new DateType( "yyyy-MM-dd'T'HH:mm'Z'", TimeZone.getTimeZone( "UTC" ) ) );

  public static void main( String[] args )
    {
    Options options = new Options();

    if( !options.parse( args ) )
      return;

    System.out.println( "source s3 uri = " + options.getInput() );
    System.out.println( "sink path = " + options.getOutput() );

    if( options.hasInputCheckpoint() )
      System.out.println( "checkpoint file path = " + options.getInputCheckpoint() );

    Fields sinkFields = LOG_KEY.append( S3Logs.FIELDS );

    if( options.isParseQueryString() )
      sinkFields = sinkFields.append( QUERY_STRING );

    Pipe pipe = createPipeline( options, sinkFields );

    // read from an S3 bucket
    // optionally restart where a previous run left off
    S3FileCheckpointer checkpointer = options.hasInputCheckpoint() ? new S3FileCheckpointer( options.getInputCheckpoint() ) : new S3FileCheckpointer();
    Tap inputTap = new S3Tap( new TextLine(), checkpointer, URI.create( options.getInput() ) );

    Fields partitionKey = YEAR.append( MONTH ).append( DAY );

    if( options.isPartitionOnKey() )
      partitionKey = partitionKey.append( KEY_CLEAN );

    if( options.getOutputFormat() == Format.json )
      sinkFields = JSON;

    Tap outputTap = TapFactories.getSinkFactory( options.getOutput() )
      .getSink( options.getOutput(), options.getOutputFormat(), partitionKey, sinkFields );

    Tap trapTap = new DirTap( new TextDelimited(), options.getErrors() );

    Flow syncFlow = new LocalFlowConnector().connect( flowDef()
      .setName( "egress" )
      .addSource( pipe, inputTap )
      .addSink( pipe, outputTap )
      .addTail( pipe )
      .addTrap( pipe, trapTap )
    );

    syncFlow.complete();
    System.out.println( "completed" );
    }

  private static Pipe createPipeline( Options options, Fields objectFields )
    {
    Pipe pipe = new Pipe( "head" );

    // watch the progress on the console
    if( options.isDebugStream() )
      pipe = new Each( pipe, new Debug( true ) );

    // parse the full log into its fields and primitive values -- S3Logs.FIELDS declared field names and field types
    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( S3Logs.FIELDS, S3Logs.REGEX ), Fields.RESULTS );

    // records the file name each log line came from
    pipe = new Each( pipe, Fields.NONE, new SourcePathFunction( LOG_KEY ), Fields.ALL );

    if( options.isParseQueryString() )
      pipe = new Each( pipe, S3Logs.REQUEST_URI, new QueryParamsParserFunction( QUERY_STRING, true ), Fields.ALL );

    // watch the progress on the console
    if( options.isDebugStream() )
      pipe = new Each( pipe, new Debug( true ) );

    // create partition key
    pipe = new Each( pipe, S3Logs.TIME, new DateFormatter( DAY, DD, UTC ), Fields.ALL );
    pipe = new Each( pipe, S3Logs.TIME, new DateFormatter( MONTH, MMM, UTC ), Fields.ALL );
    pipe = new Each( pipe, S3Logs.TIME, new DateFormatter( YEAR, YYYY, UTC ), Fields.ALL );

    // force time to a string -- JSON functions should declare a type to preserve as at some point
    pipe = new Coerce( pipe, STANDARD_TIME );
    pipe = new Coerce( pipe, STANDARD_TIME, String.class );

    if( options.isPartitionOnKey() )
      pipe = new Each( pipe, S3Logs.KEY, new RegexReplace( KEY_CLEAN, "/", "-" ), Fields.ALL );

    if( options.getOutputFormat() == Format.json )
      pipe = new Each( pipe, objectFields, new JSONCreateFunction( JSON ), Fields.ALL );

    return pipe;
    }
  }
