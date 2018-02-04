/*
 * Copyright (c) 2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package heretical.s3.sync;

import java.util.TimeZone;

import cascading.tuple.Fields;
import cascading.tuple.coerce.Coercions;
import cascading.tuple.type.DateType;

/**
 * https://docs.aws.amazon.com/AmazonS3/latest/dev/LogFormat.html
 */
public class S3Logs
  {
  public static final String REGEX = "(\\S+) ([a-z0-9][a-z0-9-.]+) \\[(.*\\+.*)] (\\b(?:\\d{1,3}\\.){3}\\d{1,3}\\b) (\\S+) (\\S+) (\\S+) (\\S+) \"(\\w+ \\S+ \\S+)\" (\\d+|-) (\\S+) (\\d+|-) (\\d+|-) (\\d+|-) (\\d+|-) \"(https?://.*/?|-)\" \"(.*)\" (\\S+)";

  public static final CleanCoercibleType<Long> CLEAN_LONG = new CleanCoercibleType<>( Coercions.LONG_OBJECT, o -> o.equals( "-" ) ? null : o );
  public static final CleanCoercibleType<String> CLEAN_STRING = new CleanCoercibleType<>( Coercions.STRING, o -> o.equals( "-" ) ? null : o );

  public static Fields BUCKET_OWNER = new Fields( "bucketOwner", String.class );
  public static Fields BUCKET = new Fields( "bucket", String.class );
  public static Fields TIME = new Fields( "time", new DateType( "dd/MMM/yyyy:HH:mm:ss Z", TimeZone.getTimeZone( "UTC" ) ) );
  public static Fields REMOTE_IP_ADDRESS = new Fields( "remoteIP", String.class );
  public static Fields REQUESTER = new Fields( "requester", CLEAN_STRING );
  public static Fields REQUEST_ID = new Fields( "requestID", String.class );
  public static Fields OPERATION = new Fields( "operation", String.class );
  public static Fields KEY = new Fields( "key", CLEAN_STRING );
  public static Fields REQUEST_URI = new Fields( "requestURI", String.class );
  public static Fields HTTP_STATUS = new Fields( "httpStatus", Integer.class );
  public static Fields ERROR_CODE = new Fields( "errorCode", CLEAN_STRING );
  public static Fields BYTES_SENT = new Fields( "bytesSent", CLEAN_LONG );
  public static Fields OBJECT_SIZE = new Fields( "objectSize", CLEAN_LONG );
  public static Fields TOTAL_TIME = new Fields( "totalTime", CLEAN_LONG );
  public static Fields TURN_AROUND_TIME = new Fields( "turnAroundTime", CLEAN_LONG );
  public static Fields REFERRER = new Fields( "referrer", CLEAN_STRING );
  public static Fields USER_AGENT = new Fields( "userAgent", String.class );
  public static Fields VERSION_ID = new Fields( "versionID", CLEAN_STRING );

  public static final Fields FIELDS = Fields.NONE
    .append( BUCKET_OWNER )
    .append( BUCKET )
    .append( TIME )
    .append( REMOTE_IP_ADDRESS )
    .append( REQUESTER )
    .append( REQUEST_ID )
    .append( OPERATION )
    .append( KEY )
    .append( REQUEST_URI )
    .append( HTTP_STATUS )
    .append( ERROR_CODE )
    .append( BYTES_SENT )
    .append( OBJECT_SIZE )
    .append( TOTAL_TIME )
    .append( TURN_AROUND_TIME )
    .append( REFERRER )
    .append( USER_AGENT )
    .append( VERSION_ID );
  }
