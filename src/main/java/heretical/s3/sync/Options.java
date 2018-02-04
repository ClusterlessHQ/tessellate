/*
 * Copyright (c) 2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package heretical.s3.sync;

import heretical.s3.sync.opt.EnumInsensitiveConverter;
import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;

/**
 *
 */
public class Options
  {
  protected OptionParser parser;
  private OptionSet optionSet;

  private final OptionSpec<Integer> displayWidth;
  private final OptionSpecBuilder debugStream;
  private final OptionSpec<String> input;
  private final OptionSpec<String> inputCheckpoint;
  private final OptionSpec<String> output;
  private final OptionSpec<Format> outputFormat;
  private final OptionSpecBuilder partitionOnKey;
  private final OptionSpecBuilder parseQueryString;

  public Options()
    {
    parser = new OptionParser();

    parser.formatHelpWith( new BuiltinHelpFormatter( 120, 4 ) );

    displayWidth = parser.accepts( "display-width", "width of display" )
      .withRequiredArg()
      .ofType( Integer.class )
      .defaultsTo( 80 );

    debugStream = parser.accepts( "debug-stream" );

    input = parser.accepts( "input" )
      .withRequiredArg()
      .ofType( String.class );

    inputCheckpoint = parser.accepts( "input-checkpoint" )
      .withRequiredArg()
      .ofType( String.class );

    output = parser.accepts( "output" )
      .withRequiredArg()
      .ofType( String.class );

    outputFormat = parser.accepts( "output-format" )
      .withRequiredArg()
      .ofType( Format.class )
      .withValuesConvertedBy( new EnumInsensitiveConverter<>( Format.class, false ) );

    partitionOnKey = parser.accepts( "partition-on-key" );

    parseQueryString = parser.accepts( "parse-query-string" );
    }

  public boolean parse( String[] args )
    {
    try
      {
      optionSet = parser.parse( args );
      }
    catch( Exception exception )
      {
      System.out.println( "exception = " + exception );
      }

    return optionSet != null;
    }

  public boolean isDebugStream()
    {
    return optionSet.has( debugStream );
    }

  public String getInput()
    {
    return input.value( optionSet );
    }

  public boolean hasInputCheckpoint()
    {
    return getInputCheckpoint() != null;
    }

  public String getInputCheckpoint()
    {
    return inputCheckpoint.value( optionSet );
    }

  public String getOutput()
    {
    return output.value( optionSet );
    }

  public Format getOutputFormat()
    {
    return outputFormat.value( optionSet );
    }

  public boolean isPartitionOnKey()
    {
    return optionSet.has( partitionOnKey );
    }

  public boolean isParseQueryString()
    {
    return optionSet.has( parseQueryString );
    }
  }
