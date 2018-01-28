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

/**
 *
 */
public class Options
  {
  protected OptionParser parser;
  private OptionSet optionSet;

  private final OptionSpec<Integer> displayWidth;
  private final OptionSpec<String> input;
  private final OptionSpec<String> inputCheckpoint;
  private final OptionSpec<String> output;
  private final OptionSpec<Format> outputFormat;

  public Options()
    {
    parser = new OptionParser();

    parser.formatHelpWith( new BuiltinHelpFormatter( 120, 4 ) );

    displayWidth = parser.accepts( "display-width", "width of display" )
      .withRequiredArg()
      .ofType( Integer.class )
      .defaultsTo( 80 );

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
  }
