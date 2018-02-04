/*
 * Copyright (c) 2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package heretical.s3.sync.operation;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import cascading.flow.FlowProcess;
import cascading.nested.json.JSONCoercibleType;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.operation.OperationException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.fasterxml.jackson.databind.JsonNode;

import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

/**
 * Parses the embedded query string {@code GET /path/file?k1=v1&k2=v2&k2=v3 HTTP/1.1} into a JSON tree.
 * <p>
 * By default the tree is an Object with Arrays (Map&lt;String,List&lt;String&gt;&gt;).
 * <p>
 * If {@code retainLastValue} is true, the tree will be an Object with String children (Map&lt;String,String&gt;).
 */
public class QueryParamsParserFunction extends BaseOperation<Tuple> implements Function<Tuple>
  {
  private JSONCoercibleType type = JSONCoercibleType.TYPE;
  private boolean retainLastValue = false;

  public QueryParamsParserFunction( Fields fieldDeclaration, boolean retainLastValue )
    {
    super( fieldDeclaration );
    this.retainLastValue = retainLastValue;
    }

  public QueryParamsParserFunction( Fields fieldDeclaration )
    {
    super( fieldDeclaration );
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Tuple> operationCall )
    {
    operationCall.setContext( Tuple.size( 1 ) );
    }

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall<Tuple> functionCall )
    {
    String string = functionCall.getArguments().getString( 0 );

    if( string == null )
      {
      functionCall.getContext().set( 0, null );
      functionCall.getOutputCollector().add( functionCall.getContext() );
      return;
      }

    int pos = string.indexOf( '?' );

    if( pos == -1 )
      {
      functionCall.getContext().set( 0, null );
      functionCall.getOutputCollector().add( functionCall.getContext() );
      return;
      }

    // remove trailing protocol spec
    String queryString = string.substring( pos + 1, string.lastIndexOf( ' ' ) );

    JsonNode canonical;

    if( retainLastValue ) // retain last seen value
      {
      Map<String, String> collect = Arrays.stream( queryString.split( "&" ) )
        .map( this::splitQueryParameter )
        .filter( p -> p[ 1 ] != null )
        .collect( Collectors.toMap( p -> p[ 0 ], p -> p[ 1 ], ( lhs, rhs ) -> rhs, LinkedHashMap::new ) );

      canonical = type.canonical( collect );
      }
    else
      {
      Map<String, List<String>> collect = Arrays.stream( queryString.split( "&" ) )
        .map( this::splitQueryParameter )
        .collect( Collectors.groupingBy( p -> p[ 0 ], LinkedHashMap::new, mapping( p -> p[ 1 ], toList() ) ) );

      canonical = type.canonical( collect );
      }

    functionCall.getContext().set( 0, canonical );
    functionCall.getOutputCollector().add( functionCall.getContext() );
    }

  private String[] splitQueryParameter( String item )
    {
    int pos = item.indexOf( "=" );
    String key = pos > 0 ? item.substring( 0, pos ) : item;
    String value = pos > 0 && item.length() > pos + 1 ? item.substring( pos + 1 ) : null;

    return new String[]{decode( key ), decode( value )};
    }

  private String decode( String encoded )
    {
    if( encoded == null )
      return null;

    try
      {
      return URLDecoder.decode( encoded, "UTF-8" );
      }
    catch( UnsupportedEncodingException exception )
      {
      throw new OperationException( exception );
      }
    }
  }
