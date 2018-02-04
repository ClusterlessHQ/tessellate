/*
 * Copyright (c) 2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package heretical.s3.sync.operation;

import java.util.LinkedHashMap;
import java.util.Map;

import cascading.CascadingTestCase;
import cascading.nested.json.JSONCoercibleType;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleListCollector;
import org.junit.Test;

import static java.util.Collections.singletonList;

/**
 *
 */
public class QueryParamsTest extends CascadingTestCase
  {
  public static final String QUERY = "GET /cascading/2.2/latest.properties?id=178885338&instance=4A09800F65D14BCA883511446869C445&os-name=Linux&jvm-name=Java+HotSpot%28TM%29+64-Bit+Server+VM&jvm-version=1.7.0_40&os-arch=amd64&product=Cascading&version=2.2.1&version-build=&frameworks=&platform-name=Hadoop&platform-version=1.0.3&platform-vendor=Apache HTTP/1.1";

  @Test
  public void testQueryString()
    {
    QueryParamsParserFunction query = new QueryParamsParserFunction( new Fields( "query" ) );

    TupleListCollector tuples = invokeFunction( query, new Tuple( QUERY ), Fields.RESULTS );

    Map map = new LinkedHashMap();

    map.put( "id", singletonList( "178885338" ) );
    map.put( "instance", singletonList( "4A09800F65D14BCA883511446869C445" ) );
    map.put( "os-name", singletonList( "Linux" ) );
    map.put( "jvm-name", singletonList( "Java HotSpot(TM) 64-Bit Server VM" ) );
    map.put( "jvm-version", singletonList( "1.7.0_40" ) );
    map.put( "os-arch", singletonList( "amd64" ) );
    map.put( "product", singletonList( "Cascading" ) );
    map.put( "version", singletonList( "2.2.1" ) );
    map.put( "version-build", singletonList( null ) );
    map.put( "frameworks", singletonList( null ) );
    map.put( "platform-name", singletonList( "Hadoop" ) );
    map.put( "platform-version", singletonList( "1.0.3" ) );
    map.put( "platform-vendor", singletonList( "Apache" ) );

    Map lhs = JSONCoercibleType.TYPE.coerce( JSONCoercibleType.TYPE.canonical( map ), Map.class );
    Map rhs = JSONCoercibleType.TYPE.coerce( tuples.entryIterator().next().getObject( 0 ), Map.class );

    assertEquals( lhs, rhs );
    }

  @Test
  public void testQueryStringUnique()
    {
    QueryParamsParserFunction query = new QueryParamsParserFunction( new Fields( "query" ), true );

    TupleListCollector tuples = invokeFunction( query, new Tuple( QUERY ), Fields.RESULTS );

    Map map = new LinkedHashMap();

    map.put( "id", "178885338" );
    map.put( "instance", "4A09800F65D14BCA883511446869C445" );
    map.put( "os-name", "Linux" );
    map.put( "jvm-name", "Java HotSpot(TM) 64-Bit Server VM" );
    map.put( "jvm-version", "1.7.0_40" );
    map.put( "os-arch", "amd64" );
    map.put( "product", "Cascading" );
    map.put( "version", "2.2.1" );
//    map.put( "version-build", null ); // dropped
//    map.put( "frameworks", null ); // dropped
    map.put( "platform-name", "Hadoop" );
    map.put( "platform-version", "1.0.3" );
    map.put( "platform-vendor", "Apache" );

    Map lhs = JSONCoercibleType.TYPE.coerce( JSONCoercibleType.TYPE.canonical( map ), Map.class );
    Map rhs = JSONCoercibleType.TYPE.coerce( tuples.entryIterator().next().getObject( 0 ), Map.class );

    assertEquals( lhs, rhs );
    }
  }
