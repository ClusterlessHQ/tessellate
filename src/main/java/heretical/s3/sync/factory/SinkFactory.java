/*
 * Copyright (c) 2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package heretical.s3.sync.factory;

import cascading.tap.Tap;
import cascading.tuple.Fields;
import heretical.s3.sync.Format;

/**
 *
 */
public abstract class SinkFactory
  {
  public abstract String getProtocol();

  public abstract Tap getSink( String outputPath, Format format, Fields partitionKey, Fields sinkFields );
  }
