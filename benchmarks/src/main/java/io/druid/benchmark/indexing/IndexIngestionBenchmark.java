/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.benchmark.indexing;

import io.druid.benchmark.datagen.BenchmarkDataGenerator;
import io.druid.benchmark.datagen.BenchmarkSchemaInfo;
import io.druid.benchmark.datagen.BenchmarkSchemas;
import io.druid.data.input.InputRow;
import io.druid.hll.HyperLogLogHash;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.serde.ComplexMetrics;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@State(Scope.Benchmark)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class IndexIngestionBenchmark
{
  @Param({"10000", "75000"})
  private int rowsPerSegment;

  @Param({"basic"})
  private String schema;

  @Param({"true"})
  private boolean rollup;

  @Param({"true", "false"})
  private boolean onheap;

  @Param({"256"})
  private int chunkMaxItems;

  @Param({"64"})
  private int chunkBytesPerItem;

  private static final Logger log = new Logger(IndexIngestionBenchmark.class);
  private static final int RNG_SEED = 9999;

  private IncrementalIndex incIndex;
  private ArrayList<InputRow> rows;
  private BenchmarkSchemaInfo schemaInfo;
  private AtomicInteger version;

  @Setup
  public void setup() throws IOException
  {
    ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde(HyperLogLogHash.getDefault()));

    rows = new ArrayList<InputRow>();
    schemaInfo = BenchmarkSchemas.SCHEMA_MAP.get(schema);

    BenchmarkDataGenerator gen = new BenchmarkDataGenerator(
            schemaInfo.getColumnSchemas(),
            RNG_SEED,
            schemaInfo.getDataInterval(),
            rowsPerSegment
    );

    for (int i = 0; i < rowsPerSegment; i++) {
      InputRow row = gen.nextRow();
      if (i % 10000 == 0) {
        log.info(i + " rows generated.");
      }
      rows.add(row);
    }

    log.info("Building an " + (onheap ? "on-heap" : "Oak") + " incremental index...");
    version = new AtomicInteger(0);
  }

  @Setup(Level.Iteration)
  public void setup2() throws IOException
  {
    incIndex = makeIncIndex();
    int prev = version.getAndIncrement();
    log.info("The index was created. Previuos version: " + prev + ". New version: " + (prev + 1));
  }

  private IncrementalIndex makeIncIndex()
  {
    if (onheap) {
      return new IncrementalIndex.Builder()
              .setIndexSchema(
                      new IncrementalIndexSchema.Builder()
                              .withMetrics(schemaInfo.getAggsArray())
                              .withRollup(rollup)
                              .build()
              )
              .setReportParseExceptions(false)
              .setMaxRowCount(rowsPerSegment * 2)
              .buildOnheap();
    } else {
      return new IncrementalIndex.Builder()
              .setIndexSchema(
                      new IncrementalIndexSchema.Builder()
                              .withMetrics(schemaInfo.getAggsArray())
                              .build()
              )
              .setReportParseExceptions(false)
              .setMaxRowCount(rowsPerSegment * 2)
              .buildOffheapOak(chunkMaxItems, chunkBytesPerItem);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.SingleShotTime)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Threads(10)
  public void addRows(Blackhole blackhole) throws Exception
  {
    long time = System.currentTimeMillis();
    int myVersion = version.get();
    log.info("Time before loop: " + time + ". Version " + myVersion);
    for (int i = 0; i < rowsPerSegment; i++) {
      InputRow row = rows.get(i);
      int rv = incIndex.add(row).getRowCount();
      blackhole.consume(rv);
    }
    long duration = System.currentTimeMillis() - time;
    double throughput = (10 * rowsPerSegment) / (double) duration;
    log.info("Index size: " + incIndex.size() + ". Duration: " + duration + " millis . Version " + myVersion + ". Throughput: " + throughput + " ops/ms");
  }

}
