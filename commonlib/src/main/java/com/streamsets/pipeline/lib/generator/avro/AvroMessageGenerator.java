/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.generator.avro;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.util.AvroSchemaHelper;
import com.streamsets.pipeline.lib.util.AvroTypeUtil;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;

public class AvroMessageGenerator extends BaseAvroDataGenerator {

  private DatumWriter<Object> datumWriter;
  private BinaryEncoder binaryEncoder;
  private final OutputStream outputStream;

  public AvroMessageGenerator(
      boolean schemaInHeader,
      OutputStream outputStream,
      AvroSchemaMetadataProvider schemaMetaProvider,
      AvroSchemaHelper schemaHelper
  ) throws IOException {
    super(schemaInHeader, schemaMetaProvider, schemaHelper);
    this.outputStream = outputStream;
    this.binaryEncoder = EncoderFactory.get().binaryEncoder(outputStream, null);
  }

  @Override
  protected void initializeWriter(Record record) throws DataGeneratorException {
    datumWriter = new GenericDatumWriter<>(schemaMetaProvider.getMeta(record).schema);
  }

  @Override
  protected void postInitialize(Record record) throws IOException, DataGeneratorException {
    AvroSchemaMetadataProvider.SchemaMetadata meta = schemaMetaProvider.getMeta(record);
    // If using Confluent Kafka Serializer we must write the magic byte
    if (schemaHelper != null && schemaHelper.hasRegistryClient() && meta.schemaId > 0) {
      schemaHelper.writeSchemaId(outputStream, meta.schemaId);
    }
  }

  @Override
  public void writeRecord(Record record) throws IOException, DataGeneratorException {
    try {
      AvroSchemaMetadataProvider.SchemaMetadata meta = schemaMetaProvider.getMeta(record);
      datumWriter.write(
          AvroTypeUtil.sdcRecordToAvro(
              record,
              meta.schema,
              meta.defaultValues
          ),
          binaryEncoder
      );
    } catch (StageException e) {
      throw new DataGeneratorException(e.getErrorCode(), e.getParams()); // params includes cause
    }
  }

  @Override
  protected Flushable getFlushable() {
    return binaryEncoder;
  }

  @Override
  protected Closeable getCloseable() {
    return outputStream;
  }
}
