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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.util.AvroSchemaHelper;
import org.apache.avro.Schema;

import java.util.Map;

import static org.apache.commons.lang.StringUtils.isEmpty;

/**
 * Provides schema metadata, such as actual schema, schema id, subject etc. on per record basis.
 */
public interface AvroSchemaMetadataProvider {
  SchemaMetadata getMeta(Record record) throws DataGeneratorException;


  /**
   * Provides static schema metadata.
   */
  class StaticAvroSchemaMetadataProvider implements AvroSchemaMetadataProvider {
    private final SchemaMetadata meta;


    public StaticAvroSchemaMetadataProvider(String subject, int schemaId, Schema schema, Map<String, Object> defaultValues) {
      meta = new SchemaMetadata(schemaId, subject, schema, defaultValues);
    }

    @Override
    public SchemaMetadata getMeta(Record record) throws DataGeneratorException {
      return meta;
    }
  }

  /**
   * Provides metadata that is retrieved for each record based on subject contained in record.
   */
  class RecordAvroSchemaMetadataProvider implements AvroSchemaMetadataProvider {
    private final String subjectEL;
    private final LoadingCache<String, SchemaMetadata> cache;
    private final ELVars vars;
    private final ELEval eval;

    public RecordAvroSchemaMetadataProvider(String subjectEL, ProtoConfigurableEntity.ELContext context, LoadingCache<String, SchemaMetadata> cache) {
      this.subjectEL = subjectEL;
      this.cache = cache;
      this.vars = context.createELVars();
      this.eval = context.createELEval("subject", RecordEL.class);
    }

    @Override
    public SchemaMetadata getMeta(Record record) throws DataGeneratorException {
      // get subject from header
      RecordEL.setRecordInContext(vars, record);
      try {
        String subj = eval.eval(vars, subjectEL, String.class);
        if (isEmpty(subj)) {
          throw new DataGeneratorException(Errors.AVRO_GENERATOR_06,
              record.getHeader().getSourceId(),
              subjectEL
          );
        }
        return cache.getUnchecked(subj);

      } catch (ELEvalException e) {
        throw new DataGeneratorException(Errors.AVRO_GENERATOR_07,
            record.getHeader().getSourceId(),
            subjectEL
        );
      }
    }

    static LoadingCache<String, SchemaMetadata> createCache(AvroSchemaHelper schemaHelper) {
      return CacheBuilder.newBuilder().build(
          new CacheLoader<String, SchemaMetadata>() {
            @Override
            public AvroSchemaMetadataProvider.SchemaMetadata load(String subject) throws Exception {
              int schemaId = schemaHelper.getSchemaIdFromSubject(subject);
              Schema schema = schemaHelper.loadFromRegistry(schemaId);
              Map<String, Object> defaultValuesFromSchema = AvroSchemaHelper.getDefaultValues(schema);
              return new AvroSchemaMetadataProvider.SchemaMetadata(schemaId, subject, schema, defaultValuesFromSchema);
            }
          }
      );
    }
  }

  /**
   * Schema metadata
   */
  class SchemaMetadata {
    public final int schemaId;
    public final String subject;
    public final Schema schema;
    public final Map<String, Object> defaultValues;

    public SchemaMetadata(int schemaId, String subject, Schema schema, Map<String, Object> defaultValues) {
      this.schemaId = schemaId;
      this.subject = subject;
      this.schema = schema;
      this.defaultValues = defaultValues;
    }
  }
}
