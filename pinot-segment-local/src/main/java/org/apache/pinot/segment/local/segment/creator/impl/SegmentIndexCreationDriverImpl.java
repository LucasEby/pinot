/**
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
package org.apache.pinot.segment.local.segment.creator.impl;

import com.google.common.base.Preconditions;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.MinionMeter;
import org.apache.pinot.common.metrics.MinionMetrics;
import org.apache.pinot.segment.local.realtime.converter.stats.RealtimeSegmentSegmentCreationDataSource;
import org.apache.pinot.segment.local.segment.creator.RecordReaderSegmentCreationDataSource;
import org.apache.pinot.segment.local.segment.creator.TransformPipeline;
import org.apache.pinot.segment.local.segment.index.converter.SegmentFormatConverterFactory;
import org.apache.pinot.segment.local.segment.index.dictionary.DictionaryIndexType;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.MultiColumnTextIndexHandler;
import org.apache.pinot.segment.local.segment.readers.CompactedPinotSegmentRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.local.startree.v2.builder.MultipleTreesBuilder;
import org.apache.pinot.segment.local.utils.CrcUtils;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.converter.SegmentFormatConverter;
import org.apache.pinot.segment.spi.creator.ColumnIndexCreationInfo;
import org.apache.pinot.segment.spi.creator.ColumnStatistics;
import org.apache.pinot.segment.spi.creator.SegmentCreationDataSource;
import org.apache.pinot.segment.spi.creator.SegmentCreator;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.creator.SegmentPreIndexStatsContainer;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.SegmentIndexCreationInfo;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderContext;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.IngestionSchemaValidator;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.SchemaValidatorFactory;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.ReadMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of an index segment creator.
 *
 */
// TODO: Check resource leaks
public class SegmentIndexCreationDriverImpl implements SegmentIndexCreationDriver {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentIndexCreationDriverImpl.class);

  private SegmentGeneratorConfig _config;
  private RecordReader _recordReader;
  private SegmentPreIndexStatsContainer _segmentStats;
  // NOTE: Use TreeMap so that the columns are ordered alphabetically
  private TreeMap<String, ColumnIndexCreationInfo> _indexCreationInfoMap;
  private SegmentCreator _indexCreator;
  private SegmentIndexCreationInfo _segmentIndexCreationInfo;
  private SegmentCreationDataSource _dataSource;
  private Schema _dataSchema;
  private TransformPipeline _transformPipeline;
  private IngestionSchemaValidator _ingestionSchemaValidator;
  private int _totalDocs = 0;
  private File _tempIndexDir;
  private String _segmentName;
  private long _totalRecordReadTimeNs = 0;
  private long _totalIndexTimeNs = 0;
  private long _totalStatsCollectorTimeNs = 0;
  private boolean _continueOnError;
  private int _incompleteRowsFound = 0;
  private int _skippedRowsFound = 0;
  private int _sanitizedRowsFound = 0;

  @Override
  public void init(SegmentGeneratorConfig config)
      throws Exception {
    init(config, getRecordReader(config));
  }

  private RecordReader getRecordReader(SegmentGeneratorConfig segmentGeneratorConfig)
      throws Exception {
    File dataFile = new File(segmentGeneratorConfig.getInputFilePath());
    Preconditions.checkState(dataFile.exists(), "Input file: " + dataFile.getAbsolutePath() + " does not exist");

    Schema schema = segmentGeneratorConfig.getSchema();
    TableConfig tableConfig = segmentGeneratorConfig.getTableConfig();
    FileFormat fileFormat = segmentGeneratorConfig.getFormat();
    String recordReaderClassName = segmentGeneratorConfig.getRecordReaderPath();
    Set<String> sourceFields =
        IngestionUtils.getFieldsForRecordExtractor(tableConfig, segmentGeneratorConfig.getSchema());

    // Allow for instantiation general record readers from a record reader path passed into segment generator config
    // If this is set, this will override the file format
    if (recordReaderClassName != null) {
      if (fileFormat != FileFormat.OTHER) {
        // NOTE: we currently have default file format set to AVRO inside segment generator config, do not want to break
        // this behavior for clients.
        LOGGER.warn("Using class: {} to read segment, ignoring configured file format: {}", recordReaderClassName,
            fileFormat);
      }
      return RecordReaderFactory.getRecordReaderByClass(recordReaderClassName, dataFile, sourceFields,
          segmentGeneratorConfig.getReaderConfig());
    }

    // NOTE: PinotSegmentRecordReader does not support time conversion (field spec must match)
    if (fileFormat == FileFormat.PINOT) {
      return new PinotSegmentRecordReader(dataFile, schema, segmentGeneratorConfig.getColumnSortOrder());
    } else {
      return RecordReaderFactory.getRecordReader(fileFormat, dataFile, sourceFields,
          segmentGeneratorConfig.getReaderConfig());
    }
  }

  public RecordReader getRecordReader() {
    return _recordReader;
  }

  public void init(SegmentGeneratorConfig config, RecordReader recordReader)
      throws Exception {
    init(config, new RecordReaderSegmentCreationDataSource(recordReader),
        new TransformPipeline(config.getTableConfig(), config.getSchema()));
  }

  public void init(SegmentGeneratorConfig config, SegmentCreationDataSource dataSource,
      TransformPipeline transformPipeline)
      throws Exception {
    _config = config;
    _recordReader = dataSource.getRecordReader();
    _dataSchema = config.getSchema();
    _continueOnError = config.isContinueOnError();

    if (config.isFailOnEmptySegment()) {
      Preconditions.checkState(_recordReader.hasNext(), "No record in data source");
    }
    _transformPipeline = transformPipeline;
    // Use the same transform pipeline if the data source is backed by a record reader
    if (dataSource instanceof RecordReaderSegmentCreationDataSource) {
      ((RecordReaderSegmentCreationDataSource) dataSource).setTransformPipeline(transformPipeline);
    }

    // Optimization for realtime segment conversion
    if (dataSource instanceof RealtimeSegmentSegmentCreationDataSource) {
      _config.setRealtimeConversion(true);
      _config.setConsumerDir(((RealtimeSegmentSegmentCreationDataSource) dataSource).getConsumerDir());
    }

    // For stats collection
    _dataSource = dataSource;

    // Initialize index creation
    _segmentIndexCreationInfo = new SegmentIndexCreationInfo();
    _indexCreationInfoMap = new TreeMap<>();
    _indexCreator = new SegmentColumnarIndexCreator();

    // Ensure that the output directory exists
    final File indexDir = new File(config.getOutDir());
    if (!indexDir.exists()) {
      indexDir.mkdirs();
    }

    _ingestionSchemaValidator =
        SchemaValidatorFactory.getSchemaValidator(_dataSchema, _recordReader.getClass().getName(),
            config.getInputFilePath());

    // Create a temporary directory used in segment creation
    _tempIndexDir = new File(indexDir, "tmp-" + UUID.randomUUID());
    LOGGER.debug("tempIndexDir:{}", _tempIndexDir);
  }

  /**
   * Generate a mutable docId to immutable docId mapping from the sortedDocIds iteration order
   *
   * @param sortedDocIds used to map sortedDocIds[immutableId] = mutableId (based on RecordReader iteration order)
   * @return int[] used to map output[mutableId] = immutableId, or null if sortedDocIds is null
   */
  private int[] getImmutableToMutableIdMap(@Nullable int[] sortedDocIds) {
    if (sortedDocIds == null) {
      return null;
    }
    int[] res = new int[sortedDocIds.length];
    for (int i = 0; i < res.length; i++) {
      res[sortedDocIds[i]] = i;
    }
    return res;
  }

  /**
   * Get sorted document IDs from the record reader if it supports this functionality.
   * This method handles the fact that getSortedDocIds() was removed from the RecordReader interface
   * but is still available on specific implementations.
   *
   * @return sorted document IDs array, or null if not available
   */
  @Nullable
  private int[] getSortedDocIdsFromRecordReader() {
    if (_recordReader instanceof PinotSegmentRecordReader) {
      return ((PinotSegmentRecordReader) _recordReader).getSortedDocIds();
    } else if (_recordReader instanceof CompactedPinotSegmentRecordReader) {
      return ((CompactedPinotSegmentRecordReader) _recordReader).getSortedDocIds();
    }
    return null;
  }

  // Helper method to inspect what's in _indexCreationInfoMap
  private void inspectIndexCreationInfo() {
    if (_indexCreationInfoMap == null) {
      System.out.println("  _indexCreationInfoMap is null");
      return;
    }

    ColumnIndexCreationInfo jsonInfo = _indexCreationInfoMap.get("jsonColumn1");
    if (jsonInfo == null) {
      System.out.println("  No jsonColumn1 in _indexCreationInfoMap");
      return;
    }

    System.out.println("  jsonColumn1 distinct value count: " + jsonInfo.getDistinctValueCount());
    System.out.println("  jsonColumn1 total entries: " + jsonInfo.getTotalNumberOfEntries());

    Object sortedElements = jsonInfo.getSortedUniqueElementsArray();
    System.out.println("  jsonColumn1 sorted elements identity: " + System.identityHashCode(sortedElements));
    System.out.println("  jsonColumn1 sorted elements type: " +
            (sortedElements != null ? sortedElements.getClass().getName() : "null"));

    // If it's an array, print some values
    if (sortedElements != null && sortedElements.getClass().isArray()) {
      int length = java.lang.reflect.Array.getLength(sortedElements);
      System.out.println("  jsonColumn1 sorted elements array length: " + length);
      for (int i = 0; i < Math.min(5, length); i++) {
        Object elem = java.lang.reflect.Array.get(sortedElements, i);
        System.out.println("    Element " + i + ": " + elem +
                " (identity: " + System.identityHashCode(elem) + ")");
      }
    }
  }

  @Override
  public void build()
      throws Exception {
    // Count the number of documents and gather per-column statistics
    LOGGER.debug("Start building StatsCollector!");
    collectStatsAndIndexCreationInfo();
    LOGGER.info("Finished building StatsCollector!");
    LOGGER.info("Collected stats for {} documents", _totalDocs);

    _incompleteRowsFound = 0;
    _skippedRowsFound = 0;
    _sanitizedRowsFound = 0;
    try {
      // TODO: Eventually pull the doc Id sorting logic out of Record Reader so that all row oriented logic can be
      //    removed from this code.
      int[] immutableToMutableIdMap = null;
      if (_recordReader instanceof PinotSegmentRecordReader) {
        immutableToMutableIdMap =
            getImmutableToMutableIdMap(((PinotSegmentRecordReader) _recordReader).getSortedDocIds());
      }

      // Initialize the index creation using the per-column statistics information
      // TODO: _indexCreationInfoMap holds the reference to all unique values on heap (ColumnIndexCreationInfo ->
      //       ColumnStatistics) throughout the segment creation. Find a way to release the memory early.
      _indexCreator.init(_config, _segmentIndexCreationInfo, _indexCreationInfoMap, _dataSchema, _tempIndexDir,
          immutableToMutableIdMap);

      // Build the index
      _recordReader.rewind();
      LOGGER.info("Start building IndexCreator!");
      GenericRow reuse = new GenericRow();
      Map<Integer, Integer> rowToStringIdentity = new LinkedHashMap<>();
      int rowNum = 0;
      System.out.println("=== STARTING MAIN INDEX BUILD LOOP ===");

      while (_recordReader.hasNext()) {
        long recordReadStopTimeNs;
        reuse.clear();

//        TransformPipeline.Result result;
        try {
          long recordReadStartTimeNs = System.nanoTime();

          GenericRow decodedRow = _recordReader.next(reuse);
          Object jsonValue = decodedRow.getValue("jsonColumn1");

          System.out.println("BUILD row " + rowNum + " - jsonColumn1: " + jsonValue +
                  " (identity: " + System.identityHashCode(jsonValue) +
                  ", class: " + (jsonValue != null ? jsonValue.getClass().getName() : "null") + ")");

          // DEFENSIVE COPY: Create a safe copy to prevent row reuse corruption
          GenericRow safeCopy = decodedRow.copy();

          Object copiedJson = safeCopy.getValue("jsonColumn1");
          System.out.println("  After copy - jsonColumn1: " + copiedJson +
                  " (identity: " + System.identityHashCode(copiedJson) +
                  ", class: " + (copiedJson != null ? copiedJson.getClass().getName() : "null") + ")");

          // Track String object identities across ALL rows
          int copiedIdentity = System.identityHashCode(copiedJson);
          if (rowToStringIdentity.containsValue(copiedIdentity)) {
            System.out.println("  WARNING: Object identity " + copiedIdentity +
                    " REUSED from previous row!");
            for (Map.Entry<Integer, Integer> entry : rowToStringIdentity.entrySet()) {
              if (entry.getValue() == copiedIdentity) {
                System.out.println("  Previously seen in row " + entry.getKey());
              }
            }
          }
            rowToStringIdentity.put(rowNum, copiedIdentity);
          } else {
              System.out.println("copiedJson is a " + copiedJson.getClass().getName());
          }

          TransformPipeline.Result result = _transformPipeline.processRow(safeCopy);

          recordReadStopTimeNs = System.nanoTime();
          _totalRecordReadTimeNs += recordReadStopTimeNs - recordReadStartTimeNs;
        } catch (Exception e) {
          if (!_continueOnError) {
            throw new RuntimeException("Error occurred while reading row during indexing", e);
          } else {
            _incompleteRowsFound++;
            LOGGER.debug("Error occurred while reading row during indexing", e);
            continue;
          }
        }

        for (GenericRow row : result.getTransformedRows()) {
          Object transformedJson = row.getValue("jsonColumn1");
          System.out.println("  After transform - jsonColumn1: " + transformedJson +
                  " (identity: " + System.identityHashCode(transformedJson) +
                  ", class: " + (transformedJson != null ? transformedJson.getClass().getName() : "null") + ")");

          // Track the transformed String identity
          int transformedIdentity = System.identityHashCode(transformedJson);
          System.out.println("  Indexing row " + rowNum + " with String identity: " + transformedIdentity);

          _indexCreator.indexRow(row);
        }
        _totalIndexTimeNs += System.nanoTime() - recordReadStopTimeNs;
        _incompleteRowsFound += result.getIncompleteRowCount();
        _skippedRowsFound += result.getSkippedRowCount();
        _sanitizedRowsFound += result.getSanitizedRowCount();
        rowNum++;
      }
      System.out.println("=== FINISHED MAIN INDEX BUILD LOOP ===");
    } catch (Exception e) {
      _indexCreator.close();
      throw e;
    } finally {
      _recordReader.close();
    }

    if (_incompleteRowsFound > 0) {
      System.out.println("Incomplete data found for " + _incompleteRowsFound + " records. This can be due to error during reader or transformations");
      LOGGER.warn("Incomplete data found for {} records. This can be due to error during reader or transformations",
          _incompleteRowsFound);
    }
    if (_skippedRowsFound > 0) {
      System.out.println("Skipped " + _skippedRowsFound + " records during transformation");
      LOGGER.info("Skipped {} records during transformation", _skippedRowsFound);
    }
    if (_sanitizedRowsFound > 0) {
      System.out.println("Sanitized " + _sanitizedRowsFound + " records during transformation");
      LOGGER.info("Sanitized {} records during transformation", _sanitizedRowsFound);
    }

    MinionMetrics metrics = MinionMetrics.get();
    String tableNameWithType = _config.getTableConfig().getTableName();
    if (_incompleteRowsFound > 0) {
      metrics.addMeteredTableValue(tableNameWithType, MinionMeter.TRANSFORMATION_ERROR_COUNT, _incompleteRowsFound);
    }
    if (_skippedRowsFound > 0) {
      metrics.addMeteredTableValue(tableNameWithType, MinionMeter.DROPPED_RECORD_COUNT, _skippedRowsFound);
    }
    if (_sanitizedRowsFound > 0) {
      metrics.addMeteredTableValue(tableNameWithType, MinionMeter.CORRUPTED_RECORD_COUNT, _sanitizedRowsFound);
    }

    LOGGER.info("Finished records indexing in IndexCreator!");

    handlePostCreation();
  }

  public void buildByColumn(IndexSegment indexSegment, ThreadSafeMutableRoaringBitmap validDocIds)
      throws Exception {
    // Count the number of documents and gather per-column statistics
    LOGGER.debug("Start building StatsCollector!");
    collectStatsAndIndexCreationInfo();
    LOGGER.info("Finished building StatsCollector!");
    LOGGER.info("Collected stats for {} documents", _totalDocs);

    try {
      // TODO: Eventually pull the doc Id sorting logic out of Record Reader so that all row oriented logic can be
      //    removed from this code.
      int[] sortedDocIds = getSortedDocIdsFromRecordReader();
      int[] immutableToMutableIdMap = getImmutableToMutableIdMap(sortedDocIds);

      // Initialize the index creation using the per-column statistics information
      // TODO: _indexCreationInfoMap holds the reference to all unique values on heap (ColumnIndexCreationInfo ->
      //       ColumnStatistics) throughout the segment creation. Find a way to release the memory early.
      _indexCreator.init(_config, _segmentIndexCreationInfo, _indexCreationInfoMap, _dataSchema, _tempIndexDir,
          immutableToMutableIdMap);

      // Build the indexes
      LOGGER.info("Start building Index by column");

      TreeSet<String> columns = _dataSchema.getPhysicalColumnNames();

      for (String col : columns) {
        _indexCreator.indexColumn(col, sortedDocIds, indexSegment, validDocIds);
      }
    } catch (Exception e) {
      _indexCreator.close();
      throw e;
    } finally {
      // The record reader is created by the `init` method and needs to be closed and
      // cleaned up even by the Column Mode builder.
      _recordReader.close();
    }

    // TODO: Using column oriented, we can't catch incomplete records.  Does that matter?

    LOGGER.info("Finished records indexing by column in IndexCreator!");

    handlePostCreation();
  }

  private void handlePostCreation()
      throws Exception {
    System.out.println("=== ENTERING handlePostCreation ===");
    ColumnStatistics timeColumnStatistics = _segmentStats.getColumnProfileFor(_config.getTimeColumnName());
    int sequenceId = _config.getSequenceId();
    if (timeColumnStatistics != null) {
      if (_totalDocs > 0) {
        _segmentName = _config.getSegmentNameGenerator()
            .generateSegmentName(sequenceId, timeColumnStatistics.getMinValue(), timeColumnStatistics.getMaxValue());
      } else {
        // When totalDoc is 0, check whether 'failOnEmptySegment' option is true. If so, directly fail the segment
        // creation.
        Preconditions.checkArgument(!_config.isFailOnEmptySegment(),
            "Failing the empty segment creation as the option 'failOnEmptySegment' is set to: "
                + _config.isFailOnEmptySegment());
        // Generate a unique name for a segment with no rows
        long now = System.currentTimeMillis();
        _segmentName = _config.getSegmentNameGenerator().generateSegmentName(sequenceId, now, now);
      }
    } else {
      _segmentName = _config.getSegmentNameGenerator().generateSegmentName(sequenceId, null, null);
    }

    try {
      System.out.println("=== CHECKPOINT A: BEFORE _indexCreator.seal ===");
      // Write the index files to disk
      _indexCreator.setSegmentName(_segmentName);
      _indexCreator.seal();
      System.out.println("=== CHECKPOINT B: AFTER _indexCreator.seal ===");
    } finally {
      _indexCreator.close();
      System.out.println("=== CHECKPOINT C: AFTER _indexCreator.close ===");
    }
    LOGGER.info("Finished segment seal!");

    // Delete the directory named after the segment name, if it exists
    final File outputDir = new File(_config.getOutDir());
    final File segmentOutputDir = new File(outputDir, _segmentName);
    if (segmentOutputDir.exists()) {
      FileUtils.deleteDirectory(segmentOutputDir);
    }

    // Move the temporary directory into its final location
    FileUtils.moveDirectory(_tempIndexDir, segmentOutputDir);

    // Delete the temporary directory
    FileUtils.deleteQuietly(_tempIndexDir);

    System.out.println("=== CHECKPOINT D: BEFORE convertFormatIfNecessary ===");
    convertFormatIfNecessary(segmentOutputDir);
    System.out.println("=== CHECKPOINT E: AFTER convertFormatIfNecessary ===");

    if (_totalDocs > 0) {
      System.out.println("=== CHECKPOINT F: BEFORE buildStarTreeV2IfNecessary ===");
      buildStarTreeV2IfNecessary(segmentOutputDir);
      System.out.println("=== CHECKPOINT G: AFTER buildStarTreeV2IfNecessary ===");
      System.out.println("=== CHECKPOINT H: BEFORE buildMultiColumnTextIndex ===");
      buildMultiColumnTextIndex(segmentOutputDir);
      System.out.println("=== CHECKPOINT I: AFTER buildMultiColumnTextIndex ===");
    }

    System.out.println("=== CHECKPOINT J: BEFORE updatePostSegmentCreationIndexes ===");
    updatePostSegmentCreationIndexes(segmentOutputDir);
    System.out.println("=== CHECKPOINT K: AFTER updatePostSegmentCreationIndexes ===");

    // Compute CRC and creation time
    long crc = CrcUtils.forAllFilesInFolder(segmentOutputDir).computeCrc();
    long creationTime;
    String creationTimeInConfig = _config.getCreationTime();
    if (creationTimeInConfig != null) {
      try {
        creationTime = Long.parseLong(creationTimeInConfig);
      } catch (Exception e) {
        LOGGER.error("Caught exception while parsing creation time in config, use current time as creation time");
        creationTime = System.currentTimeMillis();
      }
    } else {
      creationTime = System.currentTimeMillis();
    }

    // Persist creation metadata to disk
    persistCreationMeta(segmentOutputDir, crc, creationTime);

    LOGGER.info("Driver, record read time (in ms) : {}", TimeUnit.NANOSECONDS.toMillis(_totalRecordReadTimeNs));
    LOGGER.info("Driver, stats collector time (in ms) : {}", TimeUnit.NANOSECONDS.toMillis(_totalStatsCollectorTimeNs));
    LOGGER.info("Driver, indexing time (in ms) : {}", TimeUnit.NANOSECONDS.toMillis(_totalIndexTimeNs));
    System.out.println("=== EXITING handlePostCreation ===");
  }

  private void buildMultiColumnTextIndex(File segmentOutputDir)
      throws Exception {
    if (_config.getMultiColumnTextIndexConfig() != null) {
      PinotConfiguration segmentDirectoryConfigs =
          new PinotConfiguration(Map.of(IndexLoadingConfig.READ_MODE_KEY, ReadMode.mmap));

      TableConfig tableConfig = _config.getTableConfig();
      Schema schema = _config.getSchema();
      SegmentDirectoryLoaderContext segmentLoaderContext =
          new SegmentDirectoryLoaderContext.Builder()
              .setTableConfig(tableConfig)
              .setSchema(schema)
              .setSegmentName(_segmentName)
              .setSegmentDirectoryConfigs(segmentDirectoryConfigs)
              .build();

      IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, tableConfig, schema);

      try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
          .load(segmentOutputDir.toURI(), segmentLoaderContext);
          SegmentDirectory.Writer segmentWriter = segmentDirectory.createWriter()) {
        MultiColumnTextIndexHandler handler = new MultiColumnTextIndexHandler(segmentDirectory, indexLoadingConfig,
            _config.getMultiColumnTextIndexConfig());
        handler.updateIndices(segmentWriter);
        handler.postUpdateIndicesCleanup(segmentWriter);
      }
    }
  }

  private void updatePostSegmentCreationIndexes(File indexDir)
      throws Exception {
    Set<IndexType> postSegCreationIndexes = IndexService.getInstance().getAllIndexes().stream()
        .filter(indexType -> indexType.getIndexBuildLifecycle() == IndexType.BuildLifecycle.POST_SEGMENT_CREATION)
        .collect(Collectors.toSet());

    if (!postSegCreationIndexes.isEmpty()) {
      // Build other indexes
      Map<String, Object> props = new LinkedHashMap<>();
      props.put(IndexLoadingConfig.READ_MODE_KEY, ReadMode.mmap);
      PinotConfiguration segmentDirectoryConfigs = new PinotConfiguration(props);

      TableConfig tableConfig = _config.getTableConfig();
      Schema schema = _config.getSchema();
      SegmentDirectoryLoaderContext segmentLoaderContext =
          new SegmentDirectoryLoaderContext.Builder()
              .setTableConfig(tableConfig)
              .setSchema(schema)
              .setSegmentName(_segmentName)
              .setSegmentDirectoryConfigs(segmentDirectoryConfigs)
              .build();

      IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(null, tableConfig, schema);

      try (SegmentDirectory segmentDirectory = SegmentDirectoryLoaderRegistry.getDefaultSegmentDirectoryLoader()
          .load(indexDir.toURI(), segmentLoaderContext);
          SegmentDirectory.Writer segmentWriter = segmentDirectory.createWriter()) {
        for (IndexType indexType : postSegCreationIndexes) {
          IndexHandler handler =
              indexType.createIndexHandler(segmentDirectory, indexLoadingConfig.getFieldIndexConfigByColName(), schema,
                  tableConfig);
          handler.updateIndices(segmentWriter);
        }
      }
    }
  }

  private void buildStarTreeV2IfNecessary(File indexDir)
      throws Exception {
    List<StarTreeIndexConfig> starTreeIndexConfigs = _config.getStarTreeIndexConfigs();
    boolean enableDefaultStarTree = _config.isEnableDefaultStarTree();
    if (CollectionUtils.isNotEmpty(starTreeIndexConfigs) || enableDefaultStarTree) {
      MultipleTreesBuilder.BuildMode buildMode =
          _config.isOnHeap() ? MultipleTreesBuilder.BuildMode.ON_HEAP : MultipleTreesBuilder.BuildMode.OFF_HEAP;
      try (
          MultipleTreesBuilder builder = new MultipleTreesBuilder(starTreeIndexConfigs, enableDefaultStarTree, indexDir,
              buildMode)) {
        builder.build();
      }
    }
  }

  // Explanation of why we are using format converter:
  // There are 3 options to correctly generate segments to v3 format
  // 1. Generate v3 directly: This is efficient but v3 index writer needs to know buffer size upfront.
  // Inverted, star and raw indexes don't have the index size upfront. This is also least flexible approach
  // if we add more indexes in future.
  // 2. Hold data in-memory: One way to work around predeclaring sizes in (1) is to allocate "large" buffer (2GB?)
  // and hold the data in memory and write the buffer at the end. The memory requirement in this case increases linearly
  // with the number of columns. Variation of that is to mmap data to separate files...which is what we are doing here
  // 3. Another option is to generate dictionary and fwd indexes in v3 and generate inverted, star and raw indexes in
  // separate files. Then add those files to v3 index file. This leads to lot of hodgepodge code to
  // handle multiple segment formats.
  // Using converter is similar to option (2), plus it's battle-tested code. We will roll out with
  // this change to keep changes limited. Once we've migrated we can implement approach (1) with option to
  // copy for indexes for which we don't know sizes upfront.
  private void convertFormatIfNecessary(File segmentDirectory)
      throws Exception {
    System.out.println("=== convertFormatIfNecessary: segmentVersion = " + _config.getSegmentVersion() + " ===");
    SegmentVersion versionToGenerate = _config.getSegmentVersion();
    if (versionToGenerate.equals(SegmentVersion.v1)) {
      System.out.println("=== convertFormatIfNecessary: v1, returning early ===");
      // v1 by default
      return;
    }
    System.out.println("=== convertFormatIfNecessary: converting to v3 ===");
    SegmentFormatConverter converter = SegmentFormatConverterFactory.getConverter(SegmentVersion.v1, SegmentVersion.v3);
    converter.convert(segmentDirectory);
    System.out.println("=== convertFormatIfNecessary: conversion complete ===");
  }

  @Override
  public ColumnStatistics getColumnStatisticsCollector(final String columnName)
      throws Exception {
    return _segmentStats.getColumnProfileFor(columnName);
  }

  public static void persistCreationMeta(File indexDir, long crc, long creationTime)
      throws IOException {
    File segmentDir = SegmentDirectoryPaths.findSegmentDirectory(indexDir);
    File creationMetaFile = new File(segmentDir, V1Constants.SEGMENT_CREATION_META);
    try (DataOutputStream output = new DataOutputStream(new FileOutputStream(creationMetaFile))) {
      output.writeLong(crc);
      output.writeLong(creationTime);
    }
  }

  /**
   * Complete the stats gathering process and store the stats information in indexCreationInfoMap.
   */
  void collectStatsAndIndexCreationInfo() throws Exception {
    long statsCollectorStartTime = System.nanoTime();

    System.out.println("=== collectStatsAndIndexCreationInfo: START ===");
    
    _segmentStats = _dataSource.gatherStats(
        new StatsCollectorConfig(_config.getTableConfig(), _dataSchema, _config.getSegmentPartitionConfig()));
    
    _totalDocs = _segmentStats.getTotalDocCount();
    Map<String, FieldIndexConfigs> indexConfigsMap = _config.getIndexConfigsByColName();

    for (FieldSpec fieldSpec : _dataSchema.getAllFieldSpecs()) {
        if (fieldSpec.isVirtualColumn()) {
            continue;
        }

        String column = fieldSpec.getName();
        DataType storedType = fieldSpec.getDataType().getStoredType();
        
        // GET THE COLUMN STATISTICS
        ColumnStatistics columnProfile = _segmentStats.getColumnProfileFor(column);
        
        // DEBUG: Identify which stats collector class is being used for jsonColumn1
        if ("jsonColumn1".equals(column)) {
            System.out.println("\n=== IDENTIFYING jsonColumn1 STATS COLLECTOR ===");
            System.out.println("columnProfile class: " + columnProfile.getClass().getName());
            System.out.println("columnProfile toString: " + columnProfile);
            System.out.println("Cardinality: " + columnProfile.getCardinality());
            System.out.println("Total entries: " + columnProfile.getTotalNumberOfEntries());
            
            // Try to call getUniqueValuesSet and see what happens
            try {
                Object uniqueValuesSet = columnProfile.getUniqueValuesSet();
                System.out.println("getUniqueValuesSet() succeeded!");
                System.out.println("  Class: " + (uniqueValuesSet != null ? uniqueValuesSet.getClass().getName() : "null"));
                
                // Print the values
                if (uniqueValuesSet != null && uniqueValuesSet.getClass().isArray()) {
                    int length = java.lang.reflect.Array.getLength(uniqueValuesSet);
                    System.out.println("  Array length: " + length);
                    for (int i = 0; i < length; i++) {
                        Object val = java.lang.reflect.Array.get(uniqueValuesSet, i);
                        System.out.println("    [" + i + "]: " + val + " (identity: " + System.identityHashCode(val) + ")");
                    }
                }
            } catch (Exception e) {
                System.out.println("getUniqueValuesSet() threw exception: " + e.getClass().getName() + ": " + e.getMessage());
                System.out.println("This means we're using NoDictColumnStatisticsCollector");
            }
            System.out.println("=== END IDENTIFICATION ===\n");
        }
        
        // ... rest of the method
        DictionaryIndexConfig dictionaryIndexConfig = indexConfigsMap.get(column).getConfig(StandardIndexes.dictionary());
        boolean createDictionary = dictionaryIndexConfig.isDisabled();
        boolean useVarLengthDictionary = dictionaryIndexConfig.getUseVarLengthDictionary()
            || DictionaryIndexType.optimizeTypeShouldUseVarLengthDictionary(storedType, columnProfile);
        Object defaultNullValue = fieldSpec.getDefaultNullValue();
        if (storedType == DataType.BYTES) {
            defaultNullValue = new ByteArray((byte[]) defaultNullValue);
        }
        
        _indexCreationInfoMap.put(column,
            new ColumnIndexCreationInfo(columnProfile, createDictionary, useVarLengthDictionary, false, defaultNullValue));
    }
    
    _segmentIndexCreationInfo.setTotalDocs(_totalDocs);
    _totalStatsCollectorTimeNs = System.nanoTime() - statsCollectorStartTime;
    
    System.out.println("=== collectStatsAndIndexCreationInfo: END ===");
}

  /**
   * Uses config and column properties like storedType and length of elements to determine if
   * varLengthDictionary should be used for a column
   * @deprecated Use
   * {@link DictionaryIndexType#shouldUseVarLengthDictionary(String, Set, DataType, ColumnStatistics)} instead.
   */
  @Deprecated
  public static boolean shouldUseVarLengthDictionary(String columnName, Set<String> varLengthDictColumns,
      DataType columnStoredType, ColumnStatistics columnProfile) {
    return DictionaryIndexType.shouldUseVarLengthDictionary(columnName, varLengthDictColumns, columnStoredType,
        columnProfile);
  }

  /**
   * Returns the name of the segment associated with this index creation driver.
   */
  @Override
  public String getSegmentName() {
    return _segmentName;
  }

  /**
   * Returns the path of the output directory
   */
  @Override
  public File getOutputDirectory() {
    return new File(new File(_config.getOutDir()), _segmentName);
  }

  /**
   * Returns the schema validator.
   */
  @Override
  public IngestionSchemaValidator getIngestionSchemaValidator() {
    return _ingestionSchemaValidator;
  }

  public SegmentPreIndexStatsContainer getSegmentStats() {
    return _segmentStats;
  }

  public int getIncompleteRowsFound() {
    return _incompleteRowsFound;
  }

  public int getSkippedRowsFound() {
    return _skippedRowsFound;
  }

  public int getSanitizedRowsFound() {
    return _sanitizedRowsFound;
  }
}
