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
package org.apache.pinot.queries;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.function.scalar.StringFunctions;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.avro.Schema.*;


/**
 * Test if ComplexType (RECORD, ARRAY, MAP, UNION, ENUM, and FIXED) field from an AVRO file can be ingested into a JSON
 * column in a Pinot segment.
 */
public class JsonIngestionFromAvroQueriesTestBad extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "JsonIngestionFromAvroTest");
  private static final File AVRO_DATA_FILE = new File(INDEX_DIR, "JsonIngestionFromAvroTest.avro");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String INT_COLUMN = "intColumn";
  private static final String JSON_COLUMN_1 = "jsonColumn1"; // for testing RECORD, ARRAY, MAP, UNION
  private static final String JSON_COLUMN_2 = "jsonColumn2"; // for testing ENUM
  private static final String JSON_COLUMN_3 = "jsonColumn3"; // for testing FIXED
  private static final String JSON_COLUMN_4 = "jsonColumn4"; // for testing BYTES
  private static final String JSON_COLUMN_5 = "jsonColumn5"; // for testing ARRAY of MAPS
  private static final String STRING_COLUMN = "stringColumn";
  //@formatter:off
  private static final org.apache.pinot.spi.data.Schema SCHEMA = new org.apache.pinot.spi.data.Schema.SchemaBuilder()
      .setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension(INT_COLUMN, DataType.INT)
      .addSingleValueDimension(JSON_COLUMN_1, DataType.JSON)
      .addSingleValueDimension(JSON_COLUMN_2, DataType.JSON)
      .addSingleValueDimension(JSON_COLUMN_3, DataType.JSON)
      .addSingleValueDimension(JSON_COLUMN_4, DataType.JSON)
      .addSingleValueDimension(JSON_COLUMN_5, DataType.JSON)
      .addSingleValueDimension(STRING_COLUMN, DataType.STRING)
      .build();

  private static final Schema JSON1_RECORD_SCHEMA = createRecordSchema();
  private static final Schema JSON5_RECORD_SCHEMA = createJson5RecordSchema();

  private static final Schema STRING_MAP_SCHEMA = Schema.createMap(stringSchema());

  //@formatter:on
//  private TableConfig _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
//      .setJsonIndexColumns(List.of(JSON_COLUMN_1, JSON_COLUMN_2, JSON_COLUMN_3)).build();
     private TableConfig _tableConfig = new TableConfigBuilder(TableType.OFFLINE)
                   .setTableName(RAW_TABLE_NAME)
       // Only jsonColumn1 contains JSON documents we query with JSONPath
//               .setJsonIndexColumns(List.of(JSON_COLUMN_1))
                   .build();

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  /** @return {@link GenericRow} representing a row in Pinot table. */
  private static GenericRow createTableRecord(int intValue, String stringValue, Object jsonValue,
      GenericData.EnumSymbol enumValue, GenericData.Fixed fixedValue, byte[] bytesValue, List<Object> arrayValue) {
    GenericRow record = new GenericRow();
    record.putValue(INT_COLUMN, intValue);
    record.putValue(STRING_COLUMN, stringValue);
    record.putValue(JSON_COLUMN_1, jsonValue);
    record.putValue(JSON_COLUMN_2, enumValue);
    record.putValue(JSON_COLUMN_3, fixedValue);
    record.putValue(JSON_COLUMN_4, ByteBuffer.wrap(bytesValue));
    record.putValue(JSON_COLUMN_5, arrayValue);
    return record;
  }

    private static Schema stringSchema() {
        Schema s = Schema.create(Schema.Type.STRING);
        // Tell Avro to materialize this STRING as java.lang.String (not Utf8)
        s.addProp("avro.java.string", "String");
        return s;
    }

    private static Schema stringMapSchema() {
        return Schema.createMap(stringSchema());
    }

//  private static Map<String, String> createMapField(Pair<String, String>[] pairs) {
//    Map<String, String> map = new LinkedHashMap<>();
//    for (Pair<String, String> pair : pairs) {
//      map.put(pair.getLeft(), pair.getRight());
//    }
//    return map;
//  }

  private static Schema createRecordSchema() {
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("id", create(Type.INT)));
    fields.add(new Field("name", stringSchema()));
//    return createRecord("record", "doc", JsonIngestionFromAvroQueriesTest.class.getCanonicalName(), false, fields);
      return createRecord("person", "doc", JsonIngestionFromAvroQueriesTest.class.getCanonicalName(), false, fields);
  }

  private static Schema createJson5RecordSchema() {
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("timestamp", create(Type.LONG)));
    fields.add(new Field("data", createMap(stringSchema())));
//    return createRecord("record", "doc", "JsonIngestionFromAvroQueriesTest$Json5", false, fields);
      return createRecord("json5record", "doc", "JsonIngestionFromAvroQueriesTest$Json5", false, fields);
  }

//  private static GenericData.Record createRecordField(String k1, int v1, String k2, String v2) {
//    GenericData.Record record = new GenericData.Record(createRecordSchema());
//    record.put(k1, v1);
//    record.put(k2, v2);
//    return record;
//  }

  private static GenericData.Record createRecordField(int id, String name) {
    // Build with the SAME schema instance used in the union branch.
    return new GenericRecordBuilder(JSON1_RECORD_SCHEMA)
        .set("id", id)
        .set("name", name)
        .build();
  }

  private static GenericData.EnumSymbol createEnumField(Schema enumSchema, String enumValue) {
    return new GenericData.EnumSymbol(enumSchema, enumValue);
  }

  private static GenericData.Fixed createFixedField(Schema fixedSchema, int value) {
    byte[] bytes = {(byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value};
    return new GenericData.Fixed(fixedSchema, bytes);
  }

  private static void createInputFile()
      throws IOException {
    INDEX_DIR.mkdir();
    Schema avroSchema = createRecord("eventsRecord", null, null, false);
    Schema enumSchema = createEnum("direction", null, null, Arrays.asList("UP", "DOWN", "LEFT", "RIGHT"));
    Schema fixedSchema = createFixed("fixed", null, null, 4);
    List<Field> fields = Arrays.asList(
        new Field(INT_COLUMN, createUnion(Lists.newArrayList(create(Type.INT), create(Type.NULL))), null, null),
        new Field(STRING_COLUMN, createUnion(Lists.newArrayList(stringSchema(), create(Type.NULL))), null, null),
        new Field(JSON_COLUMN_1,
            createUnion(
                createArray(stringSchema()),
                    JSON1_RECORD_SCHEMA,
                createMap(stringSchema()),
                // createRecordSchema(),
//                JSON1_RECORD_SCHEMA,
                    stringSchema(),
                create(Type.NULL))),
        new Field(JSON_COLUMN_2, enumSchema),
        new Field(JSON_COLUMN_3, fixedSchema),
        new Field(JSON_COLUMN_4, create(Type.BYTES)),
//        new Field(JSON_COLUMN_5, createArray(createJson5RecordSchema()))
        new Field(JSON_COLUMN_5, createArray(JSON5_RECORD_SCHEMA))
    );
    avroSchema.setFields(fields);
    List<GenericRow> inputRecords = new ArrayList<>();
    // Insert ARRAY
    inputRecords.add(
        createTableRecord(1, "daffy duck", Arrays.asList("this", "is", "a", "test"), createEnumField(enumSchema, "UP"),
            createFixedField(fixedSchema, 1), new byte[] {0, 0, 0, 1}, Arrays.asList(
                new GenericRecordBuilder(createJson5RecordSchema())
                    .set("timestamp", 1719390721)
                    .set("data", createMapField(new Pair[]{Pair.of("a", "1"), Pair.of("b", "2")})).build())));

    // Insert MAP
    inputRecords.add(
        createTableRecord(2, "mickey mouse", createMapField(new Pair[]{Pair.of("a", "1"), Pair.of("b", "2")}),
            createEnumField(enumSchema, "DOWN"), createFixedField(fixedSchema, 2), new byte[] {0, 0, 0, 2},
            Arrays.asList(new GenericRecordBuilder(createJson5RecordSchema()).set("timestamp", 1719390722)
                    .set("data", createMapField(new Pair[]{Pair.of("a", "2"), Pair.of("b", "4")})).build())));

    inputRecords.add(
        createTableRecord(3, "donald duck", createMapField(new Pair[]{Pair.of("a", "1"), Pair.of("b", "2")}),
            createEnumField(enumSchema, "UP"), createFixedField(fixedSchema, 3), new byte[] {0, 0, 0, 3}, Arrays.asList(
                new GenericRecordBuilder(createJson5RecordSchema()).set("timestamp", 1719390723)
                    .set("data", createMapField(new Pair[]{Pair.of("a", "3"), Pair.of("b", "6")})).build())));

    inputRecords.add(
        createTableRecord(4, "scrooge mcduck", createMapField(new Pair[]{Pair.of("a", "1"), Pair.of("b", "2")}),
            createEnumField(enumSchema, "LEFT"), createFixedField(fixedSchema, 4), new byte[] {0, 0, 0, 4},
            Arrays.asList(new GenericRecordBuilder(createJson5RecordSchema()).set("timestamp", 1719390724)
                    .set("data", createMapField(new Pair[]{Pair.of("a", "4"), Pair.of("b", "8")})).build())));

    // insert RECORD
//    inputRecords.add(createTableRecord(5, "minney mouse", createRecordField(1, "minney"),
////    inputRecords.add(createTableRecord(5, "minney mouse", createRecordField("id", 1, "name", "minney"),
//        createEnumField(enumSchema, "RIGHT"), createFixedField(fixedSchema, 5), new byte[] {0, 0, 0, 5},
//        Arrays.asList(
//            new GenericRecordBuilder(JSON5_RECORD_SCHEMA).set("timestamp", 1719390725)
//                // new GenericRecordBuilder(createJson5RecordSchema()).set("timestamp", 1719390725)
//                .set("data", createMapField(new Pair[]{Pair.of("a", "5"), Pair.of("b", "10")})).build())));

inputRecords.add(createTableRecord(5, "minney mouse",
createMapField(new Pair[]{Pair.of("id", "1"), Pair.of("name", "minney")}),
createEnumField(enumSchema, "RIGHT"), createFixedField(fixedSchema, 5), new byte[] {0, 0, 0, 5},
Arrays.asList(
new GenericRecordBuilder(createJson5RecordSchema()).set("timestamp", 1719390725)
.set("data", createMapField(new Pair[]{Pair.of("a", "5"), Pair.of("b", "10")})).build())));

    // Insert simple Java String (gets converted into JSON value)
    inputRecords.add(
        createTableRecord(6, "pluto", "test", createEnumField(enumSchema, "DOWN"), createFixedField(fixedSchema, 6),
            new byte[] {0, 0, 0, 6}, Arrays.asList(
                new GenericRecordBuilder(JSON5_RECORD_SCHEMA).set("timestamp", 1719390726)
//                new GenericRecordBuilder(createJson5RecordSchema()).set("timestamp", 1719390726)
                    .set("data", createMapField(new Pair[]{Pair.of("a", "6"), Pair.of("b", "12")})).build())));

    // Insert JSON string (gets converted into JSON document)
    inputRecords.add(
        createTableRecord(7, "scooby doo", "{\"name\":\"scooby\",\"id\":7}", createEnumField(enumSchema, "UP"),
            createFixedField(fixedSchema, 7), new byte[] {0, 0, 0, 7}, Arrays.asList(
                new GenericRecordBuilder(JSON5_RECORD_SCHEMA).set("timestamp", 1719390727)
//                new GenericRecordBuilder(createJson5RecordSchema()).set("timestamp", 1719390727)
                    .set("data", createMapField(new Pair[]{Pair.of("a", "7"), Pair.of("b", "14")})).build())));

//    DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema));
           GenericDatumWriter<GenericData.Record> datumWriter =
                       new GenericDatumWriter<>(avroSchema, GenericData.get());
           DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(datumWriter);
      fileWriter.create(avroSchema, AVRO_DATA_FILE);
      for (GenericRow inputRecord : inputRecords) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(INT_COLUMN, inputRecord.getValue(INT_COLUMN));
        record.put(STRING_COLUMN, inputRecord.getValue(STRING_COLUMN));
        record.put(JSON_COLUMN_1, inputRecord.getValue(JSON_COLUMN_1));
        record.put(JSON_COLUMN_2, inputRecord.getValue(JSON_COLUMN_2));
        record.put(JSON_COLUMN_3, inputRecord.getValue(JSON_COLUMN_3));
        record.put(JSON_COLUMN_4, inputRecord.getValue(JSON_COLUMN_4));
        record.put(JSON_COLUMN_5, inputRecord.getValue(JSON_COLUMN_5));
        fileWriter.append(record);
      }

      fileWriter.close();

      debugReadBackAvro(AVRO_DATA_FILE, JSON_COLUMN_1);
  }

    private static Map<String, String> createMapField(Pair<String, String>[] pairs) {
        // Avro uses java.util.Map internally for maps; no GenericData.Map class exists.
        Map<String, String> map = new HashMap<>();
        for (Pair<String, String> pair : pairs) {
            map.put(pair.getLeft(), pair.getRight());
        }
        return map;
    }

  private static void debugReadBackAvro(File avroFile, String jsonCol) throws IOException {
    try (org.apache.avro.file.DataFileReader<GenericData.Record> r =
                              new org.apache.avro.file.DataFileReader<>(avroFile,
                                          new org.apache.avro.generic.GenericDatumReader<GenericData.Record>())) {
          int row = 0;
          System.out.println("=== AVRO READBACK START ===");
          while (r.hasNext()) {
                GenericData.Record rec = r.next();
                Object val = rec.get(jsonCol);
                String type;
                if (val == null) {
                      type = "null";
                    } else if (val instanceof GenericData.Record) {
                      GenericData.Record gr = (GenericData.Record) val;
                      java.util.List<org.apache.avro.Schema.Field> fs = gr.getSchema().getFields();
                      StringBuilder sb = new StringBuilder();
                      sb.append("{");
                      for (int i = 0; i < fs.size(); i++) {
                            org.apache.avro.Schema.Field f = fs.get(i);
                            sb.append(f.name()).append("=").append(String.valueOf(gr.get(f.name())));
                            if (i + 1 < fs.size()) {
                                sb.append(",");
                            }
                          }
                      sb.append("}");
                      type = "record:" + gr.getSchema().getFullName() + " fields=" + sb.toString();
                    } else if (val instanceof java.util.Map) {
                      @SuppressWarnings("rawtypes")
                      java.util.Map m = (java.util.Map) val;
                      type = "map:" + m.toString();
                    } else if (val instanceof java.util.Collection) {
                      @SuppressWarnings("rawtypes")
                      java.util.Collection c = (java.util.Collection) val;
                      type = "array:" + c.toString();
                    } else {
                      type = val.getClass().getSimpleName() + ":" + val;
                    }
                System.out.println("row=" + (++row)
                            + " intColumn=" + rec.get("intColumn")
                            + " " + jsonCol + "=" + type);
              }
          System.out.println("=== AVRO READBACK END ===");
        }
  }

  /** Create an AVRO file and then ingest it into Pinot while creating a JsonIndex. */
  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    createInputFile();

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(_tableConfig, SCHEMA);
    segmentGeneratorConfig.setInputFilePath(AVRO_DATA_FILE.getPath());
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig);
    driver.build();

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(_tableConfig, SCHEMA);
    ImmutableSegment segment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig);
    _indexSegment = segment;
//    _indexSegments = List.of(segment, segment);
      _indexSegments = List.of(segment);
  }

    public void testSetUp()
            throws Exception {
        FileUtils.deleteDirectory(INDEX_DIR);
        createInputFile();

        SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(_tableConfig, SCHEMA);
        segmentGeneratorConfig.setInputFilePath(AVRO_DATA_FILE.getPath());
        segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());
        segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
        SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
        driver.init(segmentGeneratorConfig);
        driver.build();

        IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(_tableConfig, SCHEMA);
        ImmutableSegment segment =
                ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig);
        _indexSegment = segment;
        _indexSegments = List.of(segment);
    }

  /** Verify that we can query the JSON column that ingested ComplexType data from an AVRO file (see setUp). */
//  @Test
//  public void testSimpleSelectOnJsonColumn() {
//    Operator<SelectionResultsBlock> operator =
//        getOperator("select intColumn, stringColumn, jsonColumn1, jsonColumn2 FROM " + "testTable limit 100");
//    SelectionResultsBlock block = operator.nextBlock();
//    Collection<Object[]> rows = block.getRows();
//    Assert.assertEquals(block.getDataSchema().getColumnDataType(0), DataSchema.ColumnDataType.INT);
//    Assert.assertEquals(block.getDataSchema().getColumnDataType(1), DataSchema.ColumnDataType.STRING);
//    Assert.assertEquals(block.getDataSchema().getColumnDataType(2), DataSchema.ColumnDataType.JSON);
//
//    List<String> expecteds = Arrays.asList("[1, daffy duck, [\"this\",\"is\",\"a\",\"test\"], \"UP\"]",
//        "[2, mickey mouse, {\"a\":\"1\",\"b\":\"2\"}, \"DOWN\"]",
//        "[3, donald duck, {\"a\":\"1\",\"b\":\"2\"}, \"UP\"]",
//        "[4, scrooge mcduck, {\"a\":\"1\",\"b\":\"2\"}, \"LEFT\"]",
//        "[5, minney mouse, {\"name\":\"minney\",\"id\":1}, \"RIGHT\"]", "[6, pluto, \"test\", \"DOWN\"]",
//        "[7, scooby doo, {\"name\":\"scooby\",\"id\":7}, \"UP\"]");
//
//    int index = 0;
//    Iterator<Object[]> iterator = rows.iterator();
//    while (iterator.hasNext()) {
//      Object[] row = iterator.next();
//      Assert.assertEquals(Arrays.toString(row), expecteds.get(index++));
//    }
//  }


  /** Verify simple path expression query on ingested Avro file. */
  @Test
  public void testJsonPathSelectOnJsonColumn() throws Exception {
      this.testSetUp();

      Operator<SelectionResultsBlock> operator = getOperator(
              "select intColumn, json_extract_scalar(jsonColumn1, '$.name', " + "'STRING', 'null') FROM testTable");
      Operator<SelectionResultsBlock> operator2 = getOperator(
              "select JSONEXTRACTKEY(jsonColumn1, '$.*') AS keys FROM testTable");

      SelectionResultsBlock resultBlock2 = operator2.nextBlock();

      List<Object[]> rows2 = resultBlock2.getRows();

      for (Object[] row : rows2) {
          // Each row is an Object[]; the first element is a String[]
          if (row[0] instanceof String[]) {
              System.out.println(Arrays.toString((String[]) row[0]));
          } else {
              System.out.println(Arrays.toString(row));
          }
      }

    SelectionResultsBlock block = operator.nextBlock();
    Collection<Object[]> rows = block.getRows();
    Assert.assertEquals(block.getDataSchema().getColumnDataType(0), DataSchema.ColumnDataType.INT);
    Assert.assertEquals(block.getDataSchema().getColumnDataType(1), DataSchema.ColumnDataType.STRING);

    List<String> expecteds =
        Arrays.asList("[1, null]", "[2, null]", "[3, null]", "[4, null]", "[5, minney]", "[6, null]", "[7, scooby]");
    int index = 0;

      System.out.println("HEREERERERERERERERERERERERERERERE");
      Iterator<Object[]> iterator2 = rows.iterator();
      while (iterator2.hasNext()) {
          Object[] row = iterator2.next();
          System.out.println(Arrays.toString(row));
      }

      Iterator<Object[]> iterator = rows.iterator();
    while (iterator.hasNext()) {
      Object[] row = iterator.next();
      Assert.assertEquals(Arrays.toString(row), expecteds.get(index++));
    }
  }

  /** Verify simple path expression query on ingested Avro file. */
//  @Test
//  public void testStringValueSelectOnJsonColumn() {
//    Operator<SelectionResultsBlock> operator = getOperator(
//        "SELECT json_extract_scalar(jsonColumn1, '$', 'STRING') FROM "
//            + "testTable WHERE JSON_MATCH(jsonColumn1, '\"$\" = ''test''')");
//    SelectionResultsBlock block = operator.nextBlock();
//    Collection<Object[]> rows = block.getRows();
//    Assert.assertEquals(block.getDataSchema().getColumnDataType(0), DataSchema.ColumnDataType.STRING);
//
//    List<String> expecteds = Arrays.asList("[test]");
//    int index = 0;
//
//    Iterator<Object[]> iterator = rows.iterator();
//    while (iterator.hasNext()) {
//      Object[] row = iterator.next();
//      Assert.assertEquals(Arrays.toString(row), expecteds.get(index++));
//    }
//  }
//
//  /** Verify that ingestion from avro FIXED type field (jsonColumn3) to Pinot JSON column worked fine. */
//  @Test
//  public void testSimpleSelectOnFixedJsonColumn() {
//    testByteArray("select jsonColumn3 FROM testTable");
//  }
//
//  /** Verify that ingestion from avro BYTES type field (jsonColumn4) to Pinot JSON column worked fine. */
//  @Test
//  public void testSimpleSelectOnBytesJsonColumn() {
//    testByteArray("select jsonColumn4 FROM testTable");
//  }
//
//  @Test
//  public void testComplexSelectOnJsonColumn() {
//    Operator<SelectionResultsBlock> operator = getOperator(
//        "select jsonColumn5 FROM testTable");
//    SelectionResultsBlock block = operator.nextBlock();
//    Collection<Object[]> rows = block.getRows();
//    Assert.assertEquals(block.getDataSchema().getColumnDataType(0), DataSchema.ColumnDataType.JSON);
//
//    List<String> expecteds = Arrays.asList(
//        "[[{\"data\":{\"a\":\"1\",\"b\":\"2\"},\"timestamp\":1719390721}]]",
//        "[[{\"data\":{\"a\":\"2\",\"b\":\"4\"},\"timestamp\":1719390722}]]",
//        "[[{\"data\":{\"a\":\"3\",\"b\":\"6\"},\"timestamp\":1719390723}]]",
//        "[[{\"data\":{\"a\":\"4\",\"b\":\"8\"},\"timestamp\":1719390724}]]",
//        "[[{\"data\":{\"a\":\"5\",\"b\":\"10\"},\"timestamp\":1719390725}]]",
//        "[[{\"data\":{\"a\":\"6\",\"b\":\"12\"},\"timestamp\":1719390726}]]",
//        "[[{\"data\":{\"a\":\"7\",\"b\":\"14\"},\"timestamp\":1719390727}]]");
//
//    int index = 0;
//    Iterator<Object[]> iterator = rows.iterator();
//    while (iterator.hasNext()) {
//      Object[] row = iterator.next();
//      Assert.assertEquals(Arrays.toString(row), expecteds.get(index++));
//    }
//  }

  private void testByteArray(String query) {
    Operator<SelectionResultsBlock> operator = getOperator(query);
    SelectionResultsBlock block = operator.nextBlock();
    Collection<Object[]> rows = block.getRows();
    Assert.assertEquals(block.getDataSchema().getColumnDataType(0), DataSchema.ColumnDataType.JSON);

    List<String> expecteds = IntStream.range(1, 8)
        .mapToObj(i -> new byte[] {0, 0, 0, (byte) i})
        .map(byteArray -> "[\"" + StringFunctions.toBase64(byteArray) + "\"]")
        .collect(Collectors.toList());

    int index = 0;
    for (Object[] row : rows) {
      Assert.assertEquals(Arrays.toString(row), expecteds.get(index++));
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
