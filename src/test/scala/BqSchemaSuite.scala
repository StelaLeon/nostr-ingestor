package com.zoomin.earth.datalake.db

import com.google.cloud.bigquery.{Field, StandardSQLTypeName}
import com.zoomin.earth.datalake.models.{BigQueryNostrAuthoredEvent, BigQueryStalkingPubKey}
import munit.FunSuite

class BqSchemaSuite extends FunSuite {

  test("BigQueryStalkingPubKey schema has correct number of fields") {
    val schema = BqSchema[BigQueryStalkingPubKey].schema
    assertEquals(schema.getFields.size, 4)
  }

  test("BigQueryStalkingPubKey fields have correct names") {
    val fields = BqSchema[BigQueryStalkingPubKey].schema.getFields
    assertEquals(fields.get(0).getName, "pubKey")
    assertEquals(fields.get(1).getName, "from")
    assertEquals(fields.get(2).getName, "until")
    assertEquals(fields.get(3).getName, "relay_url")
  }

  test("BigQueryStalkingPubKey fields have correct types") {
    val fields = BqSchema[BigQueryStalkingPubKey].schema.getFields
    assertEquals(fields.get(0).getType.getStandardType, StandardSQLTypeName.STRING)
    assertEquals(fields.get(1).getType.getStandardType, StandardSQLTypeName.INT64)
    assertEquals(fields.get(2).getType.getStandardType, StandardSQLTypeName.INT64)
    assertEquals(fields.get(3).getType.getStandardType, StandardSQLTypeName.STRING)
  }

  test("BigQueryStalkingPubKey optional fields are NULLABLE") {
    val fields = BqSchema[BigQueryStalkingPubKey].schema.getFields
    assertEquals(fields.get(1).getMode, Field.Mode.NULLABLE) // from
    assertEquals(fields.get(2).getMode, Field.Mode.NULLABLE) // until
  }

  test("BigQueryStalkingPubKey required fields are REQUIRED") {
    val fields = BqSchema[BigQueryStalkingPubKey].schema.getFields
    assertEquals(fields.get(0).getMode, Field.Mode.REQUIRED) // pubKey
    assertEquals(fields.get(3).getMode, Field.Mode.REQUIRED) // relay_url
  }

  test("BigQueryNostrAuthoredEvent schema has correct number of fields") {
    val schema = BqSchema[BigQueryNostrAuthoredEvent].schema
    assertEquals(schema.getFields.size, 9)
  }

  test("BigQueryNostrAuthoredEvent tags field is REPEATED STRING") {
    val fields = BqSchema[BigQueryNostrAuthoredEvent].schema.getFields
    val tags   = fields.get(4)
    assertEquals(tags.getName, "tags")
    assertEquals(tags.getType.getStandardType, StandardSQLTypeName.STRING)
    assertEquals(tags.getMode, Field.Mode.REPEATED)
  }

  test("BigQueryNostrAuthoredEvent all required fields are REQUIRED") {
    val fields      = BqSchema[BigQueryNostrAuthoredEvent].schema.getFields
    val allRequired = (0 until fields.size)
      .map(fields.get)
      .filter(_.getName != "tags")
      .forall(_.getMode == Field.Mode.REQUIRED)
    assert(allRequired)
  }
}
