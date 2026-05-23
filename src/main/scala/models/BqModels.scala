package com.zoomin.earth.datalake.models

import com.zoomin.earth.datalake.db.{BqRow, BqSchema}

case class BigQueryNostrAuthoredEvent(
  id: String,
  pubkey: String,
  created_at: Long,
  kind: Int,
  tags: List[List[String]],
  content: String,
  sig: String,
  relay_url: String,
  processed_at: Long
) derives BqSchema,
    BqRow

case class BigQueryStalkingPubKey(
  pubKey: String,
  from: Option[Long],
  until: Option[Long],
  relay_url: String
) derives BqSchema,
    BqRow
