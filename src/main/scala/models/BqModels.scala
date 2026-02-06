package com.zoomin.earth.datalake.models

case class BigQueryNostrAuthoredEvent(
  id: String,
  pubkey: String,
  created_at: Long,
  kind: Int,
  tags: List[List[String]],
  content: String,
  sig: String,
  relay_url: String
)
