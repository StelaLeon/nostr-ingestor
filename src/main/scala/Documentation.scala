package com.zoomin.earth.datalake.documentation

import com.zoomin.earth.datalake.models.Kind
import com.zoomin.earth.datalake.annotations.doc

import org.slf4j.LoggerFactory

import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets

@main def runDocGen(): Unit =
  val logger = LoggerFactory.getLogger("Documentation")

  val nostrKindsDocs = DocExtractor.generateDocs[Kind]
  val subscriptionUpdateStrategy = DocExtractor.generateClassDocs[com.zoomin.earth.datalake.datapipelines.orchestration.SubscriptionUpdateStrategy[_]]
  val timeWindowUpdateStrategy = DocExtractor.generateClassDocs[com.zoomin.earth.datalake.datapipelines.orchestration.TimeWindowUpdateStrategy[_]]
  
  val fullMarkdown =
    s"""# Internal Documentation
       |*Auto-generated from Scala 3 source code.*
       |
       |## Subscription Update Strategy
       |$subscriptionUpdateStrategy
       |
       |## Time Window Update Strategy
       |$timeWindowUpdateStrategy
       |
       |## Supported Nostr Kinds
       |$nostrKindsDocs
       |""".stripMargin

  val outputPath = Paths.get("target/docs/schema.md")

  Files.createDirectories(outputPath.getParent)
  Files.write(outputPath, fullMarkdown.getBytes(StandardCharsets.UTF_8))

  logger.info(s"SUCCESS: Physical documentation generated and saved to: ${outputPath.toAbsolutePath}")
