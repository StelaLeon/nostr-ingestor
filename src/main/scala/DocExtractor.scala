package com.zoomin.earth.datalake.documentation

import scala.deriving.Mirror
import scala.compiletime.constValue
import scala.quoted.*

object DocExtractor:
  inline def getDocs[T]: List[(String, String)] = ${ getDocsImpl[T] }

  private def getDocsImpl[T: Type](using Quotes): Expr[List[(String, String)]] =
    import quotes.reflect.*
    val typeRepr = TypeRepr.of[T]
    val symbol   = typeRepr.typeSymbol

    val fields = if symbol.flags.is(Flags.Enum) then symbol.children else symbol.caseFields

    val docPairs = fields.map { f =>
      val name    = f.name
      val docTree = f.annotations.find(_.tpe.typeSymbol.name == "doc")
      val desc    = docTree match {
        case Some(Apply(_, List(Literal(StringConstant(str))))) => str
        case _                                                  => "No description provided."
      }
      Expr(name -> desc)
    }
    Expr.ofList(docPairs)

  inline def debugAnnotations[T]: String = ${ debugAnnotationsImpl[T] }

  private def debugAnnotationsImpl[T: Type](using Quotes): Expr[String] =
    import quotes.reflect.*
    val symbol = TypeRepr.of[T].typeSymbol
    val info = symbol.annotations.map { ann =>
      s"name=${ann.tpe.typeSymbol.name} full=${ann.tpe.typeSymbol.fullName} tree=${ann.show}"
    }.mkString("\n")
    Expr(info)

  inline def generateClassDocs[T]: String = ${ generateClassDocsImpl[T] }

  private def generateClassDocsImpl[T: Type](using Quotes): Expr[String] =
    import quotes.reflect.*
    val symbol = TypeRepr.of[T].typeSymbol

    def extractDoc(s: Symbol): Option[String] =
      s.annotations
        .find(_.tpe.typeSymbol.name == "doc")
        .collect { case Apply(_, List(Literal(StringConstant(str)))) => str.trim }

    val classDoc = extractDoc(symbol).getOrElse("No description provided.")

    val memberDocs = TypeRepr.of[T].baseClasses
      .flatMap(_.declarations)
      .filter(m => !m.isClassConstructor && extractDoc(m).isDefined)
      .distinctBy(_.name)
      .map(m => m.name -> extractDoc(m).get)

    val sb = new StringBuilder()
    sb.append(s"### ${symbol.name}\n")
    sb.append(s"$classDoc\n\n")
    memberDocs.foreach { case (name, doc) => sb.append(s" - **$name**: $doc\n") }
    sb.append("-" * 50 + "\n\n")
    Expr(sb.toString)

  inline def generateDocs[T](using m: Mirror.Of[T]): String =
    val typeName       = constValue[m.MirroredLabel].toString
    val fieldsWithDocs = getDocs[T]

    val sb = new StringBuilder()
    sb.append(s"### Type: $typeName\n")
    sb.append(s"Fields/Cases discovered at compile time:\n")
    fieldsWithDocs.foreach { case (name, description) =>
      sb.append(s" - **$name**: $description\n")
    }
    sb.append("-" * 50 + "\n\n")
    sb.toString
