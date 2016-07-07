package org.dbfloader.app.reader

import org.dbfloader.app.LoadUtl

import scala.util.Try

import java.io.{File => JFile}

case class SourceFile(entityName: String, fileName: String, tableName: String, codeBase: String, path: String)

object FileReader {

  def groupFilesByEntity(path: String,
                         prefix: String,
                         getEntityName: String => String,
                         getCodeBase: String => String) = {

    def getOneTableName(entityName: String) =
      s"${prefix}$entityName".toUpperCase

    val root = new java.io.File(path)

    def buildListFiles(folder: JFile):Seq[JFile] = {
      folder.listFiles.flatMap(f => if (f.isDirectory) buildListFiles(f) else List(f))
    }

    def listNames(folder: java.io.File): List[SourceFile] = {

      buildListFiles(folder)
        .map(_.getName)
        .filter(_.toUpperCase.contains(".DBF"))
        .sorted
        .map((fileName: String) => {
          val entityName = getEntityName(fileName)
          SourceFile(entityName, fileName, getOneTableName(entityName), getCodeBase(fileName), path)
        })
        .toList
    }

    listNames(root).groupBy(_.entityName)
  }

  def getListEntity(path: String) = {
    val folder = new java.io.File(path)
    folder.listFiles.map(_.getName.replace(".DBF", "").substring(3))
  }


}
