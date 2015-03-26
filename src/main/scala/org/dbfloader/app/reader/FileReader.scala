package org.dbfloader.app.reader

import org.dbfloader.app.LoadUtl

case class SourceFile(entityName:String, fileName:String, tableName:String, codeBase: String)

object FileReader {

  def groupFilesByEntity(path:String, prefix:String, getEntityName: String => String, getCodeBase: String => String) = {

    def getOneTableName(entityName:String) =
      s"${prefix}$entityName".toUpperCase

    val folder = new java.io.File(path)

    val listNames: List[SourceFile] = {
      folder.listFiles
        .map(_.getName)
        .filter(_.toUpperCase.contains(".DBF"))
        .sorted
        .map((fileName: String) => {val entityName = getEntityName(fileName)
                                   SourceFile(entityName, fileName, getOneTableName(entityName),getCodeBase(fileName))})
        .toList
    }

    listNames.groupBy(_.entityName)
  }

  def getListEntity(path:String) = {
    val folder = new java.io.File(path)
    folder.listFiles.map(_.getName.replace(".DBF","").substring(3))
  }


}
