package org.dbfloader.app.reader

case class SourceFile(entityName:String, fileName:String, tableName:String, codeBase:String)

object FileReader {

  def groupFilesByEntity(path:String) = {

    def getOneTableName(fileName:String, entityName:String) =
      s"LESK_JUR_$entityName".toUpperCase

    def getEntityName(fileName:String) = fileName.replace(".DBF","").substring(3)

    val folder = new java.io.File(path)

    val listNames: List[SourceFile] = {
      folder.listFiles
        .map(_.getName)
        .filter(_.toUpperCase.contains(".DBF"))
        .sorted
        .map((fileName: String) => {val entityName = getEntityName(fileName)
                                   SourceFile(entityName, fileName, getOneTableName(fileName,entityName),fileName.substring(0,2))})
        .toList
    }

    listNames.groupBy(_.entityName)
  }

  def getListEntity(path:String) = {
    val folder = new java.io.File(path)
    folder.listFiles.map(_.getName.replace(".DBF","").substring(3))
  }


}
