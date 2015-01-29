package org.dbfloader.app.reader

case class SourceFile(entityName:String, fileName:String, tableName:String, codeBase:String)

object FileReader {

  def getTableName(fileName:String, entityName:String) = {

    def getCodeBase(fileName:String) = fileName.substring(0,2).toInt

    def getAreaName(codeBase: Int) = codeBase match {
      case 2 => "urdob"
      case 18 => "urdol"
      case _ => throw new Exception(s"Unknown codeBase ${codeBase}")
    }

    s"LESK_${getAreaName(getCodeBase(fileName))}_${entityName}".toUpperCase
  }



  def groupFilesByEntity(path:String) = {

    def getOneTableName(fileName:String, entityName:String) =
      s"LESK_JUR_$entityName".toUpperCase

    def getEntityName(fileName:String) = fileName.replace(".DBF","").substring(3)

    val folder = new java.io.File(path)

    val listNames: List[SourceFile] = {
      folder.listFiles
        .map(_.getName)
        .sorted
        .map((fileName: String) => {val entityName = getEntityName(fileName)
                                   SourceFile(entityName, fileName, getOneTableName(fileName,entityName),fileName.substring(0,2))})
        .toList
//        .filter(_.entityName == "tu_sl")
//        .filter((s) => (s.entityName != "poch"))
//        .filter((s) => (s.entityName != "rmmsn") && (s.entityName != "poch"))
    }

    listNames.groupBy(_.entityName)
  }

  def getListEntity(path:String) = {
    val folder = new java.io.File(path)
    folder.listFiles.map(_.getName.replace(".DBF","").substring(3))
  }


}
