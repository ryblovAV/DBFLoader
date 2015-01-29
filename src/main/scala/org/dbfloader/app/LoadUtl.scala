package org.dbfloader.app

import java.io.FileInputStream

import com.linuxense.javadbf.DBFReader
import grizzled.slf4j.Logging
import org.dbfloader.app.db.{SQLBulder, JDBCUtl}
import org.dbfloader.app.reader._
import org.springframework.context.support.ClassPathXmlApplicationContext

import scala.annotation.tailrec

object LoadUtl extends Logging {

  val writeToDb = true

  val path: String = "//Users//a123//data"

  val ctx = new ClassPathXmlApplicationContext("application-context.xml")
  val jdbcUtl: JDBCUtl = ctx.getBean(classOf[JDBCUtl])


  def loadOneSourceFile(sourceFile: SourceFile, sqlForMaxId:String) = {

    info(s" load file:${sourceFile}")

    val inputStream = new FileInputStream(s"$path//${sourceFile.fileName}")

    val reader = new DBFReader(inputStream)
    reader.setCharactersetName("Cp866")

    //read field from file
    val fields =  MetaDataReader.getFields(reader)

    //add CODE_BASE field to end
    if (!jdbcUtl.existsTable(sourceFile.tableName)) {
      jdbcUtl.createTable(sourceFile.tableName,
                          fields:+ Field("CODE_BASE","varchar2(2)",fields.length,"Y"),
                          SQLBulder.generateSqlCreateTable)
    }

    val records = DataReader.getRecords(reader)
    val lRecords = Transformation.transform(records)

    val existsId: Boolean = jdbcUtl.existsId(sourceFile.tableName)
    if (existsId)
      Transformation.addIdToList(lRecords,jdbcUtl.getMaxId(sqlForMaxId))
    Transformation.addCodeBaseToList(lRecords,sourceFile.codeBase)

    jdbcUtl.loadToDB(lRecords,SQLBulder.generateSqlInsert(sourceFile.tableName, fields, existsId),sourceFile.tableName, sourceFile.codeBase)

    inputStream.close()
  }

  def loadOneEntity(entityName: String, sourceFiles: List[SourceFile]):Unit = {
    val sqlForMaxId = SQLBulder.generateSelectMax(sourceFiles.map(_.tableName).distinct)
    sourceFiles.foreach((s) => loadOneSourceFile(s,sqlForMaxId))
  }

  def loadAll(mapFiles:Map[String,List[SourceFile]]) = {

    info("start load ")
    info(s"files $mapFiles")

    mapFiles.foreach((t) => loadOneEntity(t._1,t._2))
  }

  def createCopyTable(entityName:String) = {
    val tableName = s"LESK_JUR_${entityName.toUpperCase}"
    val sourceTableName = s"LESK_URDOL_${entityName.toUpperCase}"

    val fields:List[Field] = jdbcUtl.getFields(sourceTableName)
    val fieldCodeBase: Field = Field("CODE_BASE", "VARCHAR2(2)", fields.length, "N")

    jdbcUtl.createTable(tableName,fields ++ List(fieldCodeBase),SQLBulder.generateSqlCreateTableWithTypes)

    val l = jdbcUtl.getIndex(sourceTableName)
    info(s"index from $sourceTableName: $l")
    l.foreach((ind) => jdbcUtl.createIndex(tableName,ind,jdbcUtl.getIndexColumns(ind.name),_.replace("URDOL","JUR")))
    jdbcUtl.createIndex(tableName,DBIndex(s"I_CB_$tableName",""),List("CODE_BASE"),(s)=>s)

    val sourceTables:List[(String,String)] = List(
      (s"LESK_URDOB_${entityName.toUpperCase}","02"),
      (s"LESK_URDOL_${entityName.toUpperCase}","18"))

    sourceTables.foreach((table) => table match {
                                     case (name,codeBase) => jdbcUtl.copyData(name,tableName,fields,codeBase)
                                    }
    )

  }

}
