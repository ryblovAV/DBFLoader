package org.dbfloader.app

import java.io.FileInputStream

import com.linuxense.javadbf.DBFReader
import grizzled.slf4j.Logging
import org.dbfloader.app.db.{SQLBulder, JDBCUtl}
import org.dbfloader.app.reader.{SourceFile, DataReader, MetaDataReader}
import org.springframework.context.support.ClassPathXmlApplicationContext

import scala.annotation.tailrec

object LoadUtl extends Logging {

  val writeToDb = true

  val path: String = "//Users//a123//data"

  val ctx = new ClassPathXmlApplicationContext("application-context.xml")
  val jdbcUtl: JDBCUtl = ctx.getBean(classOf[JDBCUtl])


  def loadOneSourceFile(sourceFile: SourceFile, countRecord:Int):Int = {

    info(s" load file:${sourceFile}")

    val inputStream = new FileInputStream(s"$path//${sourceFile.fileName}")

    val reader = new DBFReader(inputStream)
    reader.setCharactersetName("Cp866")

    val fields = MetaDataReader.getFields(reader)

    if (!jdbcUtl.existsTable(sourceFile.tableName)) {
      jdbcUtl.createTable(sourceFile.tableName,fields)
    }


    val records = DataReader.getRecords(reader)
    val lRecords = Transformation.transform(records)

    val existsId: Boolean = jdbcUtl.existsId(sourceFile.tableName)
    if (existsId)
      Transformation.addIdToList(lRecords,countRecord)

    jdbcUtl.loadToDB(lRecords,SQLBulder.generateSqlInsert(sourceFile.tableName, fields, existsId),sourceFile.tableName)

    inputStream.close()

    records.length
  }
  @tailrec
  def loadOneEntity(entityName: String, sourceFiles: List[SourceFile], prevCountRecord:Int = 0):Unit = {
    sourceFiles match {
      case Nil =>
      case h :: t => loadOneEntity(entityName, t, loadOneSourceFile(h, prevCountRecord))
    }
  }

  def loadAll(mapFiles:Map[String,List[SourceFile]]) = {

    info("start load ")
    info(s"files $mapFiles")

    mapFiles.foreach((t) => loadOneEntity(t._1,t._2))
  }

}
