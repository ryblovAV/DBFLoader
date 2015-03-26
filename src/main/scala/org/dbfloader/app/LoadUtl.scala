package org.dbfloader.app

import java.io.FileInputStream

import com.linuxense.javadbf.DBFReader
import grizzled.slf4j.Logging
import org.dbfloader.app.db.{JDBCUtl, SQLBulder}
import org.dbfloader.app.reader._
import org.springframework.context.support.ClassPathXmlApplicationContext

object LoadUtl extends Logging {

  val writeToDb = true

  val path = "//Users//a123//data"


  val ctx = new ClassPathXmlApplicationContext("application-context.xml")
  val jdbcUtl: JDBCUtl = ctx.getBean(classOf[JDBCUtl])

  def transfrom(sourceFile: SourceFile, records: List[Array[Object]], existsId: Boolean, maxId: Int): java.util.List[Array[Object]] = {
    info(s"start transformation ${records.length}")

    val lRecords = Transformation.transform(records)

    if (existsId)
      Transformation.addIdToList(lRecords, maxId)

    Transformation.addCodeBaseToList(lRecords, sourceFile.codeBase)

    lRecords
  }

  def loadRecords(sourceFile: SourceFile, fields: List[Field], existsId: Boolean, records: List[Array[Object]]) = {

    info(s"start load to db")


    info(s"end load to db")

  }

  def loadOneSourceFile(sourceFile: SourceFile, sqlForMaxId: String) = {

    info(s" load file:${sourceFile}")

    val inputStream = new FileInputStream(s"$path//${sourceFile.fileName}")

    val reader = new DBFReader(inputStream)
    reader.setCharactersetName("Cp866")

    //read field from file
    val fields = MetaDataReader.getFields(reader)
    //fields.foreach((f) => info(s"${f.name} ${f.typeField}"))

    //add CODE_BASE field to end
    if (!jdbcUtl.existsTable(sourceFile.tableName)) {
      jdbcUtl.createTable(sourceFile.tableName,
        fields :+ Field("CODE_BASE", "varchar2(2)", fields.length, "Y"),
        SQLBulder.generateSqlCreateTable)
    }

    info(s"start read from ${sourceFile.fileName}")
    val existsId = jdbcUtl.existsId(sourceFile.tableName)
    val maxId = if (existsId) jdbcUtl.getMaxId(sqlForMaxId) else 0
    val sqlInsert = SQLBulder.generateSqlInsert(sourceFile.tableName, fields, existsId)

    val records = DataReader.getRecords(reader).grouped(10000).
      map((records) => transfrom(sourceFile,records,existsId,maxId)).
      foldLeft(true)((first,records) => jdbcUtl.loadToDB(records, sqlInsert, sourceFile.tableName, sourceFile.codeBase, first))

      inputStream.close()


  }

  def loadOneEntity(entityName: String, sourceFiles: List[SourceFile]): Unit = {
    val sqlForMaxId = SQLBulder.generateSelectMax(sourceFiles.map(_.tableName).distinct)
    sourceFiles.foreach((s) => loadOneSourceFile(s, sqlForMaxId))
  }

  def loadAll(mapFiles: Map[String, List[SourceFile]]) = {
    //    info("start load ")
    //    info(s"files $mapFiles")
    mapFiles.foreach((t) => loadOneEntity(t._1, t._2))
  }

  def createCopyTable(entityName: String) = {
    val tableName = s"LESK_JUR_${entityName.toUpperCase}"
    val sourceTableName = s"LESK_URDOL_${entityName.toUpperCase}"

    val fields: List[Field] = jdbcUtl.getFields(sourceTableName)
    val fieldCodeBase: Field = Field("CODE_BASE", "VARCHAR2(2)", fields.length, "N")

    jdbcUtl.createTable(tableName, fields ++ List(fieldCodeBase), SQLBulder.generateSqlCreateTableWithTypes)

    val l = jdbcUtl.getIndex(sourceTableName)
    info(s"index from $sourceTableName: $l")
    l.foreach((ind) => jdbcUtl.createIndex(tableName, ind, jdbcUtl.getIndexColumns(ind.name), _.replace("URDOL", "JUR")))
    jdbcUtl.createIndex(tableName, DBIndex(s"I_CB_$tableName", ""), List("CODE_BASE"), (s) => s)

    val sourceTables: List[(String, String)] = List(
      (s"LESK_URDOB_${entityName.toUpperCase}", "02"),
      (s"LESK_URDOL_${entityName.toUpperCase}", "18"))

    sourceTables.foreach((table) => table match {
      case (name, codeBase) => jdbcUtl.copyData(name, tableName, fields, codeBase)
    }
    )

  }

}
