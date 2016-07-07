package org.dbfloader.app.reader

import java.io.FileInputStream
import java.util

import org.apache.poi.hssf.usermodel.{HSSFCell, HSSFRow, HSSFWorkbook}
import org.apache.poi.ss.usermodel.{Cell, Row}
import org.apache.poi.xssf.usermodel.XSSFWorkbook

object ExcellReader {

  def generateFields(l: List[String]) = {
    s"(${l.foldLeft("")((b, a) => if (b != "") s"$b,$a" else s"$b$a")})"
  }

  def generateInsert(tableName: String, l: List[String]) = {
    val name = generateFields(l)
    val values = l.map(a => s":$a")
    s"""
       |insert into $tableName
       |$name
       |values
       |$values
     """.stripMargin
  }

  def generateCreateTable(tableName: String, fieldNames: List[String], fieldTypes: List[String]) = {

    case class Field(name: String, dataType: String)

    val fields = fieldNames.zip(fieldTypes).map(a => Field(a._1, a._2)).foldLeft("")((b, a) => if (b != "") s"$b,${a.name} ${a.dataType}" else s"$b${a.name} ${a.dataType}")

    s"""
       |create table $tableName (
       |  $fields
       |)
     """.stripMargin
  }

  def generateFieldsNames(cellIterator: util.Iterator[Cell]): List[String] = {
    var l = List.empty[String]
    while (cellIterator.hasNext) {
      l = cellIterator.next().getStringCellValue :: l
    }
    l.reverse
  }

  def generateFieldsTypes(cellIterator: util.Iterator[Cell]): List[Int] = {
    var l = List.empty[Int]
    while (cellIterator.hasNext) {
      l = cellIterator.next().getCellType :: l
    }
    l.reverse
  }

  def generateFieldsTypesName(fieldTypes: List[Int]) = {
    fieldTypes.map {
      case 0 => "NUMBER"
      case _ => "VARCHAR2(1000)"
    }
  }

  def getFields(row: Row) = {
    generateFieldsNames(row.cellIterator)
  }

  def getFieldsTypes(row: Row) = {
    generateFieldsTypes(row.cellIterator)
  }


  def getValues(row: Row, types: List[Int]) = {

    def getCellValue(cell: Cell, dataType: Int) = dataType match {
      case 0 => cell.getNumericCellValue
      case 1 => cell.getStringCellValue
      case 2 => cell.getNumericCellValue
    }

    types.zipWithIndex.map(a => getCellValue(cell = row.getCell(a._2), dataType = a._1)).toArray
  }

  def read(path: String) = {

    val tableName = "cm_svod"

    val excelFileToRead = new FileInputStream(path)
    val wb = new XSSFWorkbook(excelFileToRead)

    val sheet = wb.getSheetAt(0)
    //    HSSFRow row;
    //    HSSFCell cell;

    val rowIterator: util.Iterator[Row] = sheet.rowIterator()

    val names = getFields(row = rowIterator.next())

    val firstRow = rowIterator.next()
    val types = getFieldsTypes(row = firstRow)
    val typesNames = generateFieldsTypesName(types)

    println("names = " + names)
    println("types" + types)
    println("typesNames" + typesNames)

    val insertSql = generateInsert(tableName = tableName, l = names)

    val createTableSql = generateCreateTable(tableName, names, typesNames)

    println("insertSql = " + insertSql)
    println("createTableSql = " + createTableSql)


//    val values = getValues(row = firstRow, types = types)
//    val listRow = List(values)
//
//    var i = 100
//
//    while ((rowIterator.hasNext) && (i < 10)) {
//
//      val row = rowIterator.next()
//
//      getValues(row = row, types = types) :: listRow
//
//      i += 1
//
//    }





  }

}
