package org.dbfloader.app.reader

import com.linuxense.javadbf.DBFReader

case class Field(name:String,typeField:String, index:Int)

object MetaDataReader {

  def createField(dbfReader: DBFReader, index: Int) = {
    val dbfField = dbfReader.getField(index)
    Field(dbfField.getName, dbfField.getDataType.toString, index)
  }

  def getFields(dbfReader: DBFReader): List[Field] =
    Range(0, dbfReader.getFieldCount).map(createField(dbfReader, _)).toList

}
