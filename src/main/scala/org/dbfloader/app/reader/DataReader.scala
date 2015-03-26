package org.dbfloader.app.reader

import com.linuxense.javadbf.DBFReader
import grizzled.slf4j.Logging

import scala.annotation.tailrec

object DataReader extends Logging{

  @tailrec
  def getRecords(dbfReader: DBFReader,records:List[Array[Object]] = Nil):List[Array[Object]] = {
    val record = dbfReader.nextRecord()
    if (record != null) getRecords(dbfReader,record::records)
    else {
      info(s"read from file ${records.length} record")
      records}
  }
}
