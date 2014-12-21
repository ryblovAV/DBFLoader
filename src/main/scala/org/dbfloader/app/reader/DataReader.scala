package org.dbfloader.app.reader

import com.linuxense.javadbf.DBFReader

import scala.annotation.tailrec

object DataReader {

  @tailrec
  def getRecords(dbfReader: DBFReader,records:List[Array[Object]] = Nil):List[Array[Object]] = {
    val record = dbfReader.nextRecord()
    if (record != null) getRecords(dbfReader,record::records)
    else records
  }
}
