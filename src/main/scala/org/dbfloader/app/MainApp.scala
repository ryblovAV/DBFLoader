package org.dbfloader.app

import grizzled.slf4j.Logging
import org.dbfloader.app.reader.FileReader

object MainApp extends App with Logging {

  val files = FileReader.groupFilesByEntity(LoadUtl.path)//.filter((a)=>a._1 == "opla")
  LoadUtl.loadAll(files)


//  val entityList = FileReader.getListEntity(LoadUtl.path).filter(_ == "tuchD")
//  entityList.foreach(LoadUtl.createCopyTable(_))

  logger.info("finish")
}
