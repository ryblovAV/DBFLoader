package org.dbfloader.app

import grizzled.slf4j.Logging
import org.dbfloader.app.reader.FileReader

object MainApp extends App with Logging {

  val files = FileReader.groupFilesByEntity(LoadUtl.path)
  LoadUtl.loadAll(files)

  logger.info("finish")
}
