package org.dbfloader.app

import org.dbfloader.app.reader.FileUtl

object MainApp extends App {

  val files = FileUtl.groupFilesByEntity(LoadUtl.path)
  LoadUtl.loadAll(files)

  println("OK")

}
