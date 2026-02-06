package com.zoomin.earth.datalake.models

enum Kind(val id: Int) {
  case KIND_PROFILE   extends Kind(0)
  case KIND_TEXT      extends Kind(1)
  case KIND_CONTACTS  extends Kind(3)
  case KIND_REPOST    extends Kind(6)
  case KIND_REACTION  extends Kind(7)
  case KIND_LONG_POST extends Kind(30023)
  case KIND_ZAP       extends Kind(9735)
}

object Kind {
  val ids = Kind.values.toList.map(_.id)
}
