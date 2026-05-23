package com.zoomin.earth.datalake.models

import com.zoomin.earth.datalake.annotations.doc

@doc("We are supporting this different kinds of Nostr events that we support, each with a unique integer ID")
enum Kind(val id: Int) {
  @doc("User profile information, such as name, picture, and about")
  case KIND_PROFILE extends Kind(0)
  @doc("Text-based messages")
  case KIND_TEXT extends Kind(1)
  @doc("Contacts of a user.")
  case KIND_CONTACTS extends Kind(3)
  @doc("Reposts of other events, used to share content from other users.")
  case KIND_REPOST extends Kind(6)
  @doc("Reactions to events, such as likes or dislikes.")
  case KIND_REACTION extends Kind(7)
  @doc("Long-form content, such as articles or blog posts.")
  case KIND_LONG_POST extends Kind(30023)
  @doc("Zaps, which represent payments or tips sent to users or content creators.")
  case KIND_ZAP extends Kind(9735)
}

object Kind {
  val ids = Kind.values.toList.map(_.id)
}
