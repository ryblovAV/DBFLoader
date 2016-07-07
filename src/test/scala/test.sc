def rtrim(s: String) = s.replaceAll("\\s+$", "")

val f = "       "
val s = rtrim(f)