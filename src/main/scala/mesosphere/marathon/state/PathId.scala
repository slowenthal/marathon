package mesosphere.marathon.state

import scala.language.implicitConversions

case class PathId(path: List[String], absolute: Boolean = true) {

  def root: String = path.headOption.getOrElse("")

  def rootPath: PathId = PathId(path.headOption.map(_ :: Nil).getOrElse(Nil), absolute)

  def tail: List[String] = path.tail

  def isEmpty: Boolean = path.isEmpty

  def isRoot: Boolean = path.isEmpty

  def parent: PathId = if (tail.isEmpty) this else PathId(path.reverse.tail.reverse, absolute)

  def child: PathId = PathId(tail)

  def append(id: PathId): PathId = PathId(path ::: id.path, absolute)

  def restOf(parent: PathId): PathId = {
    def in(currentPath: List[String], parentPath: List[String]): List[String] = {
      if (currentPath.isEmpty) Nil
      else if (parentPath.isEmpty || currentPath.head != parentPath.head) currentPath
      else in(currentPath.tail, parentPath.tail)
    }
    PathId(in(path, parent.path), absolute)
  }

  def canonicalPath(base: PathId = PathId(Nil, absolute = true)): PathId = {
    require(base.absolute, "Base path is not absolute, canonical path can not be computed!")
    def in(remaining: List[String], result: List[String] = Nil): List[String] = remaining match {
      case head :: tail if head == "."  => in(tail, result)
      case head :: tail if head == ".." => in(tail, result.tail)
      case head :: tail                 => in(tail, head :: result)
      case Nil                          => result.reverse
    }
    if (absolute) PathId(in(path)) else PathId(in(base.path ::: path))
  }

  def safePath: String = toString("_")

  override def toString: String = toString("/")
  private def toString(delimiter: String): String = path.mkString(if (absolute) delimiter else "", delimiter, "")
}

object PathId {
  def apply(in: String): PathId = PathId(in.replaceAll("""(^/+)|(/+$)""", "").split("/").filter(_.nonEmpty).toList, in.startsWith("/"))
  def empty: PathId = PathId(Nil)

  implicit class StringPathId(val stringPath: String) extends AnyVal {
    def toPath: PathId = PathId(stringPath)
    def toRootPath: PathId = PathId(stringPath).canonicalPath()
  }
}

