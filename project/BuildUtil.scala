import sbt.{CrossVersion, ModuleID}

/*
 * BuildUtil.scala
 *
 * Updated: Oct 7, 2014
 *
 * Copyright (c) 2014, CodeMettle
 */
object BuildUtil {

    implicit class ExcludeModId(val u: ModuleID) extends AnyVal {
        def excludeCross(group: String, art: String, scalaVersion: String) = {
            val suff = CrossVersion partialVersion scalaVersion match {
                case Some((2, 10)) => "2.10"
                case Some((2, 11)) => "2.11"
                case _ => sys.error("excludeCross needs updating")
            }

            u.exclude(group, s"${art}_$suff")
        }
    }

}
