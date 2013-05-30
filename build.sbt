import ScalaxbKeys._
import AssemblyKeys._

name := "als_debug"

version := "1.0"

organization := "edu.berkeley.cs.amplab"

scalaVersion := "2.9.2"

libraryDependencies ++= Seq(
//  "org.spark-project" % "spark-core_2.9.3" % "0.8.0-SNAPSHOT",
// "org.scalanlp" % "breeze-math_2.9.2" % "0.1"
  "org.spark-project" %% "spark-core" % "0.7.0",
  "org.scalanlp" %% "breeze-math" % "0.1"
)

resolvers ++= Seq(
  "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  "Scala Tools Snapshots" at "http://scala-tools.org/repo-snapshots/",
  "ScalaNLP Maven2" at "http://repo.scalanlp.org/repo",
  "Spray Repo" at "http://repo.spray.io"
)

seq(scalaxbSettings: _*)

packageName in scalaxb in Compile := "org.mlbase.library"

sourceGenerators in Compile <+= scalaxb in Compile

assemblySettings

test in assembly := {}

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>  
        { 	
		case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first  
		case PathList("org", "apache", "jasper", xs @ _*) => MergeStrategy.first  
		case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.first  
		case PathList("org", "objectweb", "asm", xs @ _*) => MergeStrategy.first  
		case PathList("spark", "rdd", xs @ _*) => MergeStrategy.first  
		case "about.html" => MergeStrategy.discard
		case "log4j.properties" => MergeStrategy.first
		case x => old(x)
	}
}

unmanagedClasspath in Runtime <+= (baseDirectory) map { bd => Attributed.blank(bd / "conf") }
