name := """play-scala-forms-example"""

version := "2.6.x"

scalaVersion := "2.12.6"

crossScalaVersions := Seq("2.11.12", "2.12.4")

lazy val root = (project in file(".")).enablePlugins(PlayScala)

libraryDependencies += guice
libraryDependencies += "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.2" % Test

libraryDependencies += "org.apache.poi" % "poi" % "3.17"
libraryDependencies += "org.apache.poi" % "poi-ooxml" % "3.17"

libraryDependencies += "commons-io" % "commons-io" % "2.6"

libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.371"
