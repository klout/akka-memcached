import com.typesafe.sbtscalariform.ScalariformPlugin._
import scalariform.formatter.preferences._

name := "akka-memcached"

version := "0.25"

scalaVersion := "2.9.1"

libraryDependencies ++= Seq(
 	"log4j" % "log4j" % "1.2.14",
	"jboss" % "jboss-serialization" % "1.0.3.GA",
	"trove" % "trove" % "1.0.2",
	"com.typesafe.akka" % "akka-actor" % "2.0.3",
  	"org.specs2" %% "specs2" % "1.12.1" % "test",
    "com.google.code.simple-spring-memcached" % "spymemcached" % "2.8.1",
    "junit" % "junit" % "4.10",
    "junitperf" % "junitperf" % "1.8",
    "com.novocode" % "junit-interface" % "0.8" % "test->default",
    "com.google.guava" % "guava" % "13.0"
)

resolvers ++= Seq(	"Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
					"snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
                    "releases"  at "http://oss.sonatype.org/content/repositories/releases")

organization := "com.klout"

publishTo := Some("kloutLibraryReleases" at "http://maven-repo:8081/artifactory/libs-release-local")

credentials := Credentials(Path.userHome / ".ivy2" / ".credentials") :: Nil

scalariformSettings ++ Seq(
    ScalariformKeys.preferences := FormattingPreferences().
    setPreference(AlignParameters, true).
    setPreference(IndentSpaces, 4).
    setPreference(AlignSingleLineCaseStatements, true).
    setPreference(PreserveDanglingCloseParenthesis, true).
    setPreference(PreserveSpaceBeforeArguments, true)
)
