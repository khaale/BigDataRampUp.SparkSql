name := "BigDataRampUp.FbAttendance"

version := "1.0"

scalaVersion := "2.10.6"

assemblyJarName in assembly := "apps.jar"

test in assembly := {}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1" % Provided,
  "org.apache.spark" %% "spark-sql" % "1.6.1" % Provided,

  "com.restfb" % "restfb" % "1.23.0",
  "com.databricks" %% "spark-csv" % "1.3.0",
  "com.typesafe" % "config" % "1.3.0",

  "org.scalatest" %% "scalatest" % "2.2.4" % Test,
  "com.holdenkarau" %% "spark-testing-base" % "1.6.1_0.3.3" % Test
)

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
runMain in Compile <<= Defaults.runMainTask(fullClasspath in Compile, runner in (Compile, run))
