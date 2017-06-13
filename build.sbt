name := "complex-algorithms"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.0",
  "org.apache.spark" %% "spark-graphx" % "2.0.0",
  "org.apache.spark" %% "spark-sql" % "2.0.0",
  "org.apache.spark" %% "spark-streaming" % "2.0.0"
)

//unmanagedJars in Compile += file("lib/spark-measure_2.11-0.1-SNAPSHOT.jar")

//unmanagedResourceDirectories in Compile <++= baseDirectory {base => Seq(base / "resources/main/scala")}