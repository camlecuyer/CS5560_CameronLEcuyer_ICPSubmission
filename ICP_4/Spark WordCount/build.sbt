name := "SparkTransformationActions"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.3.1",
  "org.apache.spark" %% "spark-mllib" % "2.3.1",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0" classifier "models",
  "com.google.protobuf" % "protobuf-java" % "3.6.0",
  "edu.stanford.nlp" % "stanford-parser" % "3.6.0",
  "org.apache.httpcomponents" % "httpcore" % "4.4.5",
  "org.apache.httpcomponents" % "httpclient" % "4.5.2",
  "com.googlecode.json-simple" % "json-simple" % "1.1.1",
  "com.github.scopt" % "scopt_2.10" % "3.4.0",
  "databricks/spark-corenlp:0.3.1-s_2.11"
)

