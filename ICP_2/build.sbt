name := "ICP_2"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "edu.stanford.nlp" % "stanford-corenlp" % "3.9.1",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.9.1" classifier "models",
  "com.google.protobuf" % "protobuf-java" % "3.9.1")