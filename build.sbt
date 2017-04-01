name := "UmlsTagger"

version := "1.0"

scalaVersion := "2.12.1"

//libraryDependencies += groupID % artifactID % revision % configuration

libraryDependencies ++= Seq(
"org.apache.lucene" % "lucene-core" % "4.6.0",
"org.apache.lucene" % "lucene-queries" % "4.6.0",
"org.apache.lucene" % "lucene-analyzers-common" % "4.6.0",
"org.apache.lucene" % "lucene-queryparser" % "4.6.0",
"org.apache.solr" % "solr-solrj" % "4.6.0"
)

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
logBuffered in Test := false

mainClass in Compile := Some("Main")
