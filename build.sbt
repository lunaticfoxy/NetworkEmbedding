name := "NetworkEmbedding"

version := "1.0"

scalaVersion := "2.12.17"

val sparkVersion = "3.2.2"

resolvers := Seq(
  "snapshots" at "https://repository.apache.org/snapshots/",
  "maven-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "releases" at "https://repository.apache.org/releases/",
  "cloudera" at "https://repository.cloudera.com/content/repositories/releases/",
  "spring" at "https://repo.spring.io/libs-milestone/"
)


val hadoopDependencies = Seq("client", "yarn-api", "yarn-common", "yarn-server-web-proxy", "yarn-client")

val sparkDependencies = Seq("core", "mllib", "hive", "yarn").map {
  dep => "org.apache.spark" %% s"spark-$dep" % sparkVersion excludeAll(hadoopDependencies.map{ dep => ExclusionRule("org.apache.hadoop", s"hadoop-$dep") }: _*)
}


libraryDependencies := sparkDependencies
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.1"