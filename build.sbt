scalaVersion := "3.4.0"
scalacOptions ++= Seq(
  "-Ykind-projector:underscores"
, "-Wvalue-discard"
, "-Wunused:implicits"
, "-Wunused:explicits"
, "-Wunused:imports"
, "-Wunused:locals"
, "-Wunused:params"
, "-Wunused:privates"
, "-Xfatal-warnings"
, "-source:future"
)

libraryDependencies += "org.typelevel" %% "cats-core" % "2.10.0"
libraryDependencies += "org.typelevel" %% "cats-effect" % "3.5.4"