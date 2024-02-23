lazy val pekkoVersion = "1.0.2"
lazy val junitInterfaceVersion = "0.11"
lazy val logbackversion = "1.4.14"

ThisBuild / scalaVersion := "3.3.1"

lazy val root = project
  .in(file("."))
  .settings(
    name := "id2203-vt24-course-project-crdts",
    // Base dependencies
    libraryDependencies += "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion,
    libraryDependencies += "org.apache.pekko" %% "pekko-cluster-typed" % pekkoVersion,
    libraryDependencies += "com.novocode" % "junit-interface" % junitInterfaceVersion % "test",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % logbackversion,

    // Test dependencies
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.18" % Test,
    libraryDependencies += "org.apache.pekko" %% "pekko-actor-testkit-typed" % pekkoVersion % Test
  )