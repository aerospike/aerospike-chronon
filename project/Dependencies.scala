import sbt._

object Dependencies {
  lazy val chrononVersion = "0.0.115"

  lazy val chrononApi       = "ai.chronon"   %% "api"                    % chrononVersion
  lazy val chrononOnline    = "ai.chronon"   %% "online"                 % chrononVersion
  lazy val aerospikeClient  = "com.aerospike" % "aerospike-client-jdk8"  % "10.4.0"
  // The client's async TLS path (NettyEventLoops) is built on the netty 4.2
  // IoEventLoopGroup API. Only the modules it needs — not netty-all (which
  // drags every codec + native transport). netty-handler pulls the rest
  // transitively (transport/codec/resolver/buffer/common).
  lazy val nettyVersion     = "4.2.4.Final"
  lazy val nettyModules     = Seq(
    "io.netty" % "netty-handler"   % nettyVersion,
    "io.netty" % "netty-transport" % nettyVersion)

  // Spark/Arrow drag netty 4.1 transitively (netty-all + per-module jars);
  // without an override the test/it classpaths mix 4.1 and 4.2 classes and
  // NettyEventLoops fails with "Unexpected EventLoopGroup". Force the whole
  // io.netty graph to the version we compile against.
  lazy val nettyOverrides   = Seq(
    "netty-all", "netty-codec", "netty-codec-base", "netty-codec-http",
    "netty-codec-http2", "netty-codec-socks", "netty-handler",
    "netty-handler-proxy", "netty-resolver", "netty-buffer", "netty-common",
    "netty-transport", "netty-transport-classes-epoll",
    "netty-transport-classes-kqueue", "netty-transport-native-epoll",
    "netty-transport-native-kqueue", "netty-transport-native-unix-common"
  ).map("io.netty" % _ % nettyVersion)
  lazy val scallop          = "org.rogach"   %% "scallop"                % "4.1.0"
  lazy val sparkSql         = "org.apache.spark" %% "spark-sql"          % "3.5.3"
  lazy val logback          = "ch.qos.logback" % "logback-classic"       % "1.2.13"
  lazy val slf4j            = "org.slf4j"      % "slf4j-api"             % "1.7.36"
  lazy val scalatest        = "org.scalatest" %% "scalatest"            % "3.2.17"

}
