package oharastream.ohara.it.performance

import oharastream.ohara.agent.container.ContainerClient
import oharastream.ohara.client.configurator.v0.NodeApi.Node
import oharastream.ohara.common.util.CommonUtils
import oharastream.ohara.it.EnvTestingUtils

class TestPerformance4FtpSourceToHDFSSinkOnK8S extends TestPerformance4FtpSourceToHDFSSinkOnDocker {
  private[this] val k8sURL: Option[String] =
    sys.env.get("ohara.it.k8s").map(url => s"--k8s ${url}").orElse(Option.empty)

  override protected val nodes: Seq[Node]                 = EnvTestingUtils.k8sNodes()
  override protected val containerClient: ContainerClient = EnvTestingUtils.k8sClient()

  override protected def otherRoutes(): Map[String, String] =
    k8sURL
      .map { url =>
        val k8sMasterNodeName = url.split("http://").last.split(":").head
        Map(k8sMasterNodeName -> CommonUtils.address(k8sMasterNodeName))
      }
      .getOrElse(Map.empty)

  override protected def otherContainerCommand(): String = k8sURL.getOrElse("")
}
