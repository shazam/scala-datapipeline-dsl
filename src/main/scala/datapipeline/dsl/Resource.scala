/*
 * Copyright 2018 Shazam Entertainment Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License
 */

package datapipeline.dsl

import scala.concurrent.duration.FiniteDuration

sealed trait Resource extends DataPipelineObject

case class EmrCluster(name: String,
                      enableDebugging: Option[Boolean] = None,
                      releaseLabel: Option[String] = None,
                      masterInstanceType: Option[String] = None,
                      coreInstanceCount: Option[Int] = None,
                      coreInstanceType: Option[String] = None,
                      coreInstanceBidPrice: Option[BigDecimal] = None,
                      useOnDemandOnLastAttempt: Option[Boolean] = None,
                      terminateAfter: Option[FiniteDuration] = None,
                      keyPair: Option[String] = None,
                      region: Option[String] = None,
                      applications: Seq[String] = Nil,
                      configuration: Option[EmrConfiguration] = None) extends Resource {

  override val objectType = "EmrCluster"
}

case class EmrConfiguration(name: String,
                            classification: String,
                            properties: Map[String, String]) {
  val id = name.replaceAllLiterally(" ", "-")
}

