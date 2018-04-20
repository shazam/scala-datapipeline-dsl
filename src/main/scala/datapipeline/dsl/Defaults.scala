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

case class Defaults(failureAndRerunMode: FailureAndRerunMode = FailureAndRerunMode.None,
                    role: Option[String] = None,
                    resourceRole: Option[String] = None,
                    pipelineLogUri: Option[String] = None)

trait FailureAndRerunMode {
  val mode: String
}

object FailureAndRerunMode {

  object None extends FailureAndRerunMode {
    override val mode = "none"
  }

  object Cascade extends FailureAndRerunMode {
    override val mode = "cascade"
  }

}