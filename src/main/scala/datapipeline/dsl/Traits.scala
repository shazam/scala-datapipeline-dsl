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

trait DataPipelineObject {
  val name: String

  lazy val id: String = name.map {
    case ' ' => '-'
    case c if DataPipelineObject.ValidIdCharacter(c) => c
    case _ => '_'
  }

  val objectType: String
}

object DataPipelineObject {
  val ValidIdCharacter: Set[Char] = Set('*', '.', '_', '-') ++ ('0' to '9') ++ ('a' to 'z') ++ ('A' to 'Z')
}

trait Retryable {
  val attemptTimeout: Option[FiniteDuration]
  val maximumRetries: Option[Int]
  val retryDelay: Option[FiniteDuration]
}

