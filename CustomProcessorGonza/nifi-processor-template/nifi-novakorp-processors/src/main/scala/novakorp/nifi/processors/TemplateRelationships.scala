/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package novakorp.nifi.processors

// NiFi
import org.apache.nifi.processor.Relationship

trait TemplateRelationships {

  val Rel_Success: Relationship =
    new Relationship.Builder()
      .name("Success")
      .description("""
        success
      """.trim)
      .build

  val Rel_Failure: Relationship =
    new Relationship.Builder()
      .name("Failure")
      .description("""
          Failure
      """.trim)
      .build

  val Rel_even: Relationship =
    new Relationship.Builder()
      .name("even")
      .description("""
          A Flowfile is routed to this relationship if it's even
      """.trim)
      .build

  val Rel_odd: Relationship =
    new Relationship.Builder()
      .name("odd")
      .description("""
          A Flowfile is routed to this relationship if it's odd
      """.trim)
      .build

  val Rel_not_number: Relationship =
    new Relationship.Builder()
      .name("not a number")
      .description("""
          A Flowfile is routed to this relationship if it's not a number
      """.trim)
      .build

  lazy val relationships = Set(Rel_Failure, Rel_even, Rel_odd, Rel_not_number)
}

object TemplateRelationships extends TemplateRelationships
