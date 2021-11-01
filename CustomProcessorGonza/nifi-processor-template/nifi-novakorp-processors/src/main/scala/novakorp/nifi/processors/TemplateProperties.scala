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

import org.apache.nifi.components.{PropertyDescriptor, ValidationContext, ValidationResult, Validator}
import org.apache.nifi.expression.ExpressionLanguageScope

import scala.util.Failure
import scala.util.matching.Regex

trait TemplateProperties {
  val PROPIEDAD1: PropertyDescriptor =
    new PropertyDescriptor.Builder()
      .name("nombre prop1")
      .description("descripcion prop1")
      .required(false)
      .addValidator(TemplateProperties.CODE_VALIDATOR) // se puede agregar un vlaidador para corroborar propiedad no este vacia etcetc
      .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES) // se puede especificar el scope del expession language
      .build

  val PROPIEDAD2: PropertyDescriptor =
    new PropertyDescriptor.Builder()
      .name("nombre prop2")
      .description("descripcion prop2")
      .required(false)
      .addValidator(TemplateProperties.CODE_VALIDATOR)
      .build

  val PROPIEDAD3: PropertyDescriptor =

    new PropertyDescriptor.Builder()
      .name("nombre prop3")
      .description("descripcion prop3")
      .required(false)
      .addValidator(TemplateProperties.CODE_VALIDATOR)
      .build

  lazy val properties = List(PROPIEDAD1, PROPIEDAD2, PROPIEDAD3)
}

object TemplateProperties extends TemplateProperties {
  private val CODE_VALIDATOR = new Validator() {
    override def validate(subject: String, input: String, context: ValidationContext): ValidationResult = {
      new ValidationResult.Builder()
        .subject(subject)
        .input(input)
        .explanation(s"Compilation failed with exception!")
        .valid(true)
        .build()
    }
  }

}
