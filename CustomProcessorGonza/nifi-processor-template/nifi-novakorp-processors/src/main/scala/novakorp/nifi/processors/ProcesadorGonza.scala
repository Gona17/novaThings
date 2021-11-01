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
import java.io._
import java.util.UUID
import scala.util.Try
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.{ AbstractProcessor, Relationship }
import org.apache.nifi.processor.{ ProcessContext, ProcessSession }
import org.apache.nifi.annotation.documentation.{ Tags, CapabilityDescription }
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.processor.util.StandardValidators
import org.apache.nifi.processor.io.{ InputStreamCallback, OutputStreamCallback }
import org.apache.nifi.expression.ExpressionLanguageScope

@Tags(Array("Novakorp", "custom", "perso", "scala"))
@CapabilityDescription("Directs the flowfile depending if it's even or odd")
class ProcesadorGonza extends AbstractProcessor with TemplateProperties with TemplateRelationships {

  import scala.collection.JavaConverters._

  override def getSupportedPropertyDescriptors: java.util.List[PropertyDescriptor] = {
    properties.asJava
  }

  override def getRelationships: java.util.Set[Relationship] = {
    this.relationships.asJava
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit = {

    val inFlowFile = session.get

    Option(inFlowFile) match {
      case Some(_) =>
        val in = session.read(inFlowFile)
        val cont = scala.io.Source.fromInputStream(in).mkString("") // asi leo el contenido del FF
        try {
          //aqui vive la logica del procesador
          val num: Int = cont.toInt
          val resto: Int = num % 2
          if (resto == 0) {
            in.close()
            session.transfer(inFlowFile, Rel_even)
          } else if (resto == 1) {
            in.close()
            session.transfer(inFlowFile, Rel_odd)
          }
          in.close()
        } catch {
          case e: NumberFormatException =>
            in.close()
            session.transfer(inFlowFile, Rel_not_number)
          case x: Exception =>
            in.close()
            session.transfer(inFlowFile, Rel_Failure)
        }
        session.commit() // asi transfiero un FF al siguiente procesador

      case None =>
    }
  }
}
