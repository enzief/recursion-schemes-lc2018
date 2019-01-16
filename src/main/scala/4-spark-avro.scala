package lc2018

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import matryoshka.patterns.EnvT
import scalaz._, Scalaz._

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericContainer, GenericData, GenericRecordBuilder}
import org.apache.spark.sql.Row

import scala.collection.immutable.ListMap
import scala.language.higherKinds

/**
  * It's time to confront ourselves to the real world of manipulating data with Spark & Avro
  * Two specific pain points we have to tackle are :
  *
  * - Spark's org.apache.spark.sql.Row is basically a wrapper of Array[Any]
  * but we need to handle two specifically different behaviour according to the level of the data :
  * When we're handling Arrays and Structs, no worry we need to output a Row
  * but when we're handling "simple" types, then if it's a top-level value we need to output a Row
  * but if it's not, then the value itself must be written.
  *
  * Exemple :
  *   - Value("b") will be Row("b")
  * but
  *   - Struct(a -> Value("b")) will be Row("b") as well (the Row now representing the outer struct)
  *
  * - For Apache Avro, it's a new kind of pain you'll need to overcome, Avro basically represents all of its
  * data as if, it will be at one point or another generated into Java classes.
  * So every "record" or Struct needs to have a qualified name "unique" otherwise the Avro engine will consider
  * the struct as being the same class.
  * But as it will obviously have different fields - you'll most likely end up with an error.
  *
  * Happy hunting.
  */
object SparkConverter {

  def isOfSimpleType[D](data: GData[D]) =
    data match {
      case GStruct(_) | GArray(_) => true
      case _                      => false
    }

  /**
    * We have a proper way to overcome this problem. There is a `para` scheme that works a little bit like cata.
    * Using para, our algebra will "see" not only the result of its application to the level bellow but also
    * the structure of that level we just processed.
    *
    * To use para, we need a special kind of algebra : a GAlgebra. Given a functor F and a comonad W,
    * Galgebra[W, F, A] is simply a function F[W[A]] => A, so our carrier is simply wrapped in an additional
    * layer.
    *
    * For para's GAlgebra we use (T[F], ?) as our comonad, in other words, our carrier will be paired with the
    * "tree" we processed during the previous step.
    *
    * We will use that to know when we need to "unwrap" the value we had wrapped in a Row at the previous step
    * although we shouldn't have.
    */
  def gDataToRow[D](implicit D: Recursive.Aux[D, GData]): GAlgebra[(D, ?), GData, Row] = {
    case GBoolean(v) => Row(v)
    case GDate(v)    => Row(v)
    case GDouble(v)  => Row(v)
    case GFloat(v)   => Row(v)
    case GInteger(v) => Row(v)
    case GLong(v)    => Row(v)
    case GString(v)  => Row(v)
    case GArray(elem) =>
      val values: Seq[Any] =
        elem.map { case (d, x) => extractSimpleTypes(d, x) }
      Row(values)

    case GStruct(fields) =>
      val values: Seq[Any] =
        fields.map { case (_, (d, x)) => extractSimpleTypes(d, x) }.toSeq
      Row(values)
  }

  def extractSimpleTypes[D](d: D, x: Row)(implicit D: Recursive.Aux[D, GData]): Any =
    if (isOfSimpleType(D.project(d))) x else x.values.head

  def fromGDataToSparkRow(row: Fix[GData]): Row =
    row.para[Row](gDataToRow)
}

/**
  * We'll also need Avro to serialize streaming data into Kafka topics.
  *
  * This is just another kind of pain :). We will be using Avro's GenericContainer interface.
  * To build a GenericContainer you need an Avro schema, so we'll have to somehow "zip" the data
  * we want to serialize with its schema (this should remind you of something we already did).
  */
object AvroConverter extends SchemaToAvroAlgebras {

  import scala.collection.JavaConverters._

  /**
    * A generic data (of type [[GData]]) with each element
    * labelled with the corresponding `avro.Schema`.
    */
  type DataWithSchema[A] = EnvT[Schema, GData, A]

  object DataWithSchema {
    def apply[W[_]]: EnvW[W] = new EnvW[W]
    class EnvW[W[_]] {
      def apply[E, A](run: (E, W[A])): EnvT[E, W, A] = EnvT(run)
    }
  }

  /**
    * When we'll zip data and schema there may be times when those two don't mix
    * we need to handle that case - this is what an Incompatibility is.
    */
  case class Incompatibility[D](schema: Schema, data: D)

  /**
    * Avro API is not very typesafe, all values inside GenericRecord are treated as mere Objects.
    * They didn't defined a GenericContainer for storing simple values (like numbers, strings, etc).
    * So we need to define one, for there is no way *we* work on non-types like Any or AnyRef.
    */
  case class SimpleValue(value: Any) extends GenericContainer {
    override def getSchema: Schema = throw new NotImplementedError() // we won't use that anyway
  }

  /**
    * But this is for our convenience only, we still need to feed avro API methods with unwrapped
    * simple values, so don't forget to use this method whenever needed.
    */
  def unwrap(container: GenericContainer): Any =
    container match {
      case SimpleValue(value) => value
      case value              => value
    }

  def fromGDataToAvro[S, D](
      schema: S,
      data: D
  )(
      implicit
      S: Birecursive.Aux[S, SchemaF],
      D: Birecursive.Aux[D, GData]
  ): Incompatibility[D] \/ GenericContainer =
    (schema, data).hyloM[Incompatibility[D] \/ ?, DataWithSchema, GenericContainer](alg, zipWithSchemaAlg)

  /**
    * Converts (SchemaF, GData) to (Schema, GData) optionally
    */
  // (S, D) => Incompatibility[D] \/ DataWithSchema[(S, D)]
  def zipWithSchemaAlg[S, D](
      implicit
      S: Birecursive.Aux[S, SchemaF],
      D: Birecursive.Aux[D, GData]
  ): CoalgebraM[Incompatibility[D] \/ ?, DataWithSchema, (S, D)] = {
    case (s, d) =>
      val avro: Schema = schemaFToAvro(s)
      (S.project(s), D.project(d)) match {
        case (sF @ StructF(fieldsF), GStruct(fields)) =>
          val zip: GStruct[(S, D)] =
            GStruct(fields.map {
              case (name, fx) => name -> (fieldsF(name), fx)
            })
          DataWithSchema[GData](avro -> zip).right

        case (aF @ ArrayF(elemF), GArray(elems)) =>
          val zip = GArray(elems.map(elemF -> _))
          DataWithSchema[GData](avro -> zip).right

        case (vF @ StringF(), GString(v)) =>
          val zip = GString[(S, D)](v)
          DataWithSchema[GData](avro -> zip).right

        case (vF @ IntegerF(), GInteger(v)) =>
          val zip = GInteger[(S, D)](v)
          DataWithSchema[GData](avro -> zip).right

        case (vF @ LongF(), GLong(v)) =>
          val zip = GLong[(S, D)](v)
          DataWithSchema[GData](avro -> zip).right

        case (vF @ BooleanF(), GBoolean(v)) =>
          val zip = GBoolean[(S, D)](v)
          DataWithSchema[GData](avro -> zip).right

        case (vF @ FloatF(), GFloat(v)) =>
          val zip = GFloat[(S, D)](v)
          DataWithSchema[GData](avro -> zip).right

        case (vF @ DoubleF(), GDouble(v)) =>
          val zip = GDouble[(S, D)](v)
          DataWithSchema[GData](avro -> zip).right

        case (vF @ DateF(), GDate(v)) =>
          val zip = GDate[(S, D)](v)
          DataWithSchema[GData](avro -> zip).right

        case _ =>
          Incompatibility(avro, d).left
      }
  }

  // DataWithSchema[GenericContainer] => Incompatibility[D] \/ GenericContainer
  def alg[D]: AlgebraM[Incompatibility[D] \/ ?, DataWithSchema, GenericContainer] =
    dataWithSchemaToContainer.map(\/-(_))

  def dataWithSchemaToContainer: Algebra[DataWithSchema, GenericContainer] = {
    case EnvT((schema, gdata)) =>
      gdata match {
        case GBoolean(v) => SimpleValue(v)
        case GDate(v)    => SimpleValue(v)
        case GDouble(v)  => SimpleValue(v)
        case GFloat(v)   => SimpleValue(v)
        case GInteger(v) => SimpleValue(v)
        case GLong(v)    => SimpleValue(v)
        case GString(v)  => SimpleValue(v)

        case GArray(elem) =>
          new GenericData.Array(schema, elem.map(unwrap).asJavaCollection)

        case GStruct(fields) =>
          fields
            .foldLeft(new GenericRecordBuilder(schema)) {
              case (b, (name, data)) =>
                b.set(name, unwrap(data))
            }
            .build()
      }
  }
}
