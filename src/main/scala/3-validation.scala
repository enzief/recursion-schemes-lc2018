package lc2018

import jto.validation._
import jto.validation.jsonast._
import matryoshka._
import matryoshka.data._
import org.scalacheck.Arbitrary
import scalaz.Scalaz._
import scalaz._

import scala.collection.immutable.ListMap
import scala.language.higherKinds

/**
  * Now that we have a Schema we will need to validate incoming data (JSON)
  * and output "validated" data or "errors" with what went wrong for the sources
  * to be able to fix their exports.
  *
  * For that we'll use the JTO Validation library but first we need to define what a "Data" is
  */
sealed trait GData[A]
final case class GStruct[A](fields: ListMap[String, A]) extends GData[A]
final case class GArray[A](element: Seq[A])             extends GData[A]
final case class GBoolean[A](value: Boolean)            extends GData[A]
final case class GDate[A](value: java.util.Date)        extends GData[A]
final case class GDouble[A](value: Double)              extends GData[A]
final case class GFloat[A](value: Float)                extends GData[A]
final case class GInteger[A](value: Int)                extends GData[A]
final case class GLong[A](value: Long)                  extends GData[A]
final case class GString[A](value: String)              extends GData[A]

object GData extends GDataInstances with DataWithSchemaGenerator

/**
  * This is where you'll be working your magic.
  * This code will need to go through every part of the Schema tree
  * and create a `Rule` for each value, field of struct or array.
  */
object SchemaRules {

  /**
    * Here we only define a simple type alias to simplify the code later on.
    */
  type JRule[A] = Rule[JValue, A]

  implicit val jruleA: Applicative[JRule] = new Applicative[JRule] {
    override def point[A](a: => A): JRule[A] = Rule.pure(a)

    override def ap[A, B](fa: => JRule[A])(f: => JRule[A => B]): JRule[B] = fa.ap(f)
  }

  /**
    * One important thing is that going through a struct
    * means going through its fields one-by-one and generate `Rules`
    * that will be translated to a `Rule` for the whole struct.
    *
    * The best way will be to `traverse` the fields (there is an Applicative instance for JRule)
    */
  def fromSchemaToRules[T](schema: T)(implicit T: Recursive.Aux[T, SchemaF]): JRule[Fix[GData]] =
    T.cata(schema)(schemaFToRule)

  // SchemaF[JRule[Fix[GData]]] => JRule[Fix[GData]]
  val schemaFToRule: Algebra[SchemaF, JRule[Fix[GData]]] = {
    case ArrayF(elem) => Rules.pickSeq(elem).map(elems => Fix(GArray(elems)))
    case BooleanF()   => Rules.booleanR.map(x => Fix(GBoolean(x)))
    case DateF()      => Rules.stringR.andThen(Rules.isoDateR).map(x => Fix(GDate(x)))
    case DoubleF()    => Rules.doubleR.map(x => Fix(GDouble(x)))
    case FloatF()     => Rules.floatR.map(x => Fix(GFloat(x)))
    case IntegerF()   => Rules.intR.map(x => Fix(GInteger(x)))
    case LongF()      => Rules.longR.map(x => Fix(GLong(x)))
    case StringF()    => Rules.stringR.map(x => Fix(GString(x)))
    case StructF(fields) =>
      fields.toList
        .traverse[JRule, (String, Fix[GData])] {
          case (name, validation) =>
            (Path \ name).read(_ => validation.map(name -> _))
        }
        .map(x => Fix(GStruct[Fix[GData]](ListMap(x: _*))))
  }
}

/**
  * We need to test that validation - of course specific unit tests can be done
  * but we're quite paranoid so let's "generate" abitrary schemas using ScalaCheck
  *
  * But then again - from a Schema we'll be able to generate Rules
  * But to validate those rules we'd need data.
  * So let's generate Data as well :
  * Data that will, of course, need to be compatible with the Schema itself.
  */
trait DataWithSchemaGenerator {

  import org.scalacheck.Gen

  import scala.collection.JavaConverters._

  // Goal : first generate a schema and then recurse on it to generate the appropriate data
  // Bonus : handle number of fields
  // Bonus : handle max depth to "finish somewhere"
  // And don't forget the master defining what to generate is the schema
  def genSchemaAndData[S, D](
      implicit
      S: Birecursive.Aux[S, SchemaF],
      D: Corecursive.Aux[D, GData]
  ): Gen[(S, D)] =
    for {
      s <- genSchemaF
      d <- S.cata(s)(schemaFToDataGen)
    } yield s -> d

  implicit val genA: Applicative[Gen] = new Applicative[Gen] {
    override def point[A](a: => A): Gen[A] = Gen.const(a)

    override def ap[A, B](fa: => Gen[A])(ff: => Gen[A => B]): Gen[B] =
      for {
        a <- fa
        f <- ff
      } yield f(a)
  }

  def schemaFToDataGen[D](implicit D: Corecursive.Aux[D, GData]): Algebra[SchemaF, Gen[D]] = {
    case ArrayF(elems) =>
      Gen.listOf(elems).map(lst => D.embed(GArray(lst)))

    case StructF(fields) =>
      fields.toList
        .traverse {
          case (k, v) => v.map(k -> _)
        }
        .map { flds =>
          D.embed(GStruct(ListMap(flds: _*)))
        }

    case BooleanF() =>
      Gen.oneOf(true, false).map(value => D.embed(GBoolean[D](value)))

    case DateF() =>
      Gen.choose(0, Long.MaxValue).map(value => D.embed(GDate[D](new java.util.Date(value))))

    case DoubleF() =>
      Gen.choose(Double.MinValue, Double.MaxValue).map(value => D.embed(GDouble[D](value)))

    case FloatF() =>
      Gen.choose(Float.MinValue, Float.MaxValue).map(value => D.embed(GFloat[D](value)))

    case IntegerF() =>
      Gen.choose(Int.MinValue, Int.MaxValue).map(value => D.embed(GInteger[D](value)))

    case LongF() =>
      Gen.choose(Long.MinValue, Long.MaxValue).map(value => D.embed(GLong[D](value)))

    case StringF() =>
      Gen.alphaNumStr.map(value => D.embed(GString[D](value)))
  }

  def genSchemaF[S](implicit S: Birecursive.Aux[S, SchemaF]): Gen[S] =
    for {
      depth   <- Gen.choose(1, 1)
      nbCol   <- Gen.choose(1, 1)
      columns <- Gen.listOfN(nbCol, genStructF(depth) |> named)
    } yield S.embed(StructF(ListMap(columns: _*)))

  def named[S](genS: Gen[S]): Gen[(String, S)] =
    for {
      name <- Gen.identifier
      s    <- genS
    } yield name -> s

  def genValueF[S](implicit S: Corecursive.Aux[S, SchemaF]): Gen[S] =
    Gen.oneOf(
      S.embed(BooleanF[S]()),
      S.embed(DateF[S]()),
      S.embed(DoubleF[S]()),
      S.embed(FloatF[S]()),
      S.embed(IntegerF[S]()),
      S.embed(LongF[S]()),
      S.embed(StringF[S]())
    )

  def genArrayF[S](maxDepth: Int)(implicit S: Corecursive.Aux[S, SchemaF]): Gen[S] =
    for {
      depth <- Gen.choose(1, maxDepth)
      elems <- genNonArrayF(maxDepth - depth)
    } yield S.embed(ArrayF(elems))

  def genNonArrayF[S](maxDepth: Int)(implicit S: Corecursive.Aux[S, SchemaF]): Gen[S] =
    if (maxDepth > 0) {
      Gen.oneOf[S](genValueF, genStructF(maxDepth))
    } else {
      genValueF
    }

  def genStructF[S](maxDepth: Int)(implicit S: Corecursive.Aux[S, SchemaF]): Gen[S] =
    for {
      depth    <- Gen.choose(1, maxDepth)
      nbFields <- Gen.choose(0, 3)
      fields   <- Gen.listOfN(nbFields, genColumnF(maxDepth - depth))
    } yield S.embed(StructF(ListMap(fields: _*)))

  def genColumnF[S](maxDepth: Int)(implicit S: Corecursive.Aux[S, SchemaF]): Gen[(String, S)] = {
    def genValue: Gen[S] =
      if (maxDepth > 0) {
        Gen.oneOf[S](genValueF, genStructF(maxDepth))
      } else {
        genValueF
      }
    for {
      name <- Gen.identifier
      v    <- genValue
    } yield name -> v
  }
}

trait GDataInstances {

  implicit val genericDataFTraverse: Traverse[GData] = new Traverse[GData] {

    override def traverseImpl[G[_], A, B](
        fa: GData[A]
    )(f: A => G[B])(implicit evidence$1: Applicative[G]): G[GData[B]] = fa match {
      case GArray(elems) =>
        Functor[G].map(elems.toList traverse f)(GArray.apply)

      case GStruct(fields) =>
        val (keys, values) = fields.unzip
        Functor[G].map(values.toList traverse f)(v => GStruct(ListMap((keys zip v).toSeq: _*)))

      case GString(value)  => Applicative[G].point(GString[B](value))
      case GLong(value)    => Applicative[G].point(GLong[B](value))
      case GInteger(value) => Applicative[G].point(GInteger[B](value))
      case GDouble(value)  => Applicative[G].point(GDouble[B](value))
      case GFloat(value)   => Applicative[G].point(GFloat[B](value))
      case GDate(value)    => Applicative[G].point(GDate[B](value))
      case GBoolean(value) => Applicative[G].point(GBoolean[B](value))
    }
  }
}
