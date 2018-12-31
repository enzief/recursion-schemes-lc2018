package lc2018

import org.apache.avro.{LogicalTypes, _}
import matryoshka._, implicits._, patterns.EnvT
import scala.collection.immutable.ListMap
import scalaz._, Scalaz._

import scala.language.higherKinds
import scala.collection.JavaConverters._

/**
  * There is a problem that makes writing SchemaF <-> Avro (co)algebras more difficult.
  *
  * As a matter of fact Avro mandates that, when building a Schema, all records (the Avro
  * equivalent to our StructF) are registered using a unique name.
  *
  * This is problematic to our algebra-based method because with the algebras we've seen so
  * far we only care about one "layer" at a time, so there is no way to know the names we've
  * already used for ther records we've registered so far.
  *
  * Fortunately, we have at least two solutions to that problem. But before going any further,
  * maybe you can take a few minutes to try and imagine how we can solve that problem in general,
  * even if you don't know how to implement your solution using recursion-schemes yet.
  */
trait SchemaToAvroAlgebras extends Labelling with UsingARegistry with AvroCoalgebra {}

/**
  * The first solution comes from the observation that our schemas are in fact trees. And trees have
  * this nice property that each node have a unique path that goes from the root to it. If we can use
  * that unique path as the names of our records, we're good to go. So this solution boils down to
  * labelling each "node" of a schema with its path, and then use that path to form the names we
  * use to register our records.
  */
trait Labelling {

  /**
    * So lets define out Path as being simply a list of strings. These strings will be the field names
    * we need to traverse from the root to get to a specific element of our schema.
    */
  type Path = List[String]

  /**
    * Here is the "special trick" of the current solution.
    *
    * EnvT is a kind of "glorified pair". Given a label type E and a (pattern)-functor F, it allows us
    * to label each "node" of a F[T] with a value of type E while retaining the original structure. In
    * other words, if F is a functor, then EnvT[E, F, ?] is a functor as well.
    */
  type Labelled[A] = EnvT[Path, SchemaF, A]

  /**
    * If we are to label each "node" of a schema with its own path, we obviously need to go from the root
    * down to the leaves, so we definitely want to write a coalgebra.
    * This one might look a bit scarry though, but fear not, it's not as complcated as it looks. Lets just
    * follow the types together.
    *
    * A Coalgebra[F, A] is just a function A => F[A]. So the coalgebra bellow is just a function
    *  (Path, SchemaF[T]) => Labelled[(Path, SchemaF[T])
    * Expanding the Labelled alias it becomes
    *  (Path, SchemaF[T]) => EnvT[Path, SchemaF, (Path, SchemaF[T])]
    *
    * Ok, maybe it still looks a bit scarry...
    *
    * Lets try to put it differently. Assume you will be given a "seed" consisting of a whole schema and an
    * initial path (that will start empty). Your job is to use that to produce an EnvT that will contain
    * the path of the node you just saw (the "root" of the schema that was in the seed), and the node itself
    * but modified such that its "content" is not just a "smaller schema" as it was initially, but a new "seed"
    * consisting of a (larger) path, and the said "smaller schema".
    */
  def labelNodesWithPath[T](implicit T: Recursive.Aux[T, SchemaF]): Coalgebra[Labelled, (Path, T)] = {
    case (path, t) =>
      t.project match {
        case StructF(fields) =>
          EnvT {
            path -> StructF(fields.map { case (k, v) => k -> (k :: path, v) })
          }

        case sch =>
          EnvT(path -> sch.map(path -> _))
      }
  }

  /**
    * Now the algebra (that we had no way to write before) becomes trivial. All we have to do is to use
    * the path labelling each "node" as the name we need when registering a new avro record.
    *
    * To extract the label (resp. node) of an EnvT you can use pattern-matching (EnvT contains only a pair
    * (label, node)), or you can use the `ask` and `lower` methods that return the label and node respectively.
    */
  def labelledToSchema: Algebra[Labelled, Schema] =
    (envT: Labelled[Schema]) =>
      envT.lower match {
        case StructF(fields) =>
          val path: List[String] = envT.ask
          fields
            .foldLeft(SchemaBuilder.record(path.mkString("a", ".", "z")).fields) {
              case (builder, (key, value)) =>
                builder.name(key).`type`(value).noDefault()
            }
            .endRecord()
        case ArrayF(element) => SchemaBuilder.array().items(element)
        case BooleanF()      => Schema.create(Schema.Type.BOOLEAN)
        case DateF()         => LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))
        case DoubleF()       => Schema.create(Schema.Type.DOUBLE)
        case FloatF()        => Schema.create(Schema.Type.FLOAT)
        case IntegerF()      => Schema.create(Schema.Type.INT)
        case LongF()         => Schema.create(Schema.Type.LONG)
        case StringF()       => Schema.create(Schema.Type.STRING)
      }

  def schemaFToAvro[T](schemaF: T)(implicit T: Recursive.Aux[T, SchemaF]): Schema =
    (List.empty[String], schemaF).hylo(labelledToSchema, labelNodesWithPath)
}

/**
  * That first solution was (relatively) simple but it is not completely satisfying.
  * We needed both an algebra and a coalgebra to got from our SchemaF to Avro's Schema, which forced us to
  * use hylo.
  *
  * Fortunately, every scheme (and the related algebra) come with a "monadic" version. In this version, we
  * have to "wrap" the result of our algebras inside our monad of choice. The scheme will then use this
  * monad's bind at each step. That has plenty of cool uses.
  *
  * We can for example "short-circuit" the traversal by using \/ or Option as our monad. Or in this very case
  * we can use the State monad to keep track of what records we've already created.
  *
  * A note though: in order to use monadic schemes, we need a Traverse instance for our pattern-functor.
  */
trait UsingARegistry {

  type Registry[A] = State[Map[Int, Schema], A]

  def fingerprint(fields: Map[String, Schema]): Int = fields.hashCode

  def useARegistry: AlgebraM[Registry, SchemaF, Schema] = {
    case StructF(fields) =>
      val idx: Int = fingerprint(fields)
      State { rgt: Map[Int, Schema] =>
        rgt
          .get(idx)
          .map(rgt -> _)
          .getOrElse {
            val record = fields
              .foldLeft(SchemaBuilder.record("r%x".format(idx)).fields) {
                case (builder, (key, value)) =>
                  builder.name(key).`type`(value).noDefault()
              }
              .endRecord()
            (rgt + (idx -> record)) -> record
          }
      }
    case ArrayF(element) =>
      SchemaBuilder.array().items(element) |> State.state
    case BooleanF() =>
      Schema.create(Schema.Type.BOOLEAN) |> State.state
    case DateF() =>
      LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)) |> State.state
    case DoubleF()  => Schema.create(Schema.Type.DOUBLE) |> State.state
    case FloatF()   => Schema.create(Schema.Type.FLOAT) |> State.state
    case IntegerF() => Schema.create(Schema.Type.INT) |> State.state
    case LongF()    => Schema.create(Schema.Type.LONG) |> State.state
    case StringF()  => Schema.create(Schema.Type.STRING) |> State.state
  }

  implicit def schemaFTraverse: Traverse[SchemaF] = new Traverse[SchemaF] {
    def traverseImpl[G[_], A, B](fa: SchemaF[A])(f: A => G[B])(implicit G: Applicative[G]): G[SchemaF[B]] =
      fa match {
        case StructF(fields) =>
          fields.toList
            .traverse {
              case (k, v) => f(v).map(k -> _)
            }
            .map { ls =>
              ListMap(ls: _*) |> StructF.apply
            }
        case ArrayF(elem) => f(elem).map(ArrayF.apply)
        case BooleanF()   => G.point(BooleanF())
        case DateF()      => G.point(DateF())
        case DoubleF()    => G.point(DoubleF())
        case FloatF()     => G.point(FloatF())
        case IntegerF()   => G.point(IntegerF())
        case LongF()      => G.point(LongF())
        case StringF()    => G.point(StringF())
      }
  }

  def toAvro[T](schemaF: T)(implicit T: Recursive.Aux[T, SchemaF]): Schema =
    schemaF.cataM(useARegistry).run(Map.empty)._2
}

trait AvroCoalgebra {

  /**
    * Of course we also need a coalgebra to go from Avro to SchemaF
    * Since there are some avro shcemas that we do not handle here,
    * we need a CoalgebraM, but we're not really interested in providing meaningful errors
    * here, so we can use Option as our monad.
    */
  def avroToSchemaF: CoalgebraM[Option, SchemaF, Schema] = { schema =>
    schema.getType match {
      case Schema.Type.RECORD =>
        schema.getFields.asScala.map(f => f.name -> f.schema) |> { ls =>
          StructF(ListMap(ls: _*)).some
        }
      case Schema.Type.ARRAY   => ArrayF(schema.getElementType).some
      case Schema.Type.BOOLEAN => BooleanF().some
      case Schema.Type.DOUBLE  => DoubleF().some
      case Schema.Type.FLOAT   => FloatF().some
      case Schema.Type.INT     => IntegerF().some
      case Schema.Type.LONG =>
        Option(schema.getLogicalType)
          .map[Option[SchemaF[Schema]]] {
            Option(_)
              .filter(_.getName == LogicalTypes.timestampMillis().getName)
              .as(DateF())
          }
          .getOrElse {
            LongF().some
          }
      case Schema.Type.STRING => StringF().some
      case _                  => None
    }
  }
}
