package scalan.spark

import scala.reflect.ClassTag
import scalan._
import scalan.spark.collections.{RDDCollectionsDslExp, RDDCollectionsDslSeq, RDDCollectionsDsl}

trait SparkDsl extends ScalanCommunityDsl
with SparkContextsDsl
with SparkConfsDsl
with RDDsDsl
with PairRDDFunctionssDsl
with PartitionersDsl
with BroadcastsDsl
{ implicit def elementToClassTag[A](implicit e: Elem[A]): ClassTag[A] = e.classTag }

trait SparkDslSeq extends SparkDsl with ScalanCommunityDslSeq
with SparkContextsDslSeq
with SparkConfsDslSeq
with RDDsDslSeq
with PairRDDFunctionssDslSeq
with PartitionersDslSeq
with BroadcastsDslSeq

trait SparkDslExp extends SparkDsl with ScalanCommunityDslExp
with SparkContextsDslExp
with SparkConfsDslExp
with RDDsDslExp
with PairRDDFunctionssDslExp
with PartitionersDslExp
with BroadcastsDslExp {
  def hasViewArg(args: List[AnyRef]): Boolean = {
    var res = false
    args.map {
      case obj if !obj.isInstanceOf[Rep[_]] => obj
      case HasViews(s, iso) => {res = true; s}
      case s => s
    }
    res
  }
  val wrappersCleaner = new PartialRewriter({
    case Def(mc @ MethodCall(Def(wrapper: ExpSRDDImpl[_]), m, args, neverInvoke)) if !isValueAccessor(m) =>
      val resultElem = mc.selfType
      val wrapperIso = getIsoByElem(resultElem)
      wrapperIso match {
        case iso: Iso[base,ext] =>
          val eRes = iso.eFrom
          val newCall = unwrapMethodCall(mc, wrapper.wrappedValueOfBaseType, eRes)
          iso.to(newCall)
      }
    case Def(mc @ NewObject(clazz, args, neverInvoke)) if hasViewArg(args) =>
      unwrapNewObj(clazz, args, neverInvoke, mc.selfType)

  })
}
