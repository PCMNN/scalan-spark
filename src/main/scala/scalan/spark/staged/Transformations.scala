package scalan.spark.staged

import scalan.spark.SparkDslExp

/**
 * Created by afilippov on 6/9/15.
 */
trait Transformations { self: SparkDslExp =>
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
    case Def(mc @ MethodCall(Def(wrapper: ExpSPairRDDFunctionsImpl[_,_]), m, args, neverInvoke)) if !isValueAccessor(m) =>
      val resultElem = mc.selfType
      val wrapperIso = getIsoByElem(resultElem)
      wrapperIso match {
        case iso: Iso[base,ext] =>
          val eRes = iso.eFrom
          val newCall = unwrapMethodCall(mc, wrapper.wrappedValueOfBaseType, eRes)
          iso.to(newCall)
      }
    case Def(mc @ MethodCall(Def(wrapper: ExpSBroadcastImpl[_]), m, args, neverInvoke)) if !isValueAccessor(m) =>
      val resultElem = mc.selfType
      val wrapperIso = getIsoByElem(resultElem)
      wrapperIso match {
        case iso: Iso[base, ext] =>
          val eRes = iso.eFrom
          val newCall = unwrapMethodCall(mc, wrapper.wrappedValueOfBaseType, eRes)
          iso.to(newCall)
      }
    case Def(nobj @ NewObject(clazz, args, neverInvoke)) if hasViewArg(args) =>
      unwrapNewObj(clazz, args, neverInvoke, nobj.selfType)
    /*case Def(mc @ MethodCall(reciever,m, args, neverInvoke)) if (!isValueAccessor(m) && hasViewArg(args)) =>
      val newCall = mkMethodCall(reciever, m, unwrapSyms(args), true, mc.selfType)
      newCall*/
  })

  private case class FuseNode[A,B](source: Exp[SRDD[A]], func: Exp[A => B], el: Elem[B]) {
  }
  private object NeedFuse {
    def unapply[A](s: Exp[A]): Option[FuseNode[_,_]] = s match {
      case Def(SRDDMethods.map(source: Rep[SRDD[b]], func:Rep[Function1[_, c] @unchecked])) => source match {
        case Def(SRDDMethods.map(predSource: Rep[SRDD[a]], predFunc)) => {
          val f = func.asRep[b => c]
          val pf = predFunc.asRep[a => b]

          implicit val eC = f.elem.eRange
          implicit val eB = source.elem.eItem
          implicit val eA = predSource.elem.eItem

          val newFunc = fun[a,c] { in: Rep[a] => f(pf(in))}
          Some(FuseNode[a,c](predSource, newFunc, eC))
        }
        case _ => None
      }
      case Def(SRDDMethods.zip(source1: Rep[SRDD[a]], source2: Rep[SRDD[b]])) => (source1, source2) match {
        case  (Def(SRDDMethods.map(predSource1: Rep[SRDD[ap]], func1)), Def(SRDDMethods.map(predSource2: Rep[SRDD[bp]], func2)) ) => {
          val f1 = func1.asRep[ap => a]
          val f2 = func2.asRep[bp => b]

          implicit val eB = source2.elem.eItem
          implicit val eA = source1.elem.eItem
          implicit val eAp = predSource1.elem.eItem
          implicit val eBp = predSource2.elem.eItem

          val newFunc = fun[(ap,bp),(a,b)]{ in: Rep[(ap,bp)] => (f1(in._1), f2(in._2))}
          val newSource = predSource1 zip predSource2
          Some(FuseNode[(ap,bp),(a,b)](newSource, newFunc, PairElem(eA,eB)))
        }
        case  (Def(SRDDMethods.map(predSource1: Rep[SRDD[ap]], func)), _ ) => {
          val f = func.asRep[ap => a]
          implicit val eB = source2.elem.eItem
          implicit val eA = source1.elem.eItem
          implicit val eAp = predSource1.elem.eItem

          val newFunc = fun[(ap,b), (a,b)] { in: Rep[(ap,b)] => (f(in._1), in._2)}
          val newSource = predSource1 zip source2
          Some(FuseNode[(ap,b),(a,b)](newSource, newFunc, PairElem(eA,eB)))
        }
        case  (_ ,Def(SRDDMethods.map(predSource2: Rep[SRDD[bp]], func))) => {
          val f = func.asRep[bp => b]
          implicit val eB = source2.elem.eItem
          implicit val eA = source1.elem.eItem
          implicit val eBp = predSource2.elem.eItem

          val newFunc = fun[(a,bp), (a,b)] { in: Rep[(a,bp)] => (in._1, f(in._2))}
          val newSource = source1 zip predSource2
          Some(FuseNode[(a,bp),(a,b)](newSource, newFunc, PairElem(eA,eB)))
        }

        case _ => None
      }
      case _ => None
    }
  }
  val fusionRewriter = new PartialRewriter({
    case NeedFuse(fNode: FuseNode[a,b]) => {
      implicit val e = fNode.el
      val fused = fNode.source.map(fNode.func)
      fused //mirrorSubst(fused, rw, t)
    }
  })
  private def mirrorSubst(s1: Rep[_], rw: Rewriter, t: MapTransformer) = {
    val g1 = new PGraph(s1).transform(DefaultMirror, rw, t)
    (g1.mapping, g1.roots.head)
  }

  class RDDCacheAndFusionMirror(graph: ProgramGraph[MapTransformer]) extends Mirror[MapTransformer] {
    val manyUsages: Set[Exp[_]] = graph.scheduleAll.filter { tp => graph.hasManyUsagesGlobal(tp.sym) }.map{ _.sym}.toSet

    private object NeedCache {
      def unapply[A](s: Exp[A]): Option[Exp[SRDD[_]]] = manyUsages.contains(s) match {
        case true => s match {
          case Def(SRDDMethods.map(_,_)) => Some(s.asInstanceOf[Exp[SRDD[_]]])
          case _ => None
        }
        case false => None
      }
    }
    override def apply[A](t: MapTransformer, rw: Rewriter, s: Exp[A], node: Def[A]) = {
      (s) match {
        case NeedCache(f: Exp[SRDD[a]]) =>
          implicit val e = s.elem
          val cached = t(f).cache
          mirrorSubst(cached, rw, t) //(t + (s -> cached), cached)
        /*case NeedFuse(fNode: FuseNode[a,b]) =>
          implicit val e = fNode.el
          val fused = fNode.source.map(fNode.func)
          mirrorSubst(fused, rw, t)*/
        case _ =>
          super.apply(t, rw, s,node)
      }
    }
  }
}
