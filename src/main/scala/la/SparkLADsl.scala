package la

import scalan.{ScalanCommunityDslExp, ScalanCommunityDslSeq, ScalanCommunityDsl}
import scalan.spark.{SparkDslExp, SparkDslSeq, SparkDsl}
import scalan.spark.collections.{RDDCollectionsDslExp, RDDCollectionsDslSeq, RDDCollectionsDsl}

/**
 * Created by afilippov on 4/20/15.
 */

trait SparkLADsl extends ScalanCommunityDsl with SparkDsl with RDDCollectionsDsl
with SparkMatricesDsl

trait SparkLADslSeq extends SparkLADsl with ScalanCommunityDslSeq with SparkDslSeq with RDDCollectionsDslSeq
with SparkMatricesDslSeq

trait SparkLADslExp extends SparkLADsl with ScalanCommunityDslExp with SparkDslExp with RDDCollectionsDslExp
with SparkMatricesDslExp

