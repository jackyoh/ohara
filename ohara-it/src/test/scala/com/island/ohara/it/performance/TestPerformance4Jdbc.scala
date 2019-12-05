package com.island.ohara.it.performance

import com.island.ohara.common.setting.ObjectKey
import com.island.ohara.it.category.PerformanceGroup
import org.junit.Test
import org.junit.experimental.categories.Category

@Category(Array(classOf[PerformanceGroup]))
class TestPerformance4Jdbc extends BasicTestPerformance {

  @Test
  def test(): Unit = {

  }

  override protected[performance] def sharedJars(): Set[ObjectKey] = Set.empty
}
