package org.qingfei.movielover.preprocess

import org.junit.Assert._
import org.junit._

@Test
class PreprocessTest {

    @Test
    def testOK() = assertTrue(true)

    @Test
    def testCleanTag() =
        assertEquals(Preprocessor.cleanTag("402,260,\"space epic, science fiction, hero's journey\",1443393664")
          .toString,"402,260,space epic science fiction hero's journey,1443393664")

//    @Test
//    def testKO() = assertTrue(false)

}


