package org.apache.activemq.artemis.tests.integration.journal;

import java.io.File;
import java.nio.ByteBuffer;

import org.apache.activemq.artemis.core.io.SequentialFile;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.aioffm.libaio.FfmAIOSequentialFileFactory;
import org.apache.activemq.artemis.tests.unit.core.journal.impl.SequentialFileFactoryTestBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FfmAIOSequentialFileFactoryTest  extends SequentialFileFactoryTestBase {
   @BeforeAll
   public static void hasAIO() {
      org.junit.jupiter.api.Assumptions.assumeTrue(FfmAIOSequentialFileFactory.isSupported(), "Test case needs AIO to run");
   }

   @Override
   protected SequentialFileFactory createFactory(String folder) {
      return new FfmAIOSequentialFileFactory(new File(folder), 10);
   }

   @Test
   public void canCreateFactoryWithMaxIOLessThenTwo() {
      FfmAIOSequentialFileFactory factory = new FfmAIOSequentialFileFactory(new File("ignore"), 1);
   }

   @Test
   public void testBuffer() throws Exception {
      SequentialFile file = factory.createSequentialFile("filtetmp.log");
      file.open();
      ByteBuffer buff = factory.newBuffer(10);
      assertEquals(factory.getAlignment(), buff.limit());
      file.close();
      factory.releaseBuffer(buff);
   }
}
