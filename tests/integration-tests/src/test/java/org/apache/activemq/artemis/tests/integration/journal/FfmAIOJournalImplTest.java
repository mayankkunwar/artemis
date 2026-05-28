package org.apache.activemq.artemis.tests.integration.journal;

import java.io.File;

import org.apache.activemq.artemis.ArtemisConstants;
import org.apache.activemq.artemis.core.io.SequentialFileFactory;
import org.apache.activemq.artemis.core.io.aioffm.libaio.FfmAIOSequentialFileFactory;
import org.apache.artemis.nativo.jlibaio.LibaioContext;
import org.apache.activemq.artemis.tests.unit.core.journal.impl.JournalImplTestUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.jupiter.api.Assertions.fail;

public class FfmAIOJournalImplTest  extends JournalImplTestUnit {
   @BeforeAll
   public static void hasAIO() {
      org.junit.jupiter.api.Assumptions.assumeTrue(FfmAIOSequentialFileFactory.isSupported(), "Test case needs AIO to run");
   }

   @Override
   @BeforeEach
   public void setUp() throws Exception {
      super.setUp();
      if (!LibaioContext.isLoaded()) {
         fail(String.format("libAIO is not loaded on %s %s %s", System.getProperty("os.name"), System.getProperty("os.arch"), System.getProperty("os.version")));
      }
   }

   @Override
   protected SequentialFileFactory getFileFactory() throws Exception {
      File file = new File(getTestDir());

      deleteDirectory(file);

      file.mkdir();

      // forcing the alignment to be 512, as this test was hard coded around this size.
      return new FfmAIOSequentialFileFactory(getTestDirfile(), ArtemisConstants.DEFAULT_JOURNAL_BUFFER_SIZE_AIO, 1000000, 10, false).setAlignment(512);
   }

   @Override
   protected int getAlignment() {
      return 512;
   }
}
