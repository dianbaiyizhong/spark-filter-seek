package com.saprk.spark.filterseek;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.saprk.spark.filterseek.demo.SeekDemo;

public class SeekIntegrationTest {

  @Test
  public void twoPhaseReadReturnsExactlyTheFilteredRows() throws Exception {
    SeekDemo.DemoResult result = SeekDemo.run();
    System.out.println("ORC=" + result.orcOk + ", Parquet=" + result.parquetOk
        + ", Druid=" + result.druidOk);
    assertTrue("ORC two-phase read returned wrong rows", result.orcOk);
    assertTrue("Parquet two-phase read returned wrong rows", result.parquetOk);
    assertTrue("Druid seek.sql read returned wrong rows", result.druidOk);
  }
}
