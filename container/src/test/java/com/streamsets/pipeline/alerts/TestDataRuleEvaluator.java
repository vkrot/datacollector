/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.alerts;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.EvictingQueue;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.config.DataRuleDefinition;
import com.streamsets.pipeline.config.ThresholdType;
import com.streamsets.pipeline.el.ELBasicSupport;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import com.streamsets.pipeline.el.ELStringSupport;
import com.streamsets.pipeline.metrics.MetricsConfigurator;
import com.streamsets.pipeline.runner.LaneResolver;
import com.streamsets.pipeline.util.Configuration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestDataRuleEvaluator {

  private static final String USER_PREFIX = "user.";
  private static MetricRegistry metrics;
  private static ELEvaluator elEvaluator;
  private static ELEvaluator.Variables variables;

  @BeforeClass
  public static void setUp() {
    metrics = new MetricRegistry();
    variables = new ELEvaluator.Variables();
    elEvaluator = new ELEvaluator();
    ELBasicSupport.registerBasicFunctions(elEvaluator);
    ELRecordSupport.registerRecordFunctions(elEvaluator);
    ELStringSupport.registerStringFunctions(elEvaluator);
  }

  @Test
  public void testAlertEnabledMeterEnabled() {
    String lane = "testAlertEnabledMeterEnabled";
    DataRuleDefinition dataRuleDefinition = new DataRuleDefinition("testAlertEnabledMeterEnabled",
      "testAlertEnabledMeterEnabled", lane, 100, 10, "${record:value(\"/name\")==null}", true,
      "testAlertEnabledMeterEnabled", ThresholdType.COUNT, "2", 5, true, false, true);
    DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator(metrics, variables, elEvaluator, null, null,
      dataRuleDefinition, new Configuration());
    dataRuleEvaluator.evaluateRule(TestUtil.createSnapshot(lane).get(LaneResolver.getPostFixedLaneForObserver(lane)),
      TestUtil.createSnapshot(lane).get(LaneResolver.getPostFixedLaneForObserver(lane)), lane, new HashMap<String,
        EvictingQueue<Record>>());
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(dataRuleDefinition.getId()));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long) 3, ((Map<String, Object>) gauge.getValue()).get("currentValue"));
    Assert.assertNotNull(((Map<String, Object>) gauge.getValue()).get("timestamp"));

    Meter meter = MetricsConfigurator.getMeter(metrics, USER_PREFIX + dataRuleDefinition.getId());
    Assert.assertNotNull(meter);
    Assert.assertEquals(3, meter.getCount());
  }

  @Test
  public void testAlertDisabledMeterEnabled() {
    String lane = "testAlertDisabledMeterEnabled";
    DataRuleDefinition dataRuleDefinition = new DataRuleDefinition("testAlertDisabledMeterEnabled",
      "testAlertDisabledMeterEnabled", lane,
      100, 10, "${record:value(\"/name\")==null}", false, null, ThresholdType.COUNT, "2", 5, true,
      false, true);
    DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator(metrics, variables, elEvaluator, null, null,
      dataRuleDefinition, new Configuration());
    dataRuleEvaluator.evaluateRule(TestUtil.createSnapshot(lane).get(LaneResolver.getPostFixedLaneForObserver(lane)),
      TestUtil.createSnapshot(lane).get(LaneResolver.getPostFixedLaneForObserver(lane)), lane, new HashMap<String,
        EvictingQueue<Record>>());
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(dataRuleDefinition.getId()));
    Assert.assertNull(gauge);

    Meter meter = MetricsConfigurator.getMeter(metrics, USER_PREFIX + dataRuleDefinition.getId());
    Assert.assertNotNull(meter);
    Assert.assertEquals(3, meter.getCount());
  }

  @Test
  public void testAlertEnabledMeterDisabled() {
    String lane = "testAlertEnabledMeterDisabled";
    DataRuleDefinition dataRuleDefinition = new DataRuleDefinition("testAlertEnabledMeterDisabled",
      "testAlertEnabledMeterDisabled", lane,
      100, 10, "${record:value(\"/name\")==null}", true, "testAlertEnabledMeterDisabled", ThresholdType.COUNT, "2",
      5, false, false, true);
    DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator(metrics, variables, elEvaluator, null, null,
      dataRuleDefinition, new Configuration());
    dataRuleEvaluator.evaluateRule(TestUtil.createSnapshot(lane).get(LaneResolver.getPostFixedLaneForObserver(lane)),
      TestUtil.createSnapshot(lane).get(LaneResolver.getPostFixedLaneForObserver(lane)), lane, new HashMap<String,
        EvictingQueue<Record>>());
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(dataRuleDefinition.getId()));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long) 3, ((Map<String, Object>) gauge.getValue()).get("currentValue"));
    Assert.assertNotNull(((Map<String, Object>) gauge.getValue()).get("timestamp"));

    Meter meter = MetricsConfigurator.getMeter(metrics, USER_PREFIX + dataRuleDefinition.getId());
    Assert.assertNull(meter);
  }

  @Test
  public void testAlertRaisedCountRuleDisabled() {
    String lane = "testAlertRaisedCountRuleDisabled";
    DataRuleDefinition dataRuleDefinition = new DataRuleDefinition("testAlertRaisedCountRuleDisabled",
      "testAlertRaisedCountRuleDisabled", lane,
      100, 10, "${record:value(\"/name\")==null}", true, "testAlertRaisedCountRuleDisabled", ThresholdType.COUNT, "2",
      5, true, false, false);
    DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator(metrics, variables, elEvaluator, null, null,
      dataRuleDefinition, new Configuration());
    dataRuleEvaluator.evaluateRule(TestUtil.createSnapshot(lane).get(LaneResolver.getPostFixedLaneForObserver(lane)),
      TestUtil.createSnapshot(lane).get(LaneResolver.getPostFixedLaneForObserver(lane)), lane, new HashMap<String,
        EvictingQueue<Record>>());
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(dataRuleDefinition.getId()));
    Assert.assertNull(gauge);
  }

  @Test
  public void testNoExceptionInvalidExpression() {
    String lane = "testNoExceptionInvalidExpression";
    //Missing "}"
    DataRuleDefinition dataRuleDefinition = new DataRuleDefinition("testNoExceptionInvalidExpression",
      "testNoExceptionInvalidExpression", lane,
      100, 10, "${record:value(\"/name\")==null", true, "testNoExceptionInvalidExpression", ThresholdType.COUNT, "2",
      5, true, false, true);
    DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator(metrics, variables, elEvaluator, null, null,
      dataRuleDefinition, new Configuration());
    dataRuleEvaluator.evaluateRule(TestUtil.createSnapshot(lane).get(LaneResolver.getPostFixedLaneForObserver(lane)),
      TestUtil.createSnapshot(lane).get(LaneResolver.getPostFixedLaneForObserver(lane)), lane, new HashMap<String,
        EvictingQueue<Record>>());
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(dataRuleDefinition.getId()));
    Assert.assertNull(gauge);
  }

  @Test
  public void testNoAlertRaisedCount() {
    String lane = "testNoAlertRaisedCount";
    //Threshold value is set to 3 but the generated snapshot meets criteria 2. Therefore no alerts
    DataRuleDefinition dataRuleDefinition = new DataRuleDefinition("testNoAlertRaisedCount", "testNoAlertRaisedCount",
      lane, 100, 10, "${record:value(\"/name\")==null}", true, "testNoAlertRaisedCount", ThresholdType.COUNT, "3",
      5, true, false, true);
    DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator(metrics, variables, elEvaluator, null, null,
      dataRuleDefinition, new Configuration());
    dataRuleEvaluator.evaluateRule(TestUtil.createSnapshot(lane).get(LaneResolver.getPostFixedLaneForObserver(lane)),
      TestUtil.createSnapshot(lane).get(LaneResolver.getPostFixedLaneForObserver(lane)), lane, new HashMap<String,
        EvictingQueue<Record>>());
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(dataRuleDefinition.getId()));
    Assert.assertNull(gauge);
  }

  @Test
  public void testAlertRaisedPercentage() {
    String lane = "testAlertRaisedPercentage";
    DataRuleDefinition dataRuleDefinition = new DataRuleDefinition("testAlertRaisedPercentage",
      "testAlertRaisedPercentage", lane,
      100, 10, "${record:value(\"/name\")==null}", true, "testAlertRaisedPercentage", ThresholdType.PERCENTAGE, "40",
      5, true, false, true);
    DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator(metrics, variables, elEvaluator, null, null,
      dataRuleDefinition, new Configuration());
    dataRuleEvaluator.evaluateRule(TestUtil.createSnapshot(lane).get(LaneResolver.getPostFixedLaneForObserver(lane)),
      TestUtil.createSnapshot(lane).get(LaneResolver.getPostFixedLaneForObserver(lane)), lane, new HashMap<String,
        EvictingQueue<Record>>());
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(dataRuleDefinition.getId()));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long) 3, ((Map<String, Object>) gauge.getValue()).get("currentValue"));
    Assert.assertNotNull(((Map<String, Object>) gauge.getValue()).get("timestamp"));
  }

  @Test
  public void testNoAlertRaisedPercentage() {
    String lane = "testNoAlertRaisedPercentage";
    DataRuleDefinition dataRuleDefinition = new DataRuleDefinition("testNoAlertRaisedPercentage",
      "testNoAlertRaisedPercentage", lane, 100, 10, "${record:value(\"/name\")==null}", true,
      "testNoAlertRaisedPercentage", ThresholdType.PERCENTAGE, "60", 5, true, false, true);
    DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator(metrics, variables, elEvaluator, null, null,
      dataRuleDefinition, new Configuration());
    dataRuleEvaluator.evaluateRule(TestUtil.createSnapshot(lane).get(LaneResolver.getPostFixedLaneForObserver(lane)),
      TestUtil.createSnapshot(lane).get(LaneResolver.getPostFixedLaneForObserver(lane)), lane, new HashMap<String,
        EvictingQueue<Record>>());
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(dataRuleDefinition.getId()));
    Assert.assertNull(gauge);
  }

  @Test
  public void testGaugeChange() {
    String lane = "testGaugeChange";
    DataRuleDefinition dataRuleDefinition = new DataRuleDefinition("testGaugeChange", "testGaugeChange", lane,
      100, 10, "${record:value(\"/name\")==null}", true, "testGaugeChange", ThresholdType.COUNT, "2", 5, true,
      false, true);
    DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator(metrics, variables, elEvaluator, null, null,
      dataRuleDefinition, new Configuration());

    dataRuleEvaluator.evaluateRule(TestUtil.createSnapshot(lane).get(LaneResolver.getPostFixedLaneForObserver(lane)),
      TestUtil.createSnapshot(lane).get(LaneResolver.getPostFixedLaneForObserver(lane)), lane, new HashMap<String,
        EvictingQueue<Record>>());
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(dataRuleDefinition.getId()));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long) 3, ((Map<String, Object>) gauge.getValue()).get("currentValue"));

    dataRuleEvaluator.evaluateRule(TestUtil.createSnapshot(lane).get(LaneResolver.getPostFixedLaneForObserver(lane)),
      TestUtil.createSnapshot(lane).get(LaneResolver.getPostFixedLaneForObserver(lane)), lane, new HashMap<String,
        EvictingQueue<Record>>());
    gauge = MetricsConfigurator.getGauge(metrics, AlertsUtil.getAlertGaugeName(dataRuleDefinition.getId()));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long) 6, ((Map<String, Object>) gauge.getValue()).get("currentValue"));

  }

  @Test
  public void testMultipleAlertEnabledMeterEnabled() {
    String lane = "testAlertEnabledMeterEnabled";

    DataRuleDefinition nameNotNull = new DataRuleDefinition("nameNotNull",
      "nameNotNull", lane, 100, 10, "${record:value(\"/name\")==null}", true,
      "nameNotNull", ThresholdType.COUNT, "2", 5, true, false, true);
    DataRuleEvaluator dataRuleEvaluator = new DataRuleEvaluator(metrics, variables, elEvaluator, null, null,
      nameNotNull, new Configuration());

    dataRuleEvaluator.evaluateRule(TestUtil.createSnapshot(lane).get(LaneResolver.getPostFixedLaneForObserver(lane)),
      TestUtil.createSnapshot(lane).get(LaneResolver.getPostFixedLaneForObserver(lane)), lane, new HashMap<String,
        EvictingQueue<Record>>());
    Gauge<Object> gauge = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(nameNotNull.getId()));
    Assert.assertNotNull(gauge);
    Assert.assertEquals((long) 3, ((Map<String, Object>) gauge.getValue()).get("currentValue"));
    Assert.assertNotNull(((Map<String, Object>) gauge.getValue()).get("timestamp"));

    DataRuleDefinition nameEqualsStreamSets = new DataRuleDefinition("nameEqualsStreamSets",
      "nameEqualsStreamSets", lane, 100, 10, "${record:value(\"/zip\")==94101}", true,
      "nameEqualsStreamSets", ThresholdType.COUNT, "1", 5, true, false, true);
    DataRuleEvaluator dataRuleEvaluator2 = new DataRuleEvaluator(metrics, variables, elEvaluator, null, null,
      nameEqualsStreamSets, new Configuration());

    dataRuleEvaluator2.evaluateRule(TestUtil.createSnapshot(lane).get(LaneResolver.getPostFixedLaneForObserver(lane)),
      TestUtil.createSnapshot(lane).get(LaneResolver.getPostFixedLaneForObserver(lane)), lane, new HashMap<String,
        EvictingQueue<Record>>());
    Gauge<Object> gauge2 = MetricsConfigurator.getGauge(metrics,
      AlertsUtil.getAlertGaugeName(nameEqualsStreamSets.getId()));
    Assert.assertNotNull(gauge2);
    Assert.assertEquals((long) 2, ((Map<String, Object>) gauge2.getValue()).get("currentValue"));
    Assert.assertNotNull(((Map<String, Object>) gauge2.getValue()).get("timestamp"));

  }

}