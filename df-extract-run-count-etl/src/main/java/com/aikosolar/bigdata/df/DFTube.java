package com.aikosolar.bigdata.df;

/**
  * @author xiaowei.song
  * @version v1.0.0
  */

import java.io.Serializable;

/**
  * DF 管式设备原始数据样列类
  */
public class DFTube {

  private static final long serialVersionUID = 1L;
  public String rowkey = "";
  public String id = "";
  public String eqpID = "";
  public String site = "";
  public String clock = "";
  public String tubeID = "";
  public String text1 = "";
  public String text2 = "";
  public String text3 = "";
  public String text4 = "";
  public String boatID = "";
  public Double gasPOClBubbLeve = -100D;
  public Double gasN2_POCl3VolumeAct = 0.0D;
  public Double gasPOClBubbTempAct = 0.0D;
  public String recipe = "";
  public Integer dataVarAllRunCount = -1;
  public Integer dataVarAllRunNoLef = -1;
  public String vacuumDoorPressure = "";
  public String dataVarAllRunTime = "";
  public Long timeSecond = -1L;
  public String ds = "";
  public String testTime = "";
  public String endTime = "1970-01-01 01:01:00";
  public Long ct = -1L;
  /**
   * 是否是第一个状态位，默认非第一个状态位
   **/
  public Integer firstStatus = 0;

  public String getRowkey() {
    return rowkey;
  }

  public void setRowkey(String rowkey) {
    this.rowkey = rowkey;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getEqpID() {
    return eqpID;
  }

  public void setEqpID(String eqpID) {
    this.eqpID = eqpID;
  }

  public String getSite() {
    return site;
  }

  public void setSite(String site) {
    this.site = site;
  }

  public String getClock() {
    return clock;
  }

  public void setClock(String clock) {
    this.clock = clock;
  }

  public String getTubeID() {
    return tubeID;
  }

  public void setTubeID(String tubeID) {
    this.tubeID = tubeID;
  }

  public String getText1() {
    return text1;
  }

  public void setText1(String text1) {
    this.text1 = text1;
  }

  public String getText2() {
    return text2;
  }

  public void setText2(String text2) {
    this.text2 = text2;
  }

  public String getText3() {
    return text3;
  }

  public void setText3(String text3) {
    this.text3 = text3;
  }

  public String getText4() {
    return text4;
  }

  public void setText4(String text4) {
    this.text4 = text4;
  }

  public String getBoatID() {
    return boatID;
  }

  public void setBoatID(String boatID) {
    this.boatID = boatID;
  }

  public Double getGasPOClBubbLeve() {
    return gasPOClBubbLeve;
  }

  public void setGasPOClBubbLeve(Double gasPOClBubbLeve) {
    this.gasPOClBubbLeve = gasPOClBubbLeve;
  }

  public Double getGasN2_POCl3VolumeAct() {
    return gasN2_POCl3VolumeAct;
  }

  public void setGasN2_POCl3VolumeAct(Double gasN2_POCl3VolumeAct) {
    this.gasN2_POCl3VolumeAct = gasN2_POCl3VolumeAct;
  }

  public Double getGasPOClBubbTempAct() {
    return gasPOClBubbTempAct;
  }

  public void setGasPOClBubbTempAct(Double gasPOClBubbTempAct) {
    this.gasPOClBubbTempAct = gasPOClBubbTempAct;
  }

  public String getRecipe() {
    return recipe;
  }

  public void setRecipe(String recipe) {
    this.recipe = recipe;
  }

  public Integer getDataVarAllRunCount() {
    return dataVarAllRunCount;
  }

  public void setDataVarAllRunCount(Integer dataVarAllRunCount) {
    this.dataVarAllRunCount = dataVarAllRunCount;
  }

  public Integer getDataVarAllRunNoLef() {
    return dataVarAllRunNoLef;
  }

  public void setDataVarAllRunNoLef(Integer dataVarAllRunNoLef) {
    this.dataVarAllRunNoLef = dataVarAllRunNoLef;
  }

  public String getVacuumDoorPressure() {
    return vacuumDoorPressure;
  }

  public void setVacuumDoorPressure(String vacuumDoorPressure) {
    this.vacuumDoorPressure = vacuumDoorPressure;
  }

  public String getDataVarAllRunTime() {
    return dataVarAllRunTime;
  }

  public void setDataVarAllRunTime(String dataVarAllRunTime) {
    this.dataVarAllRunTime = dataVarAllRunTime;
  }

  public Long getTimeSecond() {
    return timeSecond;
  }

  public void setTimeSecond(Long timeSecond) {
    this.timeSecond = timeSecond;
  }

  public String getDs() {
    return ds;
  }

  public void setDs(String ds) {
    this.ds = ds;
  }

  public String getTestTime() {
    return testTime;
  }

  public void setTestTime(String testTime) {
    this.testTime = testTime;
  }

  public Integer getFirstStatus() {
    return firstStatus;
  }

  public void setFirstStatus(Integer firstStatus) {
    this.firstStatus = firstStatus;
  }

  public String getEndTime() {
    return endTime;
  }

  public void setEndTime(String endTime) {
    this.endTime = endTime;
  }

  public Long getCt() {
    return ct;
  }

  public void setCt(Long ct) {
    this.ct = ct;
  }

  public DFTube() {
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("DFTube{");
    sb.append("id='").append(id).append('\'');
    sb.append(", eqpID='").append(eqpID).append('\'');
    sb.append(", site='").append(site).append('\'');
    sb.append(", clock='").append(clock).append('\'');
    sb.append(", tubeID='").append(tubeID).append('\'');
    sb.append(", text1='").append(text1).append('\'');
    sb.append(", text2='").append(text2).append('\'');
    sb.append(", text3='").append(text3).append('\'');
    sb.append(", text4='").append(text4).append('\'');
    sb.append(", boatID='").append(boatID).append('\'');
    sb.append(", gasPOClBubbLeve=").append(gasPOClBubbLeve);
    sb.append(", gasN2_POCl3VolumeAct=").append(gasN2_POCl3VolumeAct);
    sb.append(", gasPOClBubbTempAct=").append(gasPOClBubbTempAct);
    sb.append(", recipe='").append(recipe).append('\'');
    sb.append(", dataVarAllRunCount=").append(dataVarAllRunCount);
    sb.append(", dataVarAllRunNoLef=").append(dataVarAllRunNoLef);
    sb.append(", vacuumDoorPressure='").append(vacuumDoorPressure).append('\'');
    sb.append(", dataVarAllRunTime='").append(dataVarAllRunTime).append('\'');
    sb.append(", timeSecond=").append(timeSecond);
    sb.append(", ds='").append(ds).append('\'');
    sb.append(", testTime='").append(testTime).append('\'');
    sb.append(", endTime='").append(endTime).append('\'');
    sb.append(", ct=").append(ct);
    sb.append(", firstStatus=").append(firstStatus);
    sb.append('}');
    return sb.toString();
  }
}
