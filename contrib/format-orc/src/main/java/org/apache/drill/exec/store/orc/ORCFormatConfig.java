package org.apache.drill.exec.store.orc;

import org.apache.drill.common.logical.FormatPluginConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class ORCFormatConfig implements FormatPluginConfig {

  public List<String> extensions;
  public int maxErrors;
  public boolean flattenNestedStructureData;


  public boolean getFlattenedStructureData() {

    return flattenNestedStructureData;
  }

  public int getMaxErrors(){
    return maxErrors;

  }

  public void setExtensions(String extensions) {


    if (this.extensions == null){

      this.extensions = new ArrayList<>();
    }

    this.extensions.add(extensions);
  }

  public void setFlattenNestedStructureData(boolean flattenNestedStructureData) {
    this.flattenNestedStructureData = flattenNestedStructureData;
  }

  @Override
  public boolean equals(Object ob){

    if (this == ob) {
      return true;
    }

    if (ob == null || ob.getClass() != this.getClass()) {
      return false;
    }

    ORCFormatConfig obj = (ORCFormatConfig) ob;

    return Objects.equals(this,obj) &&
      Objects.equals(this.extensions, obj.extensions) &&
      Objects.equals(this.flattenNestedStructureData,obj.flattenNestedStructureData);
  }

  @Override
  public int hashCode(){

    return Arrays.hashCode(new Object[]{maxErrors,flattenNestedStructureData,extensions});
  }
}
