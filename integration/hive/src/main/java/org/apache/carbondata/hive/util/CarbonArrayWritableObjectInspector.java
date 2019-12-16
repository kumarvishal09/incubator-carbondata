package org.apache.carbondata.hive.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.ql.io.parquet.serde.DeepParquetHiveMapInspector;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveArrayInspector;
import org.apache.hadoop.hive.ql.io.parquet.serde.StandardParquetHiveMapInspector;
import org.apache.hadoop.hive.ql.io.parquet.serde.primitive.ParquetPrimitiveInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.ArrayWritable;

public class CarbonArrayWritableObjectInspector extends SettableStructObjectInspector {
  private final TypeInfo typeInfo;
  private final List<TypeInfo> fieldInfos;
  private final List<String> fieldNames;
  private final List<StructField> fields;
  private final HashMap<String, StructFieldImpl> fieldsByName;

  public CarbonArrayWritableObjectInspector(StructTypeInfo rowTypeInfo) {
    this.typeInfo = rowTypeInfo;
    this.fieldNames = rowTypeInfo.getAllStructFieldNames();
    this.fieldInfos = rowTypeInfo.getAllStructFieldTypeInfos();
    this.fields = new ArrayList(this.fieldNames.size());
    this.fieldsByName = new HashMap();

    for (int i = 0; i < this.fieldNames.size(); ++i) {
      String name = (String) this.fieldNames.get(i);
      TypeInfo fieldInfo = (TypeInfo) this.fieldInfos.get(i);
      StructFieldImpl field = new StructFieldImpl(name, this.getObjectInspector(fieldInfo), i);
      this.fields.add(field);
      this.fieldsByName.put(name.toLowerCase(), field);
    }

  }

  private ObjectInspector getObjectInspector(TypeInfo typeInfo) {
    if (typeInfo.equals(TypeInfoFactory.doubleTypeInfo)) {
      return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    } else if (typeInfo.equals(TypeInfoFactory.booleanTypeInfo)) {
      return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
    } else if (typeInfo.equals(TypeInfoFactory.floatTypeInfo)) {
      return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
    } else if (typeInfo.equals(TypeInfoFactory.intTypeInfo)) {
      return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    } else if (typeInfo.equals(TypeInfoFactory.longTypeInfo)) {
      return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    } else if (typeInfo.equals(TypeInfoFactory.stringTypeInfo)) {
      return ParquetPrimitiveInspectorFactory.parquetStringInspector;
    } else if (typeInfo instanceof DecimalTypeInfo) {
      return PrimitiveObjectInspectorFactory
          .getPrimitiveWritableObjectInspector((DecimalTypeInfo) typeInfo);
    } else if (typeInfo.getCategory().equals(Category.STRUCT)) {
      return new CarbonArrayWritableObjectInspector(
          (StructTypeInfo) typeInfo);
    } else {
      TypeInfo keyTypeInfo;
      if (typeInfo.getCategory().equals(Category.LIST)) {
        keyTypeInfo = ((ListTypeInfo) typeInfo).getListElementTypeInfo();
        return new ParquetHiveArrayInspector(this.getObjectInspector(keyTypeInfo));
      } else if (typeInfo.getCategory().equals(Category.MAP)) {
        keyTypeInfo = ((MapTypeInfo) typeInfo).getMapKeyTypeInfo();
        TypeInfo valueTypeInfo = ((MapTypeInfo) typeInfo).getMapValueTypeInfo();
        return (ObjectInspector) (
            !keyTypeInfo.equals(TypeInfoFactory.stringTypeInfo) && !keyTypeInfo
                .equals(TypeInfoFactory.byteTypeInfo) && !keyTypeInfo
                .equals(TypeInfoFactory.shortTypeInfo) ?
                new StandardParquetHiveMapInspector(this.getObjectInspector(keyTypeInfo),
                    this.getObjectInspector(valueTypeInfo)) :
                new DeepParquetHiveMapInspector(this.getObjectInspector(keyTypeInfo),
                    this.getObjectInspector(valueTypeInfo)));
      } else if (typeInfo.equals(TypeInfoFactory.byteTypeInfo)) {
        return ParquetPrimitiveInspectorFactory.parquetByteInspector;
      } else if (typeInfo.equals(TypeInfoFactory.shortTypeInfo)) {
        return ParquetPrimitiveInspectorFactory.parquetShortInspector;
      } else if (typeInfo.equals(TypeInfoFactory.timestampTypeInfo)) {
        return PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
      } else if (typeInfo.equals(TypeInfoFactory.binaryTypeInfo)) {
        return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
      } else if (typeInfo.equals(TypeInfoFactory.dateTypeInfo)) {
        return PrimitiveObjectInspectorFactory.writableDateObjectInspector;
      } else if (typeInfo.getTypeName().toLowerCase().startsWith("char")) {
        return PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector((CharTypeInfo) typeInfo);
      } else if (typeInfo.getTypeName().toLowerCase().startsWith("varchar")) {
        return PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector((VarcharTypeInfo) typeInfo);
      } else {
        throw new UnsupportedOperationException("Unknown field type: " + typeInfo);
      }
    }
  }

  public Category getCategory() {
    return Category.STRUCT;
  }

  public String getTypeName() {
    return this.typeInfo.getTypeName();
  }

  public List<? extends StructField> getAllStructFieldRefs() {
    return this.fields;
  }

  public Object getStructFieldData(Object data, StructField fieldRef) {
    if (data == null) {
      return null;
    } else if (data instanceof ArrayWritable) {
      ArrayWritable arr = (ArrayWritable) data;
      StructFieldImpl structField = (StructFieldImpl) fieldRef;
      return structField.getIndex() < arr.get().length ? arr.get()[structField.getIndex()] : null;
    } else if (data instanceof List) {
      return ((List) data).get(((StructFieldImpl) fieldRef).getIndex());
    } else {
      throw new UnsupportedOperationException(
          "Cannot inspect " + data.getClass().getCanonicalName());
    }
  }

  public StructField getStructFieldRef(String name) {
    return (StructField) this.fieldsByName.get(name.toLowerCase());
  }

  public List<Object> getStructFieldsDataAsList(Object data) {
    if (data == null) {
      return null;
    } else if (data instanceof ArrayWritable) {
      ArrayWritable arr = (ArrayWritable) data;
      Object[] arrWritable = arr.get();
      return new ArrayList(Arrays.asList(arrWritable));
    } else if (data instanceof List) {
      return (List) data;
    } else {
      throw new UnsupportedOperationException(
          "Cannot inspect " + data.getClass().getCanonicalName());
    }
  }

  public Object create() {
    ArrayList<Object> list = new ArrayList(this.fields.size());

    for (int i = 0; i < this.fields.size(); ++i) {
      list.add((Object) null);
    }

    return list;
  }

  public Object setStructFieldData(Object struct, StructField field, Object fieldValue) {
    ArrayList<Object> list = (ArrayList) struct;
    list.set(((StructFieldImpl) field).getIndex(), fieldValue);
    return list;
  }

  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    } else if (this.getClass() != obj.getClass()) {
      return false;
    } else {
      CarbonArrayWritableObjectInspector other =
          (CarbonArrayWritableObjectInspector) obj;
      return this.typeInfo == other.typeInfo || this.typeInfo != null && this.typeInfo
          .equals(other.typeInfo);
    }
  }

  public int hashCode() {
    int hash = 5;
     hash = 29 * hash + (this.typeInfo != null ? this.typeInfo.hashCode() : 0);
    return hash;
  }

  class StructFieldImpl implements StructField {
    private final String name;
    private final ObjectInspector inspector;
    private final int index;

    public StructFieldImpl(String name, ObjectInspector inspector, int index) {
      this.name = name;
      this.inspector = inspector;
      this.index = index;
    }

    public String getFieldComment() {
      return "";
    }

    public String getFieldName() {
      return this.name;
    }

    public int getIndex() {
      return this.index;
    }

    public ObjectInspector getFieldObjectInspector() {
      return this.inspector;
    }

    public int getFieldID() {
      return this.index;
    }
  }
}