## spark使用技巧

### 1. UDF函数
#### 1.1. 年龄离散化
```java
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

class App {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("UDF").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)

    val dataset = hc.sql("select label,age from tbl")

    val ageUDF = udf((ageVal: Int) =>
      if (ageVal >= 0 && ageVal <= 18) {
        1d
      } else if (ageVal > 18 && ageVal <= 26) {
        2d
      } else if (ageVal > 26 && age <= 35) {
        3d
      } else if (ageVal > 35) {
        4d
      } else {
        0d
      }
    )

    val datasetClean = datasetClean.withColumn("age", ageUDF(dataset("age")))
  }
}
```

#### 1.2. 如何实现共用离散化函数

### 2. 特征处理

#### 2.1. 特征编码(StringIndexer)
```java
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
class App {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("StringIndexer").setMaster("local[*]")
    val sc = new SparkConf(conf)
    val hc = new HiveContext(sc)

    val dataset = hc.select("select label, feature from tbl")
    val stringIndexer = new StringIndexer().setInputCol("feature").setOutputCol("featureEnc")
    val indexed = stringIndexer.fit(dataset).transform(dataset)
  }
}

```
#### 2.2. OneHotEncoder
```java
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

class App {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Demo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)

    val dataset = hc.sql("select label, feature from tbl")
    val oneHotEncoder = new OneHotEncoder().setInputCol("feature").setOutputCol("featureEnc")
    val encoded = oneHotEncoder.transform(dataset)
  }
}
```

#### 2.3. feature组装
##### VectorAssembler组装feature数据, 构建features用于模型
```java
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

class App {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("VectorAssembler").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
  }
}
```

### 3. spark调优
```bash
#executor idle时退出
set spark.dynamicAllocation.enabled=true
set spark.shuffle.service.enabled=true
#参考http://www.slideshare.net/databricks/dynamic-allocation-in-spark
```
