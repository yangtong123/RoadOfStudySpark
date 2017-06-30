package com.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * typed操作
  * Created by yangtong on 17/6/30.
  */
object TypedOperation {
    
    case class Employee(name: String, age: Long, depId: Long, gender: String, salary: Long)
    case class Department(id: Long, name: String)
    
    def main(args: Array[String]): Unit = {
        // 创建SparkSession
        val spark = SparkSession
                .builder()
                .appName("TypedOperation")
                .master("local[2]")
                .config("spark.sql.warehouse.dir", "./data/spark-warehouse")
                .getOrCreate()
    
        import spark.implicits._
        
        val employee = spark.read.json("./data/sql/employee.json")
        val employee2 = spark.read.json("./data/sql/employee2.json")
        val department = spark.read.json("./data/sql/department.json")
    
        val employeeDS = employee.as[Employee]
        val employeeDS2 = employee2.as[Employee]
        val departmentDS = department.as[Department]
    
        /**
          * coalesce和repartition操作
          */
        // 都是用来重新定义分区的
        // 区别在于：coalesce，只能用于减少分区数量，而且可以选择不发生shuffle
        // repartiton，可以增加分区，也可以减少分区，必须会发生shuffle，相当于是进行了一次重分区操作
        /*println(employeeDS.rdd.partitions.size)
        val employeeDSRepartitioned = employeeDS.repartition(7);
        // 看一下它的分区情况
        println(employeeDSRepartitioned.rdd.partitions.size)
        
        val employeeDSCoalesced = employeeDSRepartitioned.coalesce(3);
        println(employeeDSCoalesced.rdd.partitions.size)
        employeeDSCoalesced.show()*/
    
    
        /**
          * distinct和dropDuplicates
          */
        // 都是用来进行去重的，区别在哪儿呢？
        // distinct，是根据每一条数据，进行完整内容的比对和去重
        // dropDuplicates，可以根据指定的字段进行去重
        /*val distinctEmployeeDS = employeeDS.distinct();
        distinctEmployeeDS.show()
        val dropDuplicatesEmployeeDS = employeeDS.dropDuplicates(Seq("name"))
        dropDuplicatesEmployeeDS.show()*/
    
        /**
          * except filter intersect
          */
        // except：获取在当前DataSet中有，但是在另外一个DataSet中没有的元素
        // filter：根据我们自己的逻辑，如果返回true，那么就保留该元素，否则就过滤掉该元素
        // intersect：获取两个数据集的交集
//        employeeDS.except(employeeDS2).show()
//        employeeDS.filter { employee => employee.age > 30 }.show()
//        employeeDS.intersect(employeeDS2).show()
    
        /**
          * map flatMap mapPartitions
          */
        // map：将数据集中的每条数据都做一个映射，返回一条新数据
        // flatMap：数据集中的每条数据都可以返回多条数据
        // mapPartitions：一次性对一个partition中的数据进行处理
//        employeeDS.map { (employee: Employee) => (employee.name, employee.salary + 1000) }.show()
//        departmentDS.flatMap {
//            (department: Department) => Seq(Department(department.id + 1, department.name + "_1"), Department(department.id + 2, department.name + "_2"))
//        }.show()
//        employeeDS.mapPartitions { employees => {
//            val result = scala.collection.mutable.ArrayBuffer[(String, Long)]()
//            while(employees.hasNext) {
//                val emp = employees.next()
//                result += ((emp.name, emp.salary + 1000))
//            }
//            result.iterator
//        }
//        }.show()
    
    
        /**
          * joinWith
          */
//        employeeDS.joinWith(departmentDS, $"depId" === $"id").show()
    
        /**
          * sort
          */
//        employeeDS.sort($"salary".desc).show()
    
        /**
          * randomSplit和sample
          */
        // 根据权重切分成几份
        val employeeDSArr = employeeDS.randomSplit(Array(3, 10, 20))
        employeeDSArr.foreach { ds => ds.show() }
    
        // 根据比例随机抽取
        employeeDS.sample(false, 0.3).show()
        
        
    }
}
