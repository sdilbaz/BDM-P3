//26 seconds

import scala.collection.mutable.ListBuffer

val grid_size=500

def get_neighbors(tup: (Int, Int)): List[(Int, Int)] ={
	val all=List((tup._1-1,tup._2-1),(tup._1-1,tup._2),(tup._1-1,tup._2+1),
	(tup._1,tup._2-1),(tup._1,tup._2),(tup._1,tup._2+1),
	(tup._1+1,tup._2-1),(tup._1+1,tup._2),(tup._1+1,tup._2+1))
	return all.filter(x => x._1>=0 && x._2>=0 && x._1<grid_size && x._2< grid_size)
}

def enrich(tt: ((Int, Int), Int)): List[((Int, Int), Int)] = {
	return get_neighbors(tt._1).map(a=>(a,tt._2))
}

def to_list = (tuple: Int) =>
	ListBuffer[Double](tuple.toDouble)

def append = (l1: ListBuffer[Double], value: Int) =>
	(l1 ++ ListBuffer[Double](value.toDouble))

def extend = (l1: ListBuffer[Double], l2: ListBuffer[Double]) =>
	(l1++l2)

def tuple2int(x: (Int,Int)): Int = {
	return (x._1+x._2*grid_size+1)
}

val file_name="points_k_means.txt"
val raw_data=sc.textFile(file_name)
val data=raw_data.map(x => (x.substring(1,x.length()-1).split(","))).map(x => (x(0).toInt,x(1).toInt))
val regionIds = data.map(x => (((x._1-1)/20,(x._2-1)/20),1))

val counts=regionIds.reduceByKey((a,b)=>a+b)
val enriched=counts.flatMap(enrich)
val base=enriched.combineByKey(to_list, append, extend)

val template = counts.join(base)
val rd_scores=template.map(x=>(tuple2int(x._1),x._2._1/x._2._2.sum*x._2._2.length)).sortBy(_._2,false)

println("Results:")
println(rd_scores.take(16).deep.mkString("\n"))
