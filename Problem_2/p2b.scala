import scala.collection.mutable.ListBuffer

val grid_size=500

def get_neighbors(tup: List[Int]): List[List[Int]] ={
	val all=List(List(tup(0)-1,tup(1)-1),List(tup(0)-1,tup(1)),List(tup(0)-1,tup(1)+1),
	List(tup(0),tup(1)-1),List(tup(0),tup(1)),List(tup(0),tup(1)+1),
	List(tup(0)+1,tup(1)-1),List(tup(0)+1,tup(1)),List(tup(0)+1,tup(1)+1))
	return all.filter(x => x(0)>=0 && x(1)>=0 && x(0)<grid_size && x(1)< grid_size)
}

def enrich(tt: (List[Int], Int)): List[(List[Int], Int)] = {
	return get_neighbors(tt._1).map(a=>(a,tt._2))
}

def to_list = (tuple: Int) =>
	ListBuffer[Double](tuple.toDouble)

def append = (l1: ListBuffer[Double], value: Int) =>
	(l1 ++ ListBuffer[Double](value.toDouble))

def extend = (l1: ListBuffer[Double], l2: ListBuffer[Double]) =>
	(l1++l2)

def list2id(tup:List[Int]): Int = {
	return tup(0) + tup(1)*grid_size+1
}
    
val file_name="points_k_means.txt"
val raw_data=sc.textFile(file_name)
val data=raw_data.map(x => (x.substring(1,x.length()-1).split(","))).map(x => Vector(x(0).toInt,x(1).toInt))
val regionIds = data.map(x => (List((x(0)-1)/20,(x(1)-1)/20),1))

val counts=regionIds.reduceByKey((a,b)=>a+b)
val enriched=counts.flatMap(enrich)
val base=enriched.combineByKey(to_list, append, extend)

val template = counts.join(base)
val rd_scores=template.map(x=>(list2id(x._1),x._2._1/x._2._2.sum*x._2._2.length)).sortBy(_._2,false)

println("Results:")
rd_scores.take(16)
