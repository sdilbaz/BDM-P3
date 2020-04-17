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

def to_list = (tuple: List[Int]) =>
	ListBuffer[List[Int]](tuple)

def append = (l1: ListBuffer[List[Int]], value: List[Int]) =>
	(l1 ++ ListBuffer[List[Int]](value))

def extend = (l1: ListBuffer[List[Int]], l2: ListBuffer[List[Int]]) =>
	(l1++l2)

def list2id(tup:List[Int]): Int = {
	return tup(0) + tup(1)*grid_size+1
}

def neigh_group(tl: ListBuffer[List[Int]]): (Int,ListBuffer[(String, String)]) ={
    val pop_n=ListBuffer[(String, String)]()
    def neigh_check(t1: List[Int],t2: List[Int]): Boolean ={
        return java.lang.Math.abs(t1(0)-t2(0))<=1 && java.lang.Math.abs(t1(1)-t2(1))<=1
    }
    for( i <- 0 to tl.length-1){
        for( j <- i+1 to tl.length-1){
            val a = tl(i)
            val b = tl(j)
            if (neigh_check(a,b)){
                pop_n.append(("c-"+list2id(a).toString(),"c-"+list2id(b).toString()))
            }
        }
    }
    return (pop_n.length,pop_n)
}

def neigh_group_aux(x: (Int, scala.collection.mutable.ListBuffer[List[Int]])): (Int, Int, scala.collection.mutable.ListBuffer[(String, String)]) ={
    val gg=neigh_group(x._2)
    return (x._1,gg._1,gg._2)
}

val file_name="points_k_means.txt"
val raw_data=sc.textFile(file_name)
val data=raw_data.map(x => (x.substring(1,x.length()-1).split(","))).map(x => Vector(x(0).toInt,x(1).toInt))
val regionIds = data.map(x => (List((x(0)-1)/20,(x(1)-1)/20),1))

val counts=regionIds.reduceByKey((a,b)=>a+b).map(x => (x._2,x._1))
val groups=counts.combineByKey(to_list, append, extend)

val pop_count=groups.map(x => (x._1,x._2.length,x._2.map(d => "c-"+list2id(d).toString())))
val pop_neighbors=groups.map(neigh_group_aux).filter(_._2!=0)

println("POP-COUNT sample:")
println(pop_count.take(1).deep.mkString("\n"))

println("POP-NEIGHBORS sample:")
println(pop_neighbors.take(1).deep.mkString("\n"))