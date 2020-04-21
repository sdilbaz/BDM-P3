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

def to_list = (tuple: (Int, Int)) =>
	Set(tuple)

def append = (l1: Set[(Int, Int)], value: (Int, Int)) =>
	(l1 + value)

def extend = (l1: Set[(Int, Int)], l2: Set[(Int, Int)]) =>
	(l1++l2)

def list2id(tup:List[Int]): Int = {
	return tup(0) + tup(1)*grid_size+1
}

def neigh_group(tl: Set[(Int, Int)]): (Int,ListBuffer[(String, String)]) ={
    def get_candidates(tup: (Int,Int)): List[(Int, Int)] = {
        val all=List((tup._1-1,tup._2-1),(tup._1,tup._2-1),(tup._1+1,tup._2-1),(tup._1-1,tup._2))
        return all.filter(x => x._1>=0 && x._2>=0 && x._1<grid_size && x._2< grid_size)
    }
    val pop_n=ListBuffer[(String, String)]()
    tl.foreach{ c=>
        val cands = get_candidates(c)
        cands.foreach{ cand =>
            if (tl(cand)){
                pop_n.append(("c-"+(cand._1+cand._2*grid_size+1),"c-"+(c._1+c._2*grid_size+1)))
            }
        }
    }
    return (pop_n.length,pop_n)
}

def neigh_group_aux(x: (Int, Set[(Int, Int)])): (Int, Int, ListBuffer[(String, String)]) ={
    val gg=neigh_group(x._2)
    return (x._1,gg._1,gg._2)
}

val file_name="points_k_means.txt"
val raw_data=sc.textFile(file_name)
val data=raw_data.map(x => (x.substring(1,x.length()-1).split(","))).map(x => (x(0).toInt,x(1).toInt))
val regionIds = data.map(x => (((x._1-1)/20,(x._2-1)/20),1))

val counts=regionIds.reduceByKey((a,b)=>a+b).map(x => (x._2,x._1))
val groups=counts.combineByKey(to_list, append, extend)

val pop_count=groups.map(x => (x._1,x._2.size,x._2.map(d => "c-"+(d._1+d._2*grid_size+1).toString())))
val pop_neighbors=groups.map(neigh_group_aux).filter(_._2!=0)

println("POP-COUNT sample:")
println(pop_count.take(1).deep.mkString("\n"))

println("POP-NEIGHBORS sample:")
println(pop_neighbors.take(1).deep.mkString("\n"))


