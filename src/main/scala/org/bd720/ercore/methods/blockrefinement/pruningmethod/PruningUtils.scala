package org.bd720.ercore.methods.blockrefinement.pruningmethod
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bd720.ercore.methods.datastructure.{ProfileBlocks, UnweightedEdge}
object PruningUtils {
  object WeightTypes {
    val CBS = "cbs"
    val JS = "js"
    val chiSquare = "chiSquare"
    val ARCS = "arcs"
    val ECBS = "ecbs"
    val EJS = "ejs"
  }
  object ThresholdTypes {
    val MAX_FRACT_2 = "maxdiv2"
    val AVG = "avg"
  }
  object ComparisonTypes {
    val AND = "and"
    val OR = "or"
  }
  def getAllNeighbors(profileId: Int, block: Array[Set[Int]], separators: Array[Int]): Set[Int] = {
    var output: Set[Int] = Set.empty[Int]
    var i = 0
    while (i < separators.length && profileId > separators(i)) {
      output ++= block(i)
      i += 1
    }
    i += 1
    while (i < separators.length) {
      output ++= block(i)
      i += 1
    }
    if (profileId <= separators.last) {
      output ++= block.last
    }
    output
  }
  def CalcPCPQ(profileBlocksFiltered: RDD[ProfileBlocks], blockIndex: Broadcast[scala.collection.Map[Long, (Set[Long], Set[Long])]],
               maxID: Int, separatorID: Long, groundtruth: Broadcast[scala.collection.immutable.Set[(Long, Long)]]): RDD[(Double, Iterable[UnweightedEdge])] = {
    profileBlocksFiltered mapPartitions {
      partition =>
        val arrayPesi = Array.fill[Int](maxID + 1) {
          0
        } 
        val arrayVicini = Array.ofDim[Int](maxID + 1) 
        var numeroVicini = 0 
        partition map { 
          pb =>
            val profileID = pb.profileID 
            val blocchiInCuiCompare = pb.blocks 
            blocchiInCuiCompare foreach { 
              block =>
                val idBlocco = block.blockID 
                val profiliNelBlocco = blockIndex.value.get(idBlocco) 
                if (profiliNelBlocco.isDefined) {
                  val profiliCheContiene = {
                    if (separatorID >= 0 && profileID <= separatorID) { 
                      profiliNelBlocco.get._2
                    }
                    else {
                      profiliNelBlocco.get._1 
                    }
                  }
                  profiliCheContiene foreach { 
                    secondProfileID =>
                      val vicino = secondProfileID.toInt 
                      val pesoAttuale = arrayPesi(vicino) 
                      if (pesoAttuale == 0) { 
                        arrayVicini.update(numeroVicini, vicino) 
                        arrayPesi.update(vicino, 1) 
                        numeroVicini = numeroVicini + 1 
                      }
                  }
                }
            }
            var cont = 0 
            var edges: List[UnweightedEdge] = Nil 
            for (i <- 0 to numeroVicini - 1) { 
              if (profileID < arrayVicini(i)) { 
                cont += 1
              }
              if (groundtruth.value.contains((profileID, arrayVicini(i)))) { 
                edges = UnweightedEdge(profileID, arrayVicini(i)) :: edges 
              }
              else if (groundtruth.value.contains((arrayVicini(i), profileID))) {
                edges = UnweightedEdge(arrayVicini(i), profileID) :: edges
              }
              arrayPesi.update(arrayVicini(i), 0) 
            }
            numeroVicini = 0 
            (cont.toDouble, edges) 
        }
    }
  }
}
