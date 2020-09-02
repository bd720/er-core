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
        } //Usato per memorizzare i pesi di ogni vicino
        val arrayVicini = Array.ofDim[Int](maxID + 1) //Usato per tenere gli ID dei miei vicini
        var numeroVicini = 0 //Memorizza il numero di vicini che ho
        partition map { //Mappo gli elementi contenuti nella partizione sono: [id profilo, blocchi]
          pb =>
            val profileID = pb.profileID //ID PROFILO
            val blocchiInCuiCompare = pb.blocks //Blocchi in cui compare questo profilo
            blocchiInCuiCompare foreach { //Per ognuno dei blocchi in cui compare
              block =>
                val idBlocco = block.blockID //ID BLOCCO
                val profiliNelBlocco = blockIndex.value.get(idBlocco) //Leggo gli ID di tutti gli altri profili che sono in quel blocco
                if (profiliNelBlocco.isDefined) {
                  val profiliCheContiene = {
                    if (separatorID >= 0 && profileID <= separatorID) { //Se siamo in un contesto clean e l'id del profilo appartiene al dataset1, i suoi vicini sono nel dataset2
                      profiliNelBlocco.get._2
                    }
                    else {
                      profiliNelBlocco.get._1 //Altrimenti sono nel dataset1
                    }
                  }
                  profiliCheContiene foreach { //Per ognuno dei suoi vicini in questo blocco
                    secondProfileID =>
                      val vicino = secondProfileID.toInt //ID del vicino
                      val pesoAttuale = arrayPesi(vicino) //Leggo il peso attuale che ha questo vicino
                      if (pesoAttuale == 0) { //Se è 0 vuol dire che non l'avevo mai trovato prima
                        arrayVicini.update(numeroVicini, vicino) //Aggiungo all'elenco dei vicini questo nuovo vicino
                        arrayPesi.update(vicino, 1) //Aggiorno il suo peso ad 1
                        numeroVicini = numeroVicini + 1 //Incremento il numero di vicini
                      }
                  }
                }
            }
            var cont = 0 //Contatore che legge quanti vicini mantengo
            var edges: List[UnweightedEdge] = Nil //Edge che verrà dato in uscita per questo profilo che corrisponde ad un match nel dataset 2 (solo se lo trova), va bene solo se clean sto metodo!
            for (i <- 0 to numeroVicini - 1) { //Scorro i vicini che ho trovato
              if (profileID < arrayVicini(i)) { //Aumento il contatore solo se id profiloattuale < id vicino, così li conta una volta sola
                cont += 1
              }
              if (groundtruth.value.contains((profileID, arrayVicini(i)))) { //Il groundtruth è organizzato come (ID dataset1, ID dataset2), quindi devo cercare il profilo con ID minore
                edges = UnweightedEdge(profileID, arrayVicini(i)) :: edges //Genero l'edge che voglio tenere
              }
              else if (groundtruth.value.contains((arrayVicini(i), profileID))) {
                edges = UnweightedEdge(arrayVicini(i), profileID) :: edges
              }
              arrayPesi.update(arrayVicini(i), 0) //Il peso di questo vicino non mi serve più, lo resetto per il prossimo giro
            }
            numeroVicini = 0 //Resetto numero di vicini
            (cont.toDouble, edges) //Fornisco in output il numero di vicini mantenuto e il match vero
        }
    }
  }
}
