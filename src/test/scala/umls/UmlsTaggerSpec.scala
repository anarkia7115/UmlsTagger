package umls

import java.io.File
import org.scalatest._
import Matchers._

class UmlsTaggerSpec extends FlatSpec{

  it should "sort words right" in {
    val s = "heart attack and diabetes"
    val tagger = new UmlsTagger()
    assert("and attack diabetes heart" === tagger.sortWords(s))
  }
  
  it should "StemWords right" in {
    val s = "and attack diabetes heart"
    val tagger = new UmlsTagger()
    assert("attack diabetes heart" === tagger.stemWords(s))
  }

  it should "Build right" in {
    val input = new File("/home/shawn/fast_data/umls/cuistr.csv")
    val output = new File("/home/shawn/fast_data/umls/umlsindex")
    val tagger = new UmlsTagger()
    //tagger.buildIndex(input, output)
  }
  
  it should "MapSingleConcept right" in {
    val luceneDir = new File("/home/shawn/fast_data/umls/umlsindex")
    val tagger = new UmlsTagger()
    val strs = List("Lung Cancer", "Heart Attack", "Diabetes")
    strs.foreach(str => {
      val concepts = tagger.annotateConcepts(str, luceneDir)
      Console.println("Query: " + str)
      tagger.printConcepts(concepts)
      assert(1 === concepts.size)
      concepts.head._1 should equal (100.0 +- 0.1)
    })
  }

  it should "MapMultipleConcepts right" in {
    val luceneDir = new File("/home/shawn/fast_data/umls/umlsindex")
    val tagger = new UmlsTagger()
    val strs = List(
        "Heart Attack and diabetes",
        "carcinoma (small-cell) of lung",
        "asthma side effects")
    strs.foreach(str => {
      val concepts = tagger.annotateConcepts(str, luceneDir)
      Console.println("Query: " + str)
      tagger.printConcepts(concepts)
    })
  }
}

