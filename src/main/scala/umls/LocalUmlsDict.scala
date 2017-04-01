package umls

import java.io.File
import java.io.PrintWriter
import java.io.StringReader
import java.util.concurrent.atomic.AtomicLong
import java.util.regex.Pattern

import scala.Array.canBuildFrom
import scala.Array.fallbackCanBuildFrom
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.collection.mutable.ListBuffer
import scala.collection.mutable

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.Field.Index
import org.apache.lucene.document.Field.Store
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.IndexReader
import org.apache.lucene.index.IndexWriter
import org.apache.lucene.index.IndexWriterConfig
import org.apache.lucene.index.Term
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.search.BooleanQuery
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.ScoreDoc
import org.apache.lucene.search.TermQuery
import org.apache.lucene.store.SimpleFSDirectory
import org.apache.lucene.util.Version

class LocalUmlsDict(umlsRangeFile:String= "./data/uniq_mesh.txt") {
  val TOP_QUERY_MAX=100
  val punctPattern = Pattern.compile("\\p{Punct}")
  val spacePattern = Pattern.compile("\\s+")
  
  def printTopTen():Unit = {
    Source.fromFile(umlsRangeFile)
      .getLines()
      .take(10)
      .foreach(
      line => {
        println(line)
      })
  }

  def loadData():List[String] ={
    val result = ListBuffer[String]()
    var linesRead = 0
    Source.fromFile(umlsRangeFile)
      .getLines()
      .foreach(
      line => {
        if (linesRead % 1000 == 0) {
          Console.println("%d lines read".format(linesRead))
        }
        result += line;
        linesRead += 1;
      })
    return result.toList
  }

  def calcRelatedMesh(
    umlsRange:List[String], 
    luceneIndexDir:File, 
    outputFile:String
  ):Unit = {

    val reader = DirectoryReader.open(
      new SimpleFSDirectory(luceneIndexDir)) 
    val searcher = new IndexSearcher(reader)

    val pw = new PrintWriter(new File(outputFile))

    // get cuis for mesh words
    umlsRange.foreach(
      phrase => {
        val relatedWordToPrint = mutable.Set[String]()
        val cuiList = fuzzyMatch(phrase, reader, searcher)
        //val cuiList = exactMatch(phrase, reader, searcher)
        // get all words under these cuis
        cuiList.foreach(
          cui => {
            relatedWordToPrint ++= queryCui(cui, reader, searcher, "str")
          }
        )
        // write to file
        relatedWordToPrint.foreach(
          relatedWord => {
            pw.write("%s\t%s\n".format(phrase, relatedWord))
          }
        )
      }
    )

    pw.close

  }

  /*
   * return cui list
   * */
  def exactMatch(
    phrase:String, 
    reader: IndexReader,
    searcher: IndexSearcher
    ): Set[String] = {

    val query = new TermQuery(new Term("str", phrase))
    val result = queryMatch(query, reader, searcher)
    if (result.size == 0) {
      println("[%s] has not hit! ".format(phrase))
    }

    return result
  }

  def queryMatch(query:TermQuery, reader: IndexReader, searcher: IndexSearcher): 
    Set[String] = {
    val hits = searcher.search(query, TOP_QUERY_MAX).scoreDocs

    val results = mutable.Set[String]()

    hits.foreach(hit => {
      val doc = reader.document(hit.doc)
      results += doc.get("cui")
    })
    return results.toSet
  }

  def fuzzyMatch(phrase:String, reader: IndexReader, searcher: IndexSearcher): 
    Set[String] = {

    val result = mutable.Set[String]()
    // exact match
    val query = new TermQuery(new Term("str", phrase))
    val exactMatch = queryMatch(query, reader, searcher)
    result ++= exactMatch

    // norm match
    val normPhrase = normalizeCasePunct(phrase)
    val normQuery = new TermQuery(new Term("str_norm", normPhrase))
    val normMatch = queryMatch(normQuery, reader, searcher)
    result ++= normMatch

    // sorted match
    val sortedPhrase = sortWords(normPhrase)
    val sortedQuery = new TermQuery(new Term("str_sorted", sortedPhrase))
    val sortedMatch = queryMatch(sortedQuery, reader, searcher)
    result ++= sortedMatch

    // stemmed match
    val stemmedPhrase = stemWords(normPhrase)
    val stemmedQuery = new TermQuery(new Term("str_stemmed", stemmedPhrase))
    val stemmedMatch = queryMatch(stemmedQuery, reader, searcher)
    result ++= stemmedMatch

    if (result.size == 0) {
      println("[%s] has not hit! ".format(phrase))
    }

    return result.toSet
  }

  def normalizeCasePunct(str: String): String = {
    val str_lps = punctPattern
      .matcher(str.toLowerCase())
      .replaceAll(" ")
    spacePattern.matcher(str_lps).replaceAll(" ")
  }

  def sortWords(str: String): String = {
    val words = str.split(" ")
    words.sortWith(_ < _).mkString(" ")
  }
  
  def stemWords(str: String): String = {
    val stemmedWords = ArrayBuffer[String]()
    val tokenStream = getAnalyzer().tokenStream(
      "str_stemmed", new StringReader(str))
    val ctattr = tokenStream.addAttribute(
      classOf[CharTermAttribute])    
    tokenStream.reset()
    while (tokenStream.incrementToken()) {
      stemmedWords += ctattr.toString()
    }
    stemmedWords.mkString(" ")
  }

  def getAnalyzer(): Analyzer = {
    new StandardAnalyzer(Version.LUCENE_46)
  }
  

  /*
   * return related word list
   * */

  def queryCui(
    cui:String, 
    reader: IndexReader,
    searcher: IndexSearcher,
    key: String
    ): List[String] = {

    val query = new TermQuery(new Term("cui", cui))
    val hits = searcher.search(query, TOP_QUERY_MAX).scoreDocs

    val results = ListBuffer[String]()

    hits.foreach(hit => {
      val doc = reader.document(hit.doc)
      results += doc.get("str")
    })

    return results.toList
  }

  def run() {
    val umlsRange = loadData()
    val ld = new File("./data/umlsindex")
    val out = "./data/expanded_mesh.txt"
    calcRelatedMesh(umlsRange, ld, out)
  }

}
