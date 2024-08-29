import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//import breeze.linalg._
//import breeze.plot._
//import scalaplot._
//import org.apache.spark.ml.stat._
import org.apache.spark.sql.expressions.Window
//import org.apache.spark.ml.feature.VectorAssembler
//import org.apache.spark.ml.linalg.Matrix

object scrip {
  def main(args: Array[String]): Unit = {
    // création d'une session spark
    val spark = SparkSession.builder
      .appName("Projet")
      .master("local[*]")
      .getOrCreate()

    // 1. Chargement du fichier listings.csv
    var df = spark.read
      .option("header", "true")
      .option("sep", ";")
      .csv("listings.csv")

    // 2-a. Affichage du schema du DataFrame
    df.printSchema()

    // 2-b. Lecture des 20 premières lignes
    df.show(5, false)

    // 3. Suppression des les lignes où le prix ou le nombre de reviews est nul
    //df = df.filter(col("price").isNotNull || col("number_of_reviews").isNotNull)
    df = df.na.drop("any", Seq("price", "number_of_reviews"))
    df.show(5, false)

    // 4. Convertir la colonne price en type numérique (double) en retirant d'abord "$"
    df = df.withColumn(
      "price",
      regexp_replace(col("price"), "\\$", "").cast("double")
    )
    df.show(5, false)

    // 5. Calcule du prix moyen des annonces par quartier
    val prixMoyenAnnonParQuartier = df.groupBy("neighbourhood","room_type").agg(avg("price").alias("($) avgPrice"))
    //avgPrice.show()
     
    // 6. Affichage des quartiers avec le moyen le plus élevé 
    var quartieravecPrixMoyenMax = prixMoyenAnnonParQuartier.orderBy(desc("($) avgPrice"))
    quartieravecPrixMoyenMax.show()
    //val adsByDistrict = df.groupBy("neighbourhood").agg(avg("price").alias("$PriceByNeigbourhood"))
    //var maxPriceByDistrict = adsByDistrict.orderBy(desc("$PriceByNeigbourhood"))
    //maxPriceByDistrict.show()

    // 7. Recherche des annonces avec une disponibilité de plus de 300jours par an
    val annonceDispoPlus300 = df.filter(col("availability_365") > 300)
    annonceDispoPlus300.show()

    // 8. Combien de telles annonces existent-elles par quartier
    var nbreAnnDispoPlus300ParQuart = annonceDispoPlus300.groupBy("neighbourhood").count()
    nbreAnnDispoPlus300ParQuart.show()

    // 9. Calcule de la repartition des types d'annonces(room_type) par quartier
    var repartTypeAnnParQuart = df.groupBy("neighbourhood", "room_type").count()
    //println("La repartition des types d'annonces(room_type) par quartier")
    repartTypeAnnParQuart.show()

    // 10. Quelle est la moyenne (reviews_per_month) par quartier
    var avgReviewParQuart = df.groupBy("neighbourhood").agg(avg("reviews_per_month").alias("moyen de revue par mois"))
    // 11. Affichage du Top 10 des quartiers avec les meilleures notes moyennes
    var top10 = avgReviewParQuart.orderBy(desc("moyen de revue par mois"))
    top10.show(10, false)

    // 12-13.. Y-a-t-il une correlation entre le prix(price) et la note moyenne des utilisateurs(reviews_per_month)
    // a- Convertissement de "reviews_per_month" en numérique
    df = df.withColumn("reviews_per_month", col("reviews_per_month").cast("double"))
    // b- Confirmation de la conversion
    df.printSchema()
    // c- Calcule du coefficient de correlation
    val correlation = df.stat.corr("price", "reviews_per_month")
    println("########################################################################################")
    println(s"# Le coefficient de correlation entre le prix et la note moyenne est: $correlation    #")
    println("# Il y'a correlation entre le prix et les revues par mois                              #")
    println("########################################################################################")
    // 14- Affichage de la distribution des prix sous forme d'histogramme
    // en développement
    
    // 15. Le prix le courant
    var prixPlusCourant = df.groupBy("price").agg(count("*").alias("Frequence")).orderBy(desc("Frequence"))
    println()
    println("#######################################################################################")
    prixPlusCourant.limit(1).show()
    println("#######################################################################################")


    //16. Calcule le nombre total de reviews par annonce
    val nbreTotalreviewsParAnn = df.groupBy("room_type").agg(count("number_of_reviews").alias("total reviews par annonces"))
    //17. Affichage du Top10 des annonces les plus revues
    val top10AnnoncePlusRevu = nbreTotalreviewsParAnn.orderBy(desc("total reviews par annonces"))
    top10AnnoncePlusRevu.show(10, false)
    
    //18-a. Définition une fenêtre de partitionnement par quartier et d'ordre par prix décroissant
    val windowSpec = Window.partitionBy("neighbourhood").orderBy(desc("price"))
    //18-b. Calculer le rang des annonces au sein de chaque quartier
    val rankedDf = df.withColumn("rank", rank().over(windowSpec))
    rankedDf.show()

    //19. Affichage les annonces ayant les trois prix les plus élevés pour chaque quartier.
    val topThreeDf = rankedDf.where("rank <= 3")
    topThreeDf.show()

    //20.Enregistrer le DataFrame résultant dans une vue temporaire
    rankedDf.createOrReplaceTempView("RankedListings")
    topThreeDf.createOrReplaceTempView("TopThreeListings")

    //21. Requête SQL pour obtenir les infos suivants
    //22. Les 5 quartiers avec les prix moyens les plus bas.
    val lowestAvgPriceNeighbourhoods = spark.sql("""
        SELECT neighbourhood, AVG(price) as avg_price
        FROM RankedListings
        GROUP BY neighbourhood
        ORDER BY avg_price ASC
        LIMIT 5 
        """)
    lowestAvgPriceNeighbourhoods.show()
    
    //23. Le nombre d'annonces par type de chambre pour chaque quartier.
    val listingsByRoomType = spark.sql("""
        SELECT neighbourhood, room_type, COUNT(*) as num_listings
        FROM RankedListings
        GROUP BY neighbourhood, room_type
        ORDER BY neighbourhood, room_type
        """)
    listingsByRoomType.show()


    //26. Chargement du dataset crime_violence.csv
    var crimeDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ";")
      .csv("crime_violence.csv")
    // Affichage des premières lignes du DataFrame
    crimeDf.show(5)

    //27-a. Renommer la colonne 'Analysis Neighborhood' pour qu'elle corresponde au nom dans le DataFrame Airbnb
    crimeDf = crimeDf.withColumnRenamed("Analysis Neighborhood", "neighbourhood")

    //27-b. Calcule du nombre total d'incidents par quartier
    val incidentsByNeighbourhood = crimeDf.groupBy("neighbourhood").agg(count("*").alias("total_incidents"))
    incidentsByNeighbourhood.show()

    //27-c. Calcule le prix moyen des annonces Airbnb par quartier
    val avgPriceByNeighbourhood = df.groupBy("Neighbourhood").agg(avg("price").alias("avg_price"))
    avgPriceByNeighbourhood.show()

    //27-d. // Joindre les deux DataFrames sur la colonne neighbourhood
    val combinedDf = avgPriceByNeighbourhood.join(incidentsByNeighbourhood,Seq("neighbourhood"), "inner")
    
    //27-e. Affichage des résultats
    combinedDf.show()

    //28. Écriture du DataFrame résultant dans un fichier CSV
    combinedDf.write
      .option("header", "true")
      .csv("combined_neighbourhood_data.csv")

    
    spark.stop()
  }
}
