project_name = "proj_1060_labor_division"
permissions = "fulldata"
%run Snippets/header_008

###### SCOPUS + PLOS MERGE

# structured_statements_metadata is the structured_statements dataframe with Scopus metadata joined on
structured_statements_metadata = spark.read.parquet("/Projects/proj_1060_labor_division/structured_statements_metadata")
display(structured_statements_metadata)

from pyspark.sql.functions import explode
# select the desired columns and explode arrays
structured_Scopus = structured_statements_metadata.select(
    "Eid", "contributions", "Year", "subjareas", "Au", "publication_type", "doi", "issn", "source"
).withColumn("contribution", explode("contributions")).withColumn("author", explode("Au"))
# select the required columns from exploded dataframe
structured_Scopus = structured_Scopus.select(
    "Eid", "contribution", "Year", "subjareas", "author", "publication_type", "doi", "issn", "source")
# rename columns for better readability
structured_Scopus = structured_Scopus.withColumnRenamed("contribution", "credit") \
    .withColumnRenamed("author", "au")
display(structured_Scopus)

from pyspark.sql.functions import expr
# filter to keep only rows where "publication_type" is "ar"
structured_Scopus = structured_Scopus.filter(structured_Scopus.publication_type == "ar")
# filter to keep only rows where "Year" is 2020, 2021, 2022, or 2023
structured_Scopus = structured_Scopus.filter(structured_Scopus.Year.isin([2020, 2021, 2022, 2023]))
# filter to keep rows where "credit.auid" matches "au.auid"
structured_Scopus = structured_Scopus.filter(expr("credit.auid = au.auid"))
display(structured_Scopus)

from pyspark.sql.functions import col, countDistinct
# extract information from the "credit" column
structured_Scopus = structured_Scopus.withColumn("CRediT_mapping", col("credit.CRediT_mapping"))
# extract information from the "au" column
structured_Scopus = structured_Scopus.withColumn("Authorseq", col("au.Authorseq")) \
    .withColumn("auid", col("au.auid")) \
    .withColumn("given_name", col("au.given_name")) \
    .withColumn("surname", col("au.surname"))
# drop the original columns
structured_Scopus = structured_Scopus.drop("credit")
structured_Scopus = structured_Scopus.drop("au")
# rename columns
structured_Scopus = structured_Scopus.withColumnRenamed("Eid", "eid") \
    .withColumnRenamed("Year", "year") \
    .withColumnRenamed("CRediT_mapping", "credit") \
    .withColumnRenamed("Authorseq", "position") \
    .withColumnRenamed("given_name", "name")
# group by DOI and count distinct AUIDs
doi_auid_count = structured_Scopus.groupBy("doi").agg(countDistinct("auid").alias("aunum"))
# join the count back to the original DataFrame
structured_Scopus = structured_Scopus.join(doi_auid_count, on="doi", how="left")
# reorder columns
structured_Scopus = structured_Scopus.select(
    "eid", "auid", "name", "surname", "position", "credit", "year", "subjareas", "publication_type", "doi", "issn", "source", "aunum")
display(structured_Scopus)

# df_ani is the article level Scopus metadata datset
df_ani = spark.read.parquet(basePath + tablename_ani)
display(df_ani)

from pyspark.sql.functions import explode_outer
# select desired columns
df_ani = df_ani.select("Eid", "subjareas", "Au", "publication_type", "doi", "issn", "source")
# filter rows where "publication_type" is "ar"
df_ani = df_ani.filter(df_ani.publication_type == "ar")
# explode the "Au" column
df_ani = df_ani.withColumn("au", explode_outer("Au"))
display(df_ani)

from pyspark.sql.functions import col
# extract information from the "au" column
df_ani = df_ani.withColumn("Authorseq", col("au.Authorseq")) \
    .withColumn("auid", col("au.auid")) \
    .withColumn("given_name", col("au.given_name")) \
    .withColumn("surname_df_ani", col("au.surname"))
# drop the original columns
df_ani = df_ani.drop("au")
# rename columns
df_ani = df_ani.withColumnRenamed("Eid", "eid") \
    .withColumnRenamed("Authorseq", "position") \
    .withColumnRenamed("given_name", "name_df_ani")
display(df_ani)

from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
spark = SparkSession.builder.appName("DOI AUID Count").getOrCreate()
# group by DOI and count distinct AUIDs
doi_auid_count = df_ani.groupBy("doi").agg(countDistinct("auid").alias("aunum_df_ani"))
# join the count back to the original DataFrame
df_ani = df_ani.join(doi_auid_count, on="doi", how="left")
display(df_ani)

from pyspark.sql import SparkSession
from pyspark.sql.functions import substring
spark = SparkSession.builder.appName("Name Initial").getOrCreate()
# add name initial variable
df_ani = df_ani.withColumn("initial_df_ani", substring("name_df_ani", 1, 1))
display(df_ani)

# read PLOS imported data
PLOS = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/vdicesare@ugr.es/PLOScredit-2.csv")
display(PLOS)

from pyspark.sql.functions import col
# rename columns
PLOS = PLOS.withColumnRenamed("surname", "surname_PLOS") \
    .withColumnRenamed("name", "name_PLOS")
display(PLOS)

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, countDistinct
spark = SparkSession.builder.appName("DOI Name Surname Count").getOrCreate()
# create a unique identifier for authors by combining name and surname
PLOS = PLOS.withColumn("author_id", concat_ws(" ", PLOS.name_PLOS, PLOS.surname_PLOS))
# group by DOI and count distinct author_id values
doi_author_count = PLOS.groupBy("doi").agg(countDistinct("author_id").alias("aunum_PLOS"))
# join the count back to the original DataFrame
PLOS = PLOS.join(doi_author_count, on="doi", how="left")
display(PLOS)

from pyspark.sql import SparkSession
from pyspark.sql.functions import substring
spark = SparkSession.builder.appName("Name Initial").getOrCreate()
# add name initial variable
PLOS = PLOS.withColumn("initial_PLOS", substring("name_PLOS", 1, 1))
display(PLOS)

from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, col, upper
spark = SparkSession.builder.appName("DOI AUID Count").getOrCreate()
# join the dataframes on 'doi'
full_PLOS = df_ani.join(PLOS, (df_ani.doi == PLOS.doi), "inner") \
                 .drop(PLOS.doi)
# reorder columns
full_PLOS = full_PLOS.select(
    "eid", "auid", "name_df_ani", "surname_df_ani", "name_PLOS", "surname_PLOS", "initial_df_ani", "initial_PLOS", "position", "credit", "year", "subjareas", "publication_type", "doi", "issn", "source", "aunum_df_ani", "aunum_PLOS")
# transform specified columns to uppercase
columns_to_upper = ["name_df_ani", "surname_df_ani", "name_PLOS", "surname_PLOS", "initial_df_ani", "initial_PLOS"]
for col_name in columns_to_upper:
    full_PLOS = full_PLOS.withColumn(col_name, upper(col(col_name)))
# remove rows where surname_df_ani or surname_PLOS is null
full_PLOS = full_PLOS.na.drop(subset=["surname_df_ani", "surname_PLOS"])
display(full_PLOS)

### ROUND 1
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
spark = SparkSession.builder.appName("SurnameFilter").getOrCreate()
# find matching surnames and store in full_PLOS1
full_PLOS1 = full_PLOS.filter(full_PLOS.surname_df_ani == full_PLOS.surname_PLOS)
# group by surname_df_ani, surname_PLOS, and doi, then count occurrences
combined_counts = full_PLOS1.groupBy("surname_df_ani", "surname_PLOS", "doi").agg(count("*").alias("count_per_combination"))
# filter full_PLOS1 to keep only rows where count_per_combination is 1
full_PLOS1 = full_PLOS1.join(combined_counts, on=["surname_df_ani", "surname_PLOS", "doi"]).filter(col("count_per_combination") == 1)
# select only columns present in full_PLOS to maintain the same schema and order
full_PLOS1 = full_PLOS1.select(full_PLOS.columns)
display(full_PLOS1)

### ROUND 2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
spark = SparkSession.builder.appName("InitialSurnameFilter").getOrCreate()
# get the rows not included in full_PLOS1
full_PLOS2 = full_PLOS.subtract(full_PLOS1)
# find matching initials and surnames and store in full_PLOS2
full_PLOS2 = full_PLOS2.filter((full_PLOS2.initial_df_ani == full_PLOS2.initial_PLOS) &
                               (full_PLOS2.surname_df_ani == full_PLOS2.surname_PLOS))
# group by initial_df_ani, surname_df_ani, initial_PLOS, surname_PLOS, and doi, then count occurrences
combined_counts = full_PLOS2.groupBy("initial_df_ani", "surname_df_ani", "initial_PLOS", "surname_PLOS", "doi").agg(count("*").alias("count_per_combination"))
# filter full_PLOS2 to keep only rows where count_per_combination is 1
full_PLOS2 = full_PLOS2.join(combined_counts, on=["initial_df_ani", "surname_df_ani", "initial_PLOS", "surname_PLOS", "doi"]).filter(col("count_per_combination") == 1)
# select only columns present in full_PLOS1 to maintain the same schema and order
full_PLOS2 = full_PLOS2.select(full_PLOS1.columns)
display(full_PLOS2)

### ROUND 3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
spark = SparkSession.builder.appName("NameSurnameFilter").getOrCreate()
# get the rows not included in full_PLOS1 or full_PLOS2
full_PLOS3 = full_PLOS.subtract(full_PLOS1)
full_PLOS3 = full_PLOS3.subtract(full_PLOS2)
# find matching names and surnames and store in full_PLOS3
full_PLOS3 = full_PLOS3.filter((full_PLOS3.name_df_ani == full_PLOS3.name_PLOS) &
                               (full_PLOS3.surname_df_ani == full_PLOS3.surname_PLOS))
# group by name_df_ani, surname_df_ani, name_PLOS, surname_PLOS, and doi, then count occurrences
combined_counts = full_PLOS3.groupBy("name_df_ani", "surname_df_ani", "name_PLOS", "surname_PLOS", "doi").agg(count("*").alias("count_per_combination"))
# filter full_PLOS3 to keep only rows where count_per_combination is 1
full_PLOS3 = full_PLOS3.join(combined_counts, on=["name_df_ani", "surname_df_ani", "name_PLOS", "surname_PLOS", "doi"]).filter(col("count_per_combination") == 1)
# select only columns present in full_PLOS2 to maintain the same schema and order
full_PLOS3 = full_PLOS3.select(full_PLOS2.columns)
display(full_PLOS3)

### ROUND 4
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, trim, count
spark = SparkSession.builder.appName("SplitSurnameFilter").getOrCreate()
# get the rows not included in full_PLOS1, full_PLOS2 or full_PLOS3
full_PLOS4 = full_PLOS.subtract(full_PLOS1)
full_PLOS4 = full_PLOS4.subtract(full_PLOS2)
full_PLOS4 = full_PLOS4.subtract(full_PLOS3)
# define the regex pattern to match the part before and after the first space or hyphen
pattern_before = r"^([^ -]+)"
pattern_after = r"^[^ -]+[ -](.+)$"
# create the new variables using regexp_extract
full_PLOS4 = full_PLOS4.withColumn("first_surname_PLOS", regexp_extract(col("surname_PLOS"), pattern_before, 1))
full_PLOS4 = full_PLOS4.withColumn("second_surname_PLOS", regexp_extract(col("surname_PLOS"), pattern_after, 1))
# trim the columns to remove any leading or trailing spaces
full_PLOS4 = full_PLOS4.withColumn("first_surname_PLOS", trim(col("first_surname_PLOS")))
full_PLOS4 = full_PLOS4.withColumn("second_surname_PLOS", trim(col("second_surname_PLOS")))
# filter out rows where the splitting process was not applied (which is to say, where second_surname_PLOS is empty)
full_PLOS4 = full_PLOS4.filter(col("second_surname_PLOS") != "")
# find matching surnames between surname_df_ani and second_surname_PLOS
full_PLOS4 = full_PLOS4.filter(full_PLOS4.surname_df_ani == full_PLOS4.second_surname_PLOS)
# group by surname_df_ani, second_surname_PLOS, and doi, then count occurrences
combined_counts = full_PLOS4.groupBy("surname_df_ani", "second_surname_PLOS", "doi").agg(count("*").alias("count_per_combination"))
# filter full_PLOS4 to keep only rows where count_per_combination is 1
full_PLOS4 = full_PLOS4.join(combined_counts, on=["surname_df_ani", "second_surname_PLOS", "doi"]).filter(col("count_per_combination") == 1)
# drop the original surname_PLOS column and rename second_surname_PLOS to surname_PLOS
full_PLOS4 = full_PLOS4.drop("surname_PLOS").withColumnRenamed("second_surname_PLOS", "surname_PLOS")
# select only columns present in full_PLOS to maintain the same schema and order
full_PLOS4 = full_PLOS4.select(full_PLOS.columns)
display(full_PLOS4)

### LAST ROUND
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
spark = SparkSession.builder.appName("FinalPLOSMerge").getOrCreate()
# merge full_PLOS1, full_PLOS2, full_PLOS3 and full_PLOS4 vertically
last_PLOS = full_PLOS1.union(full_PLOS2).union(full_PLOS3).union(full_PLOS4)
# group by DOI and count distinct AUIDs
doi_auid_count = last_PLOS.groupBy("doi").agg(countDistinct("auid").alias("aunum"))
# join the count back to the original dataFrame
last_PLOS = last_PLOS.join(doi_auid_count, on="doi", how="left")
# eliminate those DOIs where there are missing authors that could not be identified
last_PLOS = last_PLOS.filter(col("aunum") == col("aunum_PLOS"))
# drop and rename variables in order to match structured_Scopus
last_PLOS = last_PLOS.drop("name_df_ani", "surname_df_ani", "initial_df_ani", "initial_PLOS", "aunum_df_ani", "aunum_PLOS")
last_PLOS = last_PLOS.withColumnRenamed("name_PLOS", "name").withColumnRenamed("surname_PLOS", "surname")
# also reorder variables in order to match structured_Scopus
last_PLOS = last_PLOS.select("eid", "auid", "name", "surname", "position", "credit", "year", "subjareas", "publication_type", "doi", "issn", "source", "aunum")
display(last_PLOS)

from pyspark.sql.functions import concat_ws
# convert the 'credit' column in structured_Scopus dataframe from ARRAY<STRING> to STRING
structured_Scopus = structured_Scopus.withColumn("credit", concat_ws(", ", "credit"))
# final merge between structured_Scopus and full_PLOS
df_final = structured_Scopus.unionAll(last_PLOS)
display(df_final)


###### OVERALL + PAPER LEVEL ANALYSIS

### SUPPLEMENTARY MATERIAL 1
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, when, col
spark = SparkSession.builder.appName("assign_fields").getOrCreate()
# explode the 'subjareas' column to create a new row for each subjarea
df_final = df_final.withColumn("subjareas", explode(col("subjareas")))
# convert 'subjareas' column to string type
df_final = df_final.withColumn("subjareas", col("subjareas").cast("string"))
# create a new column 'fields' based on the distribution
df_final = df_final.withColumn("fields",
    when(df_final.subjareas.isin("MEDI", "NURS", "VETE", "DENT", "HEAL"), "health_sciences")
    .when(df_final.subjareas.isin("AGRI", "BIOC", "IMMU", "NEUR", "PHAR"), "life_sciences")
    .when(df_final.subjareas.isin("CENG", "CHEM", "COMP", "EART", "ENER", "ENGI", "ENVI", "MATE", "MATH", "PHYS"), "physical_sciences")
    .when(df_final.subjareas.isin("ARTS", "BUSI", "DECI", "ECON", "PSYC", "SOCI"), "social_sciences")
    .otherwise("multidisciplinary"))
display(df_final)

### FIGURE 2 + SUPPLEMENTARY MATERIAL 1
from pyspark.sql import functions as F
# group by 'year' and count distinct DOIs
supplementary_material_1A = df_final.groupBy('year').agg(F.countDistinct('doi').alias('unique_dois'))
display(supplementary_material_1A)

### SUPPLEMENTARY MATERIAL 1
from pyspark.sql import functions as F
# group by 'year' and 'field' and count distinct DOIs
supplementary_material_1B = df_final.groupBy("year", "fields").agg(
    F.countDistinct("doi").alias("unique_dois"))
display(supplementary_material_1B)

### SUPPLEMENTARY MATERIAL 1
from pyspark.sql import functions as F
# group by 'year' and 'subjarea' and count distinct DOIs
supplementary_material_1C = df_final.groupBy("year", "subjareas").agg(
    F.countDistinct("doi").alias("unique_dois"))
display(supplementary_material_1C)

### TABLE 1
df_unique = df_final.select("eid", "auid", "fields").dropDuplicates()
from pyspark.sql import functions as F
authors_per_paper = df_unique.groupBy("eid").agg(
    F.countDistinct("auid").alias("num_au"))
summary_overall = (
    authors_per_paper.agg(
        F.expr("percentile_approx(num_au, 0.0)").alias("min"),
        F.expr("percentile_approx(num_au, 0.25)").alias("q1"),
        F.expr("percentile_approx(num_au, 0.5)").alias("median"),
        F.avg("num_au").alias("mean"),
        F.expr("percentile_approx(num_au, 0.75)").alias("q3"),
        F.expr("percentile_approx(num_au, 1.0)").alias("max"),
        F.stddev("num_au").alias("sd")))
# mode
mode_value = (
    authors_per_paper.groupBy("num_au")
    .agg(F.count("*").alias("freq"))
    .orderBy(F.desc("freq"), F.asc("num_au"))
    .limit(1)
    .collect()[0]["num_au"])
summary_with_mode = summary_overall.withColumn("mode", F.lit(mode_value))
summary_with_mode.show(truncate=False)
from pyspark.sql.window import Window
# count authors per paper per field
authors_per_paper_field = df_unique.groupBy("fields", "eid").agg(
    F.countDistinct("auid").alias("num_au"))
# summary stats per field
summary_by_field = (
    authors_per_paper_field.groupBy("fields")
    .agg(
        F.expr("percentile_approx(num_au, 0.0)").alias("min"),
        F.expr("percentile_approx(num_au, 0.25)").alias("q1"),
        F.expr("percentile_approx(num_au, 0.5)").alias("median"),
        F.avg("num_au").alias("mean"),
        F.expr("percentile_approx(num_au, 0.75)").alias("q3"),
        F.expr("percentile_approx(num_au, 1.0)").alias("max"),
        F.stddev("num_au").alias("sd")))
# mode per field
window = Window.partitionBy("fields").orderBy(F.desc("freq"), F.asc("num_au"))
mode_df = (
    authors_per_paper_field.groupBy("fields", "num_au")
    .agg(F.count("*").alias("freq"))
    .withColumn("rank", F.row_number().over(window))
    .filter(F.col("rank") == 1)
    .select("fields", F.col("num_au").alias("mode")))
# join mode to summary
summary_by_field_with_mode = summary_by_field.join(mode_df, on="fields")
summary_by_field_with_mode.show(truncate=False)

### TABLE 2
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# keep only unique (eid, credit, fields) combinations
df_credit = df_final.select("eid", "credit", "fields").dropDuplicates()
# split credit by comma and explode so each role is in a separate row
df_credit_exploded = df_credit.withColumn(
    "credit_split", F.split(F.col("credit"), ",\s*")  # split by comma and optional space
).withColumn("single_credit", F.explode(F.col("credit_split")))
# remove leading/trailing spaces
df_credit_exploded = df_credit_exploded.withColumn(
    "single_credit", F.trim(F.col("single_credit")))
# keep only unique credits per eid
df_credit_unique = df_credit_exploded.select("eid", "fields", "single_credit").dropDuplicates()
# count unique credits per paper (eid)
credits_per_paper = df_credit_unique.groupBy("eid").agg(
    F.countDistinct("single_credit").alias("num_credit"))
# overall descriptive statistics
summary_overall_credit = (
    credits_per_paper.agg(
        F.expr("percentile_approx(num_credit, 0.0)").alias("min"),
        F.expr("percentile_approx(num_credit, 0.25)").alias("q1"),
        F.expr("percentile_approx(num_credit, 0.5)").alias("median"),
        F.avg("num_credit").alias("mean"),
        F.expr("percentile_approx(num_credit, 0.75)").alias("q3"),
        F.expr("percentile_approx(num_credit, 1.0)").alias("max"),
        F.stddev("num_credit").alias("sd")))
# mode overall
mode_value_credit = (
    credits_per_paper.groupBy("num_credit")
    .agg(F.count("*").alias("freq"))
    .orderBy(F.desc("freq"), F.asc("num_credit"))
    .limit(1)
    .collect()[0]["num_credit"])
summary_overall_credit = summary_overall_credit.withColumn("mode", F.lit(mode_value_credit))
summary_overall_credit.show(truncate=False)
# descriptive statistics per field
credits_per_paper_field = df_credit_unique.groupBy("fields", "eid").agg(
    F.countDistinct("single_credit").alias("num_credit"))
summary_by_field_credit = (
    credits_per_paper_field.groupBy("fields")
    .agg(
        F.expr("percentile_approx(num_credit, 0.0)").alias("min"),
        F.expr("percentile_approx(num_credit, 0.25)").alias("q1"),
        F.expr("percentile_approx(num_credit, 0.5)").alias("median"),
        F.avg("num_credit").alias("mean"),
        F.expr("percentile_approx(num_credit, 0.75)").alias("q3"),
        F.expr("percentile_approx(num_credit, 1.0)").alias("max"),
        F.stddev("num_credit").alias("sd")))
# mode per field
window = Window.partitionBy("fields").orderBy(F.desc("freq"), F.asc("num_credit"))
mode_df_credit = (
    credits_per_paper_field.groupBy("fields", "num_credit")
    .agg(F.count("*").alias("freq"))
    .withColumn("rank", F.row_number().over(window))
    .filter(F.col("rank") == 1)
    .select("fields", F.col("num_credit").alias("mode")))
summary_by_field_credit_with_mode = summary_by_field_credit.join(mode_df_credit, on="fields")
summary_by_field_credit_with_mode.show(truncate=False)

### SUPPLEMENTARY MATERIAL 2
from pyspark.sql import functions as F
# extract 'sourcetitle' from the 'source' column
df_source_exploded = df_final.select('issn', 'year', 'doi', F.col('source.sourcetitle').alias('journal'))
# group by 'issn', 'journal_title', and 'year' and count distinct DOIs
supplementary_material_2 = df_source_exploded.groupBy('issn', 'journal', 'year') \
                                                   .agg(F.countDistinct('doi').alias('unique_doi_count'))
display(supplementary_material_2)
