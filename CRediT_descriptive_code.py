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

### FIGURE 1 + SUPPLEMENTARY MATERIAL 1
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

### SUPPLEMENTARY MATERIAL 2
from pyspark.sql import functions as F
# extract 'sourcetitle' from the 'source' column
df_source_exploded = df_final.select('issn', 'year', 'doi', F.col('source.sourcetitle').alias('journal'))
# group by 'issn', 'journal_title', and 'year' and count distinct DOIs
supplementary_material_2 = df_source_exploded.groupBy('issn', 'journal', 'year') \
                                                   .agg(F.countDistinct('doi').alias('unique_doi_count'))
display(supplementary_material_2)

### FIGURE 2
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, count
spark = SparkSession.builder.appName("group_by_fields_num_au").getOrCreate()
# group by 'doi' and count the number of unique 'auid' values
figure2A = df_final.groupBy("doi").agg(
    countDistinct("auid").alias("num_au"))
# group by 'num_au' and count the number of papers
figure2A = figure2A.groupBy("num_au").agg(
    count("*").alias("num_paper"))
# filter the results to include only rows where 'num_au' is less than or equal to 100
figure2A = figure2A.filter(figure2A.num_au <= 50)
# order the results in ascending order by 'num_au'
figure2A = figure2A.orderBy(figure2A.num_au.asc())
display(figure2A)

### FIGURE 2
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, count
spark = SparkSession.builder.appName("group_by_fields_num_au").getOrCreate()
# group by 'fields' and 'doi' and count the number of unique 'auid' values
figure2B = df_final.groupBy("fields", "doi").agg(
    countDistinct("auid").alias("num_au"))
# group by 'fields' and 'num_au' and count the number of papers
figure2B = figure2B.groupBy("fields", "num_au").agg(
    count("*").alias("num_paper"))
# filter the results to include only rows where 'num_au' is less than or equal to 100
figure2B = figure2B.filter(figure2B.num_au <= 50)
# filter to plot each field individually
figure2B = figure2B.filter(figure2B.fields == "health_sciences")
# order the results in ascending order by 'num_au'
figure2B = figure2B.orderBy(figure2B.num_au.asc())
display(figure2B)

### FIGURE 3
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, col, concat_ws, regexp_replace, explode, size, split, collect_set
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("group_by_doi").getOrCreate()
# group by 'doi' and collect all data related to each individual 'doi' for all the other variables
figure3A = df_final.groupBy("doi").agg(
    collect_list("credit").alias("credit_list"),
    collect_list("fields").alias("fields_list"))
# convert the 'credit_list' variable into a single string format
figure3A = figure3A.withColumn("credit_string", concat_ws(", ", col("credit_list")))
# remove the square brackets and quotes from the string
figure3A = figure3A.withColumn("credit_string", regexp_replace("credit_string", "[\[\]']", ""))
# split the 'credit_string' variable into an array of keywords
figure3A = figure3A.withColumn("credit_array", split(col("credit_string"), ", "))
# explode the 'credit_array' column into separate rows
figure3A = figure3A.withColumn("credit_exploded", explode(col("credit_array")))
# count the number of unique keywords per cell in the 'credit_array' column
figure3A = figure3A.groupBy("doi").agg(
    size(collect_set("credit_exploded")).alias("num_unique_keywords"),
    collect_set("credit_list").alias("credit_list"),
    collect_set("fields_list").alias("fields_list"))
# merge the 'num_unique_keywords' variable from 'figure3A' with the 'fields' variable from 'df_final' by 'doi'
figure3A = figure3A.join(df_final, "doi").select(
    col("doi"),
    col("num_unique_keywords"),
    col("fields"))
# remove duplicate rows
figure3A = figure3A.dropDuplicates()
# drop the 'fields' column
figure3A = figure3A.drop('fields')
# keep only unique combinations of 'doi' and 'num_unique_keywords'
figure3A = figure3A.select('doi', 'num_unique_keywords').distinct()
# filter out 'num_unique_keywords' equal to 15, group, count 'doi', and order by 'num_unique_keywords'
figure3A = figure3A.filter(figure3A.num_unique_keywords <= 14) \
               .groupBy('num_unique_keywords') \
               .agg(F.count('doi').alias('doi_count')) \
               .orderBy('num_unique_keywords', ascending=True)
display(figure3A)

### FIGURE 3
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, col, concat_ws, regexp_replace, explode, size, split, collect_set
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("group_by_doi").getOrCreate()
# group by 'doi' and collect all data related to each individual 'doi' for all the other variables
figure3B = df_final.groupBy("doi").agg(
    collect_list("credit").alias("credit_list"),
    collect_list("fields").alias("fields_list"))
# convert the 'credit_list' variable into a single string format
figure3B = figure3B.withColumn("credit_string", concat_ws(", ", col("credit_list")))
# remove the square brackets and quotes from the string
figure3B = figure3B.withColumn("credit_string", regexp_replace("credit_string", "[\[\]']", ""))
# split the 'credit_string' variable into an array of keywords
figure3B = figure3B.withColumn("credit_array", split(col("credit_string"), ", "))
# explode the 'credit_array' column into separate rows
figure3B = figure3B.withColumn("credit_exploded", explode(col("credit_array")))
# count the number of unique keywords per cell in the 'credit_array' column
figure3B = figure3B.groupBy("doi").agg(
    size(collect_set("credit_exploded")).alias("num_unique_keywords"),
    collect_set("credit_list").alias("credit_list"),
    collect_set("fields_list").alias("fields_list"))
# merge the 'num_unique_keywords' variable with 'fields' variable from 'df_final' by 'doi'
figure3B = figure3B.join(df_final, "doi").select(
    col("doi"),
    col("num_unique_keywords"),
    col("fields"))
# filter by the 'social_sciences' field
figure3B = figure3B.filter(col("fields") == "health_sciences")
# remove duplicate rows
figure3B = figure3B.dropDuplicates()
# drop the 'fields' column
figure3B = figure3B.drop('fields')
# keep only unique combinations of 'doi' and 'num_unique_keywords'
figure3B = figure3B.select('doi', 'num_unique_keywords').distinct()
# filter out 'num_unique_keywords' equal to 15, group, count 'doi', and order by 'num_unique_keywords'
figure3B = figure3B.filter(figure3B.num_unique_keywords <= 14) \
               .groupBy('num_unique_keywords') \
               .agg(F.count('doi').alias('doi_count')) \
               .orderBy('num_unique_keywords', ascending=True)
display(figure3B)