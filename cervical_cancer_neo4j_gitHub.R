#--- Setup ----
{
  library(devtools)
  # install_github("davidlrosenblum/neo4r")  # You will need to install this version of the package for Neo4j >3.5
  library(neo4r)
  library(sqldf)
  library(dplyr)
  library(data.table)
  library(readxl)
  options(scipen = 999)
  
  
}

# setwd("Cervical_Cancer")
df = fread("cervical_cancer.csv")
head(df)
gc()

names(df) <- gsub(x = names(df), pattern = "\\.", replacement = "_")
df = rename(df, target = Biopsy)
str(df)
df <- df %>% dplyr::mutate(id = row_number())

#### Identify continuous variables & level counts #### 
{
  data_type <- df %>% 
    # head(n=1) %>% 
    collect %>% 
    lapply(class) %>% 
    unlist
  
  
  
  cnt_dstnct_cols <- data.frame(t(df %>% summarise_all(n_distinct)))
  cnt_dstnct_cols <- cnt_dstnct_cols %>% dplyr::mutate(id = row_number())
  colnames(cnt_dstnct_cols)[1] <- "V1"
  nrows <- nrow(df)
  
  type <- data.frame(t(data.frame(lapply(df,class))))
  type <- type %>% dplyr::mutate(id = row_number())
  colnames(type)[1] <- "Data_Type"
  
  var_typ <- merge(cnt_dstnct_cols, type, by = 'id')
  
  var_typ$Data_Type <- ifelse(var_typ$Data_Type == 'integer' | var_typ$Data_Type == 'numeric', 'numeric', 'non_numeric')
  var_typ$perc <- var_typ$V1 / nrow(df)  # the larger the number, the more distinct each record is
  
  var_typ$continuous = ifelse(var_typ$Data_Type != 'non_numeric', 1,0)
  
  
}



#--- EDGES ----
# for each column, from value --> column name with the standardized value as weight
cols = colnames(df)
e1 = data.frame(NULL)

i = 1
for (i in i:ncol(df)) {
  
  e_temp = sqldf(paste0("select id as from_id, '",cols[[i]],": '||", cols[[i]]," as label, '",cols[[i]],"' as to_id, .1 as weight, ", cols[[i]]," as val
                          from df
                          where from_id not null
                            and to_id not null
                            and to_id <> 'id'"))
  
  # Check to see if weight should be updated
  if (var_typ[i,c("continuous")]==1) {
    e_temp$weight = round(e_temp$val / max(e_temp$val),6)
    # head(e_temp)
  }
  
  if (nrow(e_temp) > 0) {
    e1 = plyr::rbind.fill(e1,e_temp)
  }
  
  i = i + 1
}

e1$weight = ifelse(e1$to_id=="target",.255555555,e1$weight)
e1$target = ifelse(e1$to_id=="target",1,0)
e1$to_id = ifelse(e1$to_id=="target",e1$label,e1$to_id)


#Make the NAs = 0
e1[is.na(e1)] = 0

edges = e1


# Remove edges to validation set ####
{
  validation = c(23,65,97,2,52,54,102,104)
  edges = edges %>%
    filter(!(from_id %in% validation & target == 1))
  
  val_target = df %>%
    filter(id %in% validation) %>%
    select(id,target)
  
  print(val_target)
}

# Nodes ####
{
  # edges[] <- lapply( edges, factor)
  edges$from_id = as.factor(edges$from_id)
  edges$to_id = as.factor(edges$to_id)
  
  nodes = sqldf("
                select distinct id 
                from (
                  select distinct from_id as id
                  from edges
                  
                  union
                  
                  select distinct to_id as id
                  from edges
                ) a")
  
  nodes$validation = ifelse(nodes$id %in% validation,1,0)
}


setwd("C:\\Users\\TimEa\\AppData\\Local\\Neo4j\\Relate\\Data\\dbmss\\dbms-de2a7cfc-0823-43d6-b259-b1a44ce7db45\\import")
fwrite(nodes,"nodes.csv", row.names=FALSE)
fwrite(edges,"edges.csv", row.names=FALSE)

#--- Write to Neo4j ----
{
  library(neo4r)
  
  packageVersion('neo4r')
  
  con <- neo4j_api$new(
    url = "http://localhost:7474", 
    user = "neo4j", 
    password = ""
  )
  con$ping()
  con$get_version()
  
  
  # delete the database 
  call_neo4j(con
             , include_stats = T, query = paste0("
             MATCH (a) DETACH DELETE a
  "))
  
  
    
  # Write nodes
  call_neo4j(con
             , include_stats = T, query = paste0("
  LOAD CSV WITH HEADERS FROM 'file:///nodes.csv' AS row
  MERGE (e:Node {id: (row.id), validation: row.validation})
  RETURN count(e)
  ;
  "))
  
  # Set Index
  call_neo4j(con
             , include_stats = T, query = paste0("
  CREATE INDEX ON :Node(id)
  "))
  
  # Set Index
  call_neo4j(con
             , include_stats = T, query = paste0("
  CREATE INDEX ON :Node(validation)
  "))
  
  
  # Write edges
  call_neo4j(con
             , include_stats = T, query = paste0("
  WITH 'file:///edges.csv' AS uri
  LOAD CSV WITH HEADERS FROM uri AS row FIELDTERMINATOR ',' 
  MATCH (source:Node {id: row.from_id})
  MATCH (target:Node {id: row.to_id})
  MERGE (source)-[:CONNECTED_TO {weight: toFloat(row.weight), val: (row.val), label: row.label}]-(target)
  ;
  "))
  
  # Set Target
  call_neo4j(con
             , include_stats = T, query = paste0("
  MATCH (n:Node)
  WHERE n.id STARTS WITH 'target'
  SET n.target = 1
  ;
  "))
  
  call_neo4j(con
             , include_stats = T, query = paste0("
  MATCH (n:Node)
  WHERE NOT n.id STARTS WITH 'target'
  SET n.target = 0
  ;
  "))
    
  # Set Index
  call_neo4j(con
             , include_stats = T, query = paste0("
  CREATE INDEX ON :Node(target)
  "))
  
  
  
}


# Graph Data Science ####
{
  
  
  
  call_neo4j(con
             , include_stats = F, query = paste0("
   CALL gds.graph.project.cypher(
  'myGraph',
  'MATCH (n) WHERE n.target = 0 RETURN id(n) AS id, labels(n) AS labels',
  'MATCH (n)-[r]->(m) WHERE n.target = 0 AND m.target = 0 RETURN id(n) AS source, id(m) AS target, type(r) as type, r.weight as weight'
   )
  "))
  
  
  call_neo4j(con
             , include_stats = F, query = paste0("
   CALL gds.pageRank.write('myGraph', {
  maxIterations: 20,
  dampingFactor: 0.85,
  writeProperty: 'pagerank'
  })
  YIELD nodePropertiesWritten, ranIterations
  "))
  
  
  # GRAPH EMBEDDING ####
  call_neo4j(con
             , include_stats = F, query = paste0("
   CALL gds.fastRP.mutate('myGraph',
  {
    embeddingDimension: 4,
    randomSeed: 42,
    mutateProperty: 'embedding',
    relationshipWeightProperty: 'weight',
    iterationWeights: [0.8, 1, 1, 1]
  })
  YIELD nodePropertiesWritten
  "))
  
  
  # GRAPH EMBEDDING ####
  call_neo4j(con
             , include_stats = F, query = paste0("
   CALL gds.knn.write('myGraph', {
    topK: 2,
    nodeProperties: ['embedding'],
    randomSeed: 42,
    concurrency: 1,
    sampleRate: 1.0,
    deltaThreshold: 0.0,
    writeRelationshipType: 'SIMILAR',
    writeProperty: 'score'
  })
  YIELD nodesCompared, relationshipsWritten, similarityDistribution
  RETURN nodesCompared, relationshipsWritten, similarityDistribution.mean as meanSimilarity
  "))
  
  
  # DROP PROJECTION ####
  call_neo4j(con
             , include_stats = F, query = paste0("
   CALL gds.graph.drop('myGraph');
  "))
  
}

print(val_target)




# Look up validation set 

"
MATCH p = (n:Node)-[r:SIMILAR]-(n2:Node)-[r2:CONNECTED_TO]-(n3:Node)
WHERE n.validation = '1'
    AND n3.target = 1
RETURN n.id as id, Type(r), round(r.score*100.00,3) as score, n2.id, r2.val
ORDER BY id

"

# Return specific node
"
MATCH (n:Node)-[r]-(n2:Node) 
WHERE n.id = '2'
RETURN n,r,n2
"


# Return Target for specific node


similar_nodes = c("52" ,"5")
sn = df %>%
  filter(id %in% similar_nodes)



library(magrittr)
"MATCH p = (n:Node)-[r:SIMILAR]-(n2:Node)-[r2:CONNECTED_TO]-(n3:Node) WHERE n.id = '2' AND n3.id = 'target'   RETURN r2.val" %>%
  call_neo4j(con, type = "graph") %>%
  unnest_graph()

df = as_tibble(df)



