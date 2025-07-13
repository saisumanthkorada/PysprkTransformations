# PysprkTransformations

## Goal 1

- Ingest and validate a raw movie CSV using a predefined Spark schema  
- Extract and clean movie titles and release years via regex  
- Normalize genres into arrays, explode them, and pivot to 'yes' or 'no' flags
- Ingested into silver container

## Goal 2

- Ingest and validate a raw rating csv file
- Extract and clean time stamp and converted unifix timestamp to regular time stamp using the functions in pyspark
- Ingested the data into Silver container .
