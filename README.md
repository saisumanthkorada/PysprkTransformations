# PysprkTransformations

## Goal

- Ingest and validate a raw movie CSV using a predefined Spark schema  
- Extract and clean movie titles and release years via regex  
- Normalize genres into arrays, explode them, and pivot to one-hot encoded flags  
