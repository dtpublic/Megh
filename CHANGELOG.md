Megh Changelog
========================================================================================================================

Version 3.4.0 - 2016-06-06
------------------------------------------------------------------------------------------------------------------------

SPOI-8247 - Deduper output ports - duplicate, expired and error are not schema enabled  
SPOI-8111 - Using Schema generated class name as a operator property
SPOI-6619 - Creation of Laggards Module
SPOI-7973 - Separate JAAS security classes into its own module  
Removed duplicate dependency
SPOI-8122 - App Data Tracker operators failing continuously
Fixed checkstyle violation
SPOI-8120 - Deduper POJO class on input port returns null in streamcodec
SPOI-8043 - Deduper needs a custom stream codec to route tuples with same key to same partitions
SPOI-8044 - Deduper - Separate out dynamic partitioning
SPOI-8042 - Deduper ports do not have schemaRequired annotation
SPOI-8005 - Deduper Bucket Managers not showing up in dtAssemble
SPOI-7943 - The demo applications fails due to numberOfBuckets is less than 1
SPOI-7930 - implement serializer/deserializer for AggregatorMap
SPOI-7911 - Upgrade Megh japicmp to 7.0
SPOI-7818 - Remove dimensions code duplication between megh and malhar
ADT-9 - ADT does not compile with the latest malhar and megh,  
  SPOI-7897 - move aggregatorRegistry depended implementation from AppDataSingleSchemaDimensionStoreHDHT to AbstractAppDataDimensionStoreHDHT
SPOI-7898 - Default JAAS support classes duplicated in dt-library
SPOI-7815 - Megh: Fix japicmp and checkstyle configuration
SPOI-7880 - Fix Checkstyle Violations in Megh
SPOI-7329 - Support elasticity with HDHT

