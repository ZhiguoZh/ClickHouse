clickhouse_add_executable (wal_action_metadata wal_action_metadata.cpp)
target_link_libraries (wal_action_metadata PRIVATE dbms)

clickhouse_add_executable (and_filter and_filter.cpp)
target_link_libraries (and_filter PRIVATE dbms)