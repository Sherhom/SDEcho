cmake_minimum_required(VERSION 3.24)

project(trendiff)

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} SRC)
SET(PG_BASE "/home/yf/pg14") 
add_executable(run ${SRC})

INCLUDE_DIRECTORIES("${CMAKE_CURRENT_SOURCE_DIR}")
INCLUDE_DIRECTORIES("${PG_BASE}/include") 

LINK_DIRECTORIES("${PG_BASE}/lib") 

TARGET_LINK_LIBRARIES(run "${PG_BASE}/lib/libpq.so")


