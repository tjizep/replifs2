cmake_minimum_required(VERSION 3.10)
project(replifs)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")
#find_package( cxxopts REQUIRED ) <-- this doesnt really work since it doesnt exposw the actual include directory

set(CMAKE_CXX_STANDARD 17)

# for profiling
#set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0")
#set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer")
#add_compile_options(-DFUSE_USE_VERSION=26)
# add_compile_options(-Wwritable-strings)
# OSX specifics
#add_compile_options(-D_FILE_OFFSET_BITS=64)
#add_compile_options(-D_POSIX_C_SOURCE=200809L)
#find_package( OSXFuse REQUIRED )

# we can use these as backing too (but their'e slow in this application (possibly in others too))
#find_package( LMDB REQUIRED ) 
#find_package( Rocksdb REQUIRED )

#message( ${Glib_INCLUDE_DIR}  )

include_directories(
        src
        src/repo/rabbit/include
        src/repo/zlib-1.2.8
        src/repo/fse
        src/repo/lz4-r101
        ${STX_H}
        #${OSXFUSE_INCLUDE_DIR}/fuse
        #${ROCKSDB_INCLUDE_DIR}
        #${LMDB_INCLUDE_DIRS}
        #${CMAKE_SOURCE_DIR}/compat
)

add_executable(replifs
        #src/cxxopts.h
        src/persist/storage/pool.cpp
        src/persist/btree.cpp
        src/repo/lz4-r101/lz4.c
        #sfs.cpp
        #src/fuse/repli_ll.cpp
        #src/fuse/repli.cpp
        src/storage/tests/storage.cpp
        #src/fuse/repli_ll.cpp src/storage/structured_file.h src/storage/lru_cache.h src/storage/transaction.h
        src/storage/tests/bt_tx_ctx.h)
        #src/storage/file_storage_alloc.h
        #src/storage/GraphDB.cpp src/storage/GraphDB.h
        #src/fuse/replifs.h tests/test.h src/storage/storage_alloc.h

target_compile_options(replifs PRIVATE -Wall -Wextra)
target_link_libraries(
        replifs
        #${OSXFUSE_LIBRARY}
        #${ROCKSDB_LIBRARIES}
        #${LMDB_LIBRARIES}
)