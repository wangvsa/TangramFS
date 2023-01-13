#------------------------------------------------------------------------------
# Include source and build directories
#------------------------------------------------------------------------------
include_directories(
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src/ucx
    ${CMAKE_SOURCE_DIR}/src/client
    ${CMAKE_SOURCE_DIR}/src/server
    )


#------------------------------------------------------------------------------
# Set sources
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# External dependencies
#------------------------------------------------------------------------------
# MPI
find_package(MPI REQUIRED)
if(MPI_FOUND)
    include_directories(${MPI_CXX_INCLUDE_DIRS})
    set(TANGRAMFS_EXT_INCLUDE_DEPENDENCIES
            ${MPI_C_INCLUDE_DIRS} ${TANGRAMFS_EXT_INCLUDE_DEPENDENCIES})
    set(TANGRAMFS_EXT_LIB_DEPENDENCIES
            ${MPI_C_LIBRARIES} ${TANGRAMFS_EXT_LIB_DEPENDENCIES})
else()
    message(STATUS, "MPI not found")
endif()

# UCX is not a cmake project
include_directories(${UCX_DIR}/include)
set(TANGRAMFS_EXT_LIB_DEPENDENCIES
        ${UCX_DIR}/lib/libuct.so
        ${UCX_DIR}/lib/libucs.so
        ${TANGRAMFS_EXT_LIB_DEPENDENCIES})

#------------------------------------------------------------------------------
# Libraries - libtangramfs.so
#------------------------------------------------------------------------------
set(TANGRAMFS_BUILD_INCLUDE_DEPENDENCIES
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_BINARY_DIR}
        )

set(TANGRAMFS_SRCS
        ${CMAKE_CURRENT_SOURCE_DIR}/client/tangramfs.c
        ${CMAKE_CURRENT_SOURCE_DIR}/client/tangramfs-rpc.c
        ${CMAKE_CURRENT_SOURCE_DIR}/client/tangramfs-posix-wrapper.c
        ${CMAKE_CURRENT_SOURCE_DIR}/client/tangramfs-semantics-impl.c
        ${CMAKE_CURRENT_SOURCE_DIR}/client/tangramfs-delegator.c
        ${CMAKE_CURRENT_SOURCE_DIR}/client/commitfs.c
        ${CMAKE_CURRENT_SOURCE_DIR}/client/sessionfs.c
        ${CMAKE_CURRENT_SOURCE_DIR}/common/tangramfs-utils.c
        ${CMAKE_CURRENT_SOURCE_DIR}/common/seg_tree.c
        ${CMAKE_CURRENT_SOURCE_DIR}/common/lock-token.c
        ${CMAKE_CURRENT_SOURCE_DIR}/common/tangramfs-lock-manager.c
        ${CMAKE_CURRENT_SOURCE_DIR}/ucx/tangramfs-ucx-comm.c
        ${CMAKE_CURRENT_SOURCE_DIR}/ucx/tangramfs-ucx-server.c
        ${CMAKE_CURRENT_SOURCE_DIR}/ucx/tangramfs-ucx-client.c
        ${CMAKE_CURRENT_SOURCE_DIR}/ucx/tangramfs-ucx-delegator.c
        ${CMAKE_CURRENT_SOURCE_DIR}/ucx/tangramfs-ucx-taskmgr.c
        ${CMAKE_CURRENT_SOURCE_DIR}/ucx/tangramfs-ucx-rma.c)
add_library(tangramfs ${TANGRAMFS_SRCS})

target_include_directories(tangramfs
        PUBLIC "$<BUILD_INTERFACE:${TANGRAMFS_BUILD_INCLUDE_DEPENDENCIES}>"
        $<INSTALL_INTERFACE:${TANGRAMFS_INSTALL_INCLUDE_INTERFACE}>
        )

target_link_libraries(tangramfs
        PUBLIC ${TANGRAMFS_EXT_LIB_DEPENDENCIES}
        PRIVATE pthread
        )
target_compile_definitions(tangramfs
        PUBLIC _LARGEFILE64_SOURCE
        PRIVATE $<$<BOOL:${TANGRAMFS_PRELOAD}>:TANGRAMFS_PRELOAD>)

tangramfs_set_lib_options(tangramfs "tangramfs" ${TANGRAMFS_LIBTYPE})

#------------------------------------------------------------------------------
# Binaries - server, test
#------------------------------------------------------------------------------
set(TFS_SERVER_SRCS
        ${CMAKE_CURRENT_SOURCE_DIR}/server/tangramfs-metadata-manager.c
        ${CMAKE_CURRENT_SOURCE_DIR}/server/tangramfs-server.c
        ${CMAKE_CURRENT_SOURCE_DIR}/client/tangramfs-delegator.c
        ${CMAKE_CURRENT_SOURCE_DIR}/client/tangramfs-rpc.c
        ${CMAKE_CURRENT_SOURCE_DIR}/common/tangramfs-utils.c
        ${CMAKE_CURRENT_SOURCE_DIR}/common/lock-token.c
        ${CMAKE_CURRENT_SOURCE_DIR}/common/tangramfs-lock-manager.c
        ${CMAKE_CURRENT_SOURCE_DIR}/common/seg_tree.c
        ${CMAKE_CURRENT_SOURCE_DIR}/ucx/tangramfs-ucx-comm.c
        ${CMAKE_CURRENT_SOURCE_DIR}/ucx/tangramfs-ucx-server.c
        ${CMAKE_CURRENT_SOURCE_DIR}/ucx/tangramfs-ucx-delegator.c
        ${CMAKE_CURRENT_SOURCE_DIR}/ucx/tangramfs-ucx-taskmgr.c
        ${CMAKE_CURRENT_SOURCE_DIR}/ucx/tangramfs-ucx-client.c
        ${CMAKE_CURRENT_SOURCE_DIR}/ucx/tangramfs-ucx-rma.c)
add_executable(server ${TFS_SERVER_SRCS})
target_link_libraries(server
        PUBLIC ${TANGRAMFS_EXT_LIB_DEPENDENCIES}
        PUBLIC pthread)


#-----------------------------------------------------------------------------
# Specify project header files to be installed
#-----------------------------------------------------------------------------
set(TANGRAMFS_HEADERS
        ${CMAKE_SOURCE_DIR}/include/tangramfs.h
        ${CMAKE_SOURCE_DIR}/include/uthash.h
)
install(
        FILES
        ${TANGRAMFS_HEADERS}
        DESTINATION
        ${TANGRAMFS_INSTALL_INCLUDE_DIR}
        COMPONENT
        headers
)

#-----------------------------------------------------------------------------
# Specify targets to CMake Install
#-----------------------------------------------------------------------------
set(targets tangramfs server)
foreach(target ${targets})
    install(
        TARGETS
        ${target}    
        EXPORT
        ${TANGRAMFS_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${TANGRAMFS_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${TANGRAMFS_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${TANGRAMFS_INSTALL_BIN_DIR}
    )
endforeach()

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install for import into other projects
#-----------------------------------------------------------------------------
install(
        EXPORT
        ${TANGRAMFS_EXPORTED_TARGETS}
        DESTINATION
        ${TANGRAMFS_INSTALL_DATA_DIR}/cmake/tangramfs
        FILE
        ${TANGRAMFS_EXPORTED_TARGETS}.cmake
)

#-----------------------------------------------------------------------------
# Export all exported targets to the build tree for use by parent project
#-----------------------------------------------------------------------------
if(NOT TANGRAMFS_EXTERNALLY_CONFIGURED)
    EXPORT (
            TARGETS
            ${TANGRAMFS_EXPORTED_LIBS}
            FILE
            ${TANGRAMFS_EXPORTED_TARGETS}.cmake
    )
endif()


#------------------------------------------------------------------------------
# Set variables for parent scope
#------------------------------------------------------------------------------
# Used by config.cmake.build.in and Testing
set(TANGRAMFS_INCLUDES_BUILD_TIME
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_BINARY_DIR}
        ${TANGRAMFS_EXT_INCLUDE_DEPENDENCIES}
        PARENT_SCOPE
        )

# Used by config.cmake.install.in
set(TANGRAMFS_INCLUDES_INSTALL_TIME
        ${TANGRAMFS_INSTALL_INCLUDE_DIR}
        ${TANGRAMFS_EXT_INCLUDE_DEPENDENCIES}
        PARENT_SCOPE
        )
