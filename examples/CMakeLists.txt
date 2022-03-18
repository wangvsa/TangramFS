#------------------------------------------------------------------------------
# Include source and build directories
#------------------------------------------------------------------------------
include_directories(${CMAKE_SOURCE_DIR}/include)

#------------------------------------------------------------------------------
# MPI
#------------------------------------------------------------------------------
find_package(MPI REQUIRED)
if(MPI_FOUND)
    include_directories(${MPI_CXX_INCLUDE_DIRS})
    set(TANGRAMFS_EXT_INCLUDE_DEPENDENCIES ${MPI_C_INCLUDE_DIRS}
            ${TANGRAMFS_EXT_INCLUDE_DEPENDENCIES}
            )
    set(TANGRAMFS_EXT_LIB_DEPENDENCIES
            ${MPI_C_LIBRARIES} ${TANGRAMFS_EXT_LIB_DEPENDENCIES})
else()
    message(STATUS, "MPI not found")
endif()

include_directories(${UCX_DIR}/include)


#------------------------------------------------------------------------------
# Example and Workflow
#------------------------------------------------------------------------------
add_executable(example example.c)
target_link_libraries(example
                      PUBLIC ${MPI_C_LIBRARIES}
                      tangramfs
                     )
add_dependencies(example tangramfs)

add_executable(iops_bench iops_bench.c)
target_link_libraries(iops_bench
                      PUBLIC ${MPI_C_LIBRARIES}
                      tangramfs
                     )
add_dependencies(iops_bench tangramfs)

add_executable(workflow workflow.c)
target_link_libraries(workflow
                      PUBLIC ${MPI_C_LIBRARIES}
                      tangramfs
                     )
add_dependencies(workflow tangramfs)


#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
set(targets example workflow iops_bench)
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

