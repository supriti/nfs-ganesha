add_definitions(
  -D__USE_GNU
  -D_GNU_SOURCE
)

set(LIB_PREFIX 64)

add_subdirectory(os)

if(USE_FSAL_VFS OR USE_FSAL_LUSTRE)
    add_subdirectory(vfs)
endif(USE_FSAL_VFS OR USE_FSAL_LUSTRE)

if(USE_FSAL_XFS)
    add_subdirectory(xfs)
endif(USE_FSAL_XFS)
if(USE_FSAL_PANFS)
    add_subdirectory(panfs)
endif(USE_FSAL_PANFS)

########### next target ###############



########### install files ###############
