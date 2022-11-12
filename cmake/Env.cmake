ob_define(DEBUG_PREFIX "-fdebug-prefix-map=${CMAKE_SOURCE_DIR}=.")
ob_define(OB_LD_BIN ld)
ob_define(ASAN_IGNORE_LIST "${CMAKE_SOURCE_DIR}/asan_ignore_list.txt")

ob_define(DEP_3RD_DIR "${CMAKE_SOURCE_DIR}/deps/3rd")
ob_define(DEVTOOLS_DIR "${CMAKE_SOURCE_DIR}/deps/3rd/usr/local/oceanbase/devtools")
ob_define(DEP_DIR "${CMAKE_SOURCE_DIR}/deps/3rd/usr/local/oceanbase/deps/devel")

ob_define(OB_BUILD_CDC OFF)
ob_define(OB_USE_CLANG ON)
ob_define(OB_USE_CCACHE ON)
ob_define(OB_ERRSIM OFF)
ob_define(BUILD_NUMBER 1)
ob_define(OB_GPERF_MODE OFF)
ob_define(OB_TRANS_ERRSIM OFF)
ob_define(OB_DIS_SEARRAY OFF)
ob_define(ENABLE_LATCH_DIAGNOSE OFF)
ob_define(ENABLE_MEMORY_DIAGNOSIS OFF)
ob_define(ENABLE_OBJ_LEAK_CHECK OFF)
ob_define(ENABLE_FATAL_ERROR_HANG ON)
ob_define(ENABLE_SMART_VAR_CHECK OFF)
ob_define(ENABLE_COMPILE_DLL_MODE OFF)
ob_define(OB_CMAKE_RULES_CHECK ON)
ob_define(OB_STATIC_LINK_LGPL_DEPS ON)
ob_define(HOTFUNC_PATH "${CMAKE_SOURCE_DIR}/hotfuncs.txt")

# 'ENABLE_PERF_MODE' use for offline system insight performance test
# PERF_MODE macro controls many special code path in system
# we can open this to benchmark our system partial/layered
ob_define(ENABLE_PERF_MODE OFF)

# begin of unity build config
ob_define(OB_MAX_UNITY_BATCH_SIZE 30)
# the global switch of unity build, defualt is 'ON'
ob_define(OB_ENABLE_UNITY ON)

if(WITH_COVERAGE)
  # -ftest-coverage to generate .gcno file
  # -fprofile-arcs to generate .gcda file
  # -DDBUILD_COVERAGE marco use to mark 'coverage build type' and to handle some speical case
  set(CMAKE_COVERAGE_FLAG "-ftest-coverage -fprofile-arcs -Xclang -coverage-version='502*' -DBUILD_COVERAGE")
  set(DEBUG_PREFIX "")
endif()

ob_define(AUTO_FDO_OPT "")
if(ENABLE_AUTO_FDO)
  set(AUTO_FDO_OPT "-fprofile-sample-use=${CMAKE_SOURCE_DIR}/observer.prof")
endif()


# should not use initial-exec for tls-model if building OBCDC.
if(NOT OB_BUILD_CDC)
  add_definitions(-DENABLE_INITIAL_EXEC_TLS_MODEL)
endif()

if (OB_USE_CCACHE)
  find_program(OB_CCACHE ccache
    PATHS "${DEVTOOLS_DIR}/bin"
    NO_DEFAULT_PATH)
  if (NOT OB_CCACHE)
    message(WARNING "CCACHE NOT FOUND, COMPILE CACHE MAY NOT WORK.")
  else()
    set(CMAKE_C_COMPILER_LAUNCHER ${OB_CCACHE})
    set(CMAKE_CXX_COMPILER_LAUNCHER ${OB_CCACHE})
  endif()
endif()

if (OB_USE_CLANG)
  find_program(OB_CC clang
  "${DEVTOOLS_DIR}/bin"
    NO_DEFAULT_PATH)
  find_program(OB_CXX clang++
  "${DEVTOOLS_DIR}/bin"
    NO_DEFAULT_PATH)
  find_file(GCC9 devtools
    PATHS ${CMAKE_SOURCE_DIR}/deps/3rd/usr/local/oceanbase
    NO_DEFAULT_PATH)
  set(_CMAKE_TOOLCHAIN_PREFIX llvm-)
  set(_CMAKE_TOOLCHAIN_LOCATION "${DEVTOOLS_DIR}/bin")

  if (OB_USE_ASAN)
    ob_define(CMAKE_ASAN_FLAG "-fstack-protector-strong -fsanitize=address -fno-optimize-sibling-calls -fsanitize-blacklist=${ASAN_IGNORE_LIST}")
  endif()

  if (OB_USE_LLD)
    set(LD_OPT "-fuse-ld=${DEVTOOLS_DIR}/bin/ld.lld")
    set(REORDER_COMP_OPT "-ffunction-sections -funique-internal-linkage-names -fdebug-info-for-profiling")
    set(REORDER_LINK_OPT "-Wl,--no-rosegment,--build-id=sha1,--no-warn-symbol-ordering,--symbol-ordering-file,${HOTFUNC_PATH}")
    set(OB_LD_BIN "${DEVTOOLS_DIR}/bin/ld.lld")
  endif()
  set(CMAKE_CXX_FLAGS "--gcc-toolchain=${GCC9} ${DEBUG_PREFIX} ${AUTO_FDO_OPT} -fcolor-diagnostics ${REORDER_COMP_OPT} -fmax-type-align=8 ${CMAKE_ASAN_FLAG} ${CMAKE_COVERAGE_FLAG} -std=gnu++11")
  set(CMAKE_C_FLAGS "--gcc-toolchain=${GCC9} ${DEBUG_PREFIX} ${AUTO_FDO_OPT} -fcolor-diagnostics ${REORDER_COMP_OPT} -fmax-type-align=8 ${CMAKE_ASAN_FLAG} ${CMAKE_COVERAGE_FLAG}")
  set(CMAKE_CXX_LINK_FLAGS "${LD_OPT} --gcc-toolchain=${GCC9} ${DEBUG_PREFIX} ${AUTO_FDO_OPT}")
  set(CMAKE_SHARED_LINKER_FLAGS "${LD_OPT} -Wl,-z,noexecstack ${REORDER_LINK_OPT}")
  set(CMAKE_EXE_LINKER_FLAGS "${LD_OPT} -Wl,-z,noexecstack ${REORDER_LINK_OPT}")
else() # not clang, use gcc
  message("gcc9 not support currently, please set OB_USE_CLANG ON and we will finish it as soon as possible")
endif()

if (OB_CC AND OB_CXX)
  set(CMAKE_C_COMPILER ${OB_CC})
  set(CMAKE_CXX_COMPILER ${OB_CXX})
else()
  message(FATAL_ERROR "can't find suitable compiler")
endif()


option(OB_ENABLE_AVX2 "enable AVX2 and related instruction set support for x86_64" OFF)

include(CMakeFindBinUtils)
EXECUTE_PROCESS(COMMAND uname -m COMMAND tr -d '\n' OUTPUT_VARIABLE ARCHITECTURE )
if( ${ARCHITECTURE} STREQUAL "x86_64" )
    set(MTUNE_CFLAGS -mtune=core2)
    set(ARCH_LDFLAGS "")
    set(OCI_DEVEL_INC "${DEP_3RD_DIR}/usr/include/oracle/11.2/client64")
    add_compile_options(-DRDMA_ENABLED)
    set(rdma_lib_deps "reasy" )
else()
    set(MARCH_CFLAGS "-march=armv8-a+crc" )
    set(MTUNE_CFLAGS "-mtune=generic" )
    set(ARCH_LDFLAGS "-l:libatomic.a")
    set(OCI_DEVEL_INC "${DEP_3RD_DIR}/usr/include/oracle/19.10/client64")
endif()

EXECUTE_PROCESS(COMMAND grep -Po "release [0-9]{1}" /etc/redhat-release COMMAND awk "{print $2}" COMMAND tr -d '\n' OUTPUT_VARIABLE KERNEL_RELEASE ERROR_QUIET)
