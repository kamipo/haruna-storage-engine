find_package(PkgConfig)

pkg_check_modules(PFICOMMON REQUIRED pficommon)

INCLUDE_DIRECTORIES(${PFICOMMON_INCLUDE_DIRS})
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fimplicit-templates")

SET(HARUNA_PLUGIN_DYNAMIC "haruna")
SET(HARUNA_PLUGIN_MANDATORY TRUE)

SET(HARUNA_SOURCES ha_haruna.cc)
MYSQL_ADD_PLUGIN(haruna ${HARUNA_SOURCES} STORAGE_ENGINE MODULE_ONLY LINK_LIBRARIES ${PFICOMMON_LIBRARIES})
