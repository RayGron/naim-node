function(naim_configure_cef_support)
  if(NOT NAIM_ENABLE_CEF_BROWSING)
    set(NAIM_WITH_CEF OFF PARENT_SCOPE)
    return()
  endif()

  if(NOT UNIX OR APPLE)
    message(FATAL_ERROR "NAIM_ENABLE_CEF_BROWSING is currently supported only on Linux")
  endif()

  set(_naim_cef_root "${NAIM_CEF_ROOT}")
  if(NOT _naim_cef_root AND NAIM_CEF_DOWNLOAD_URL)
    set(_naim_cef_archive_dir "${CMAKE_BINARY_DIR}/_deps/cef")
    set(_naim_cef_archive "${_naim_cef_archive_dir}/cef-binary-distribution.archive")
    set(_naim_cef_extract_dir "${_naim_cef_archive_dir}/unpacked")
    file(MAKE_DIRECTORY "${_naim_cef_archive_dir}")
    message(STATUS "Downloading CEF binary distribution from ${NAIM_CEF_DOWNLOAD_URL}")
    file(
      DOWNLOAD
      "${NAIM_CEF_DOWNLOAD_URL}"
      "${_naim_cef_archive}"
      SHOW_PROGRESS
      STATUS _naim_cef_download_status
      TLS_VERIFY ON
    )
    list(GET _naim_cef_download_status 0 _naim_cef_download_code)
    list(GET _naim_cef_download_status 1 _naim_cef_download_message)
    if(NOT _naim_cef_download_code EQUAL 0)
      message(FATAL_ERROR "Failed to download CEF binary distribution: ${_naim_cef_download_message}")
    endif()

    file(REMOVE_RECURSE "${_naim_cef_extract_dir}")
    file(MAKE_DIRECTORY "${_naim_cef_extract_dir}")
    file(ARCHIVE_EXTRACT INPUT "${_naim_cef_archive}" DESTINATION "${_naim_cef_extract_dir}")

    set(_naim_cef_candidate_root "${_naim_cef_extract_dir}")
    if(NOT EXISTS "${_naim_cef_candidate_root}/include/cef_app.h")
      file(GLOB _naim_cef_children LIST_DIRECTORIES true "${_naim_cef_extract_dir}/*")
      foreach(_naim_cef_child IN LISTS _naim_cef_children)
        if(EXISTS "${_naim_cef_child}/include/cef_app.h")
          set(_naim_cef_candidate_root "${_naim_cef_child}")
          break()
        endif()
      endforeach()
    endif()

    set(_naim_cef_root "${_naim_cef_candidate_root}")
    set(NAIM_CEF_ROOT "${_naim_cef_root}" CACHE PATH "Path to an unpacked CEF binary distribution" FORCE)
  endif()

  if(NOT _naim_cef_root)
    message(FATAL_ERROR
      "CEF browsing was enabled but no CEF binary distribution was provided. "
      "Set NAIM_CEF_ROOT to an unpacked distribution or NAIM_CEF_DOWNLOAD_URL to an archive URL.")
  endif()

  if(NOT EXISTS "${_naim_cef_root}/include/cef_app.h")
    message(FATAL_ERROR "NAIM_CEF_ROOT does not look like a valid CEF binary distribution: ${_naim_cef_root}")
  endif()
  if(NOT EXISTS "${_naim_cef_root}/Release/libcef.so")
    message(FATAL_ERROR "CEF binary distribution is missing Release/libcef.so: ${_naim_cef_root}")
  endif()

  add_library(naim-cef SHARED IMPORTED GLOBAL)
  set_target_properties(
    naim-cef
    PROPERTIES
      IMPORTED_LOCATION "${_naim_cef_root}/Release/libcef.so"
      INTERFACE_INCLUDE_DIRECTORIES "${_naim_cef_root}"
  )

  file(
    GLOB_RECURSE
    _naim_cef_wrapper_sources
    CONFIGURE_DEPENDS
    "${_naim_cef_root}/libcef_dll/*.cc"
    "${_naim_cef_root}/libcef_dll/*.cpp"
  )
  if(NOT _naim_cef_wrapper_sources)
    message(FATAL_ERROR "CEF binary distribution is missing libcef_dll wrapper sources: ${_naim_cef_root}")
  endif()

  add_library(naim-cef-wrapper STATIC ${_naim_cef_wrapper_sources})
  target_include_directories(
    naim-cef-wrapper
    PUBLIC
      "${_naim_cef_root}"
      "${_naim_cef_root}/libcef_dll"
  )
  target_compile_definitions(naim-cef-wrapper PRIVATE WRAPPING_CEF_SHARED=1)
  target_link_libraries(naim-cef-wrapper PUBLIC naim-cef)

  set(NAIM_WITH_CEF ON PARENT_SCOPE)
  set(NAIM_CEF_ROOT_RESOLVED "${_naim_cef_root}" PARENT_SCOPE)
endfunction()

function(naim_stage_cef_runtime target_name)
  if(NOT NAIM_WITH_CEF)
    return()
  endif()

  set(_naim_cef_root "${NAIM_CEF_ROOT_RESOLVED}")
  if(NOT _naim_cef_root)
    message(FATAL_ERROR "NAIM_WITH_CEF is ON but NAIM_CEF_ROOT_RESOLVED is empty")
  endif()

  add_custom_command(
    TARGET ${target_name}
    POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "${_naim_cef_root}/Release/libcef.so"
            "$<TARGET_FILE_DIR:${target_name}>/libcef.so"
  )

  foreach(_cef_release_file chrome-sandbox libEGL.so libGLESv2.so vk_swiftshader_icd.json)
    if(EXISTS "${_naim_cef_root}/Release/${_cef_release_file}")
      add_custom_command(
        TARGET ${target_name}
        POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
                "${_naim_cef_root}/Release/${_cef_release_file}"
                "$<TARGET_FILE_DIR:${target_name}>/${_cef_release_file}"
      )
    endif()
  endforeach()

  foreach(_cef_release_dir swiftshader)
    if(EXISTS "${_naim_cef_root}/Release/${_cef_release_dir}")
      add_custom_command(
        TARGET ${target_name}
        POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_directory
                "${_naim_cef_root}/Release/${_cef_release_dir}"
                "$<TARGET_FILE_DIR:${target_name}>/${_cef_release_dir}"
      )
    endif()
  endforeach()

  foreach(_cef_resource_entry icudtl.dat snapshot_blob.bin v8_context_snapshot.bin resources.pak chrome_100_percent.pak chrome_200_percent.pak locales)
    if(EXISTS "${_naim_cef_root}/Resources/${_cef_resource_entry}")
      if(IS_DIRECTORY "${_naim_cef_root}/Resources/${_cef_resource_entry}")
        add_custom_command(
          TARGET ${target_name}
          POST_BUILD
          COMMAND ${CMAKE_COMMAND} -E copy_directory
                  "${_naim_cef_root}/Resources/${_cef_resource_entry}"
                  "$<TARGET_FILE_DIR:${target_name}>/${_cef_resource_entry}"
        )
      else()
        add_custom_command(
          TARGET ${target_name}
          POST_BUILD
          COMMAND ${CMAKE_COMMAND} -E copy_if_different
                  "${_naim_cef_root}/Resources/${_cef_resource_entry}"
                  "$<TARGET_FILE_DIR:${target_name}>/${_cef_resource_entry}"
        )
      endif()
    endif()
  endforeach()

  foreach(_cef_release_resource snapshot_blob.bin v8_context_snapshot.bin)
    if(EXISTS "${_naim_cef_root}/Release/${_cef_release_resource}")
      add_custom_command(
        TARGET ${target_name}
        POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
                "${_naim_cef_root}/Release/${_cef_release_resource}"
                "$<TARGET_FILE_DIR:${target_name}>/${_cef_release_resource}"
      )
    endif()
  endforeach()
endfunction()
