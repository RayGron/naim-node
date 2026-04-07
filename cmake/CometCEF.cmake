function(comet_configure_cef_support)
  if(NOT COMET_ENABLE_CEF_BROWSING)
    set(COMET_WITH_CEF OFF PARENT_SCOPE)
    return()
  endif()

  if(NOT UNIX OR APPLE)
    message(FATAL_ERROR "COMET_ENABLE_CEF_BROWSING is currently supported only on Linux")
  endif()

  set(_comet_cef_root "${COMET_CEF_ROOT}")
  if(NOT _comet_cef_root AND COMET_CEF_DOWNLOAD_URL)
    set(_comet_cef_archive_dir "${CMAKE_BINARY_DIR}/_deps/cef")
    set(_comet_cef_archive "${_comet_cef_archive_dir}/cef-binary-distribution.archive")
    set(_comet_cef_extract_dir "${_comet_cef_archive_dir}/unpacked")
    file(MAKE_DIRECTORY "${_comet_cef_archive_dir}")
    message(STATUS "Downloading CEF binary distribution from ${COMET_CEF_DOWNLOAD_URL}")
    file(
      DOWNLOAD
      "${COMET_CEF_DOWNLOAD_URL}"
      "${_comet_cef_archive}"
      SHOW_PROGRESS
      STATUS _comet_cef_download_status
      TLS_VERIFY ON
    )
    list(GET _comet_cef_download_status 0 _comet_cef_download_code)
    list(GET _comet_cef_download_status 1 _comet_cef_download_message)
    if(NOT _comet_cef_download_code EQUAL 0)
      message(FATAL_ERROR "Failed to download CEF binary distribution: ${_comet_cef_download_message}")
    endif()

    file(REMOVE_RECURSE "${_comet_cef_extract_dir}")
    file(MAKE_DIRECTORY "${_comet_cef_extract_dir}")
    file(ARCHIVE_EXTRACT INPUT "${_comet_cef_archive}" DESTINATION "${_comet_cef_extract_dir}")

    set(_comet_cef_candidate_root "${_comet_cef_extract_dir}")
    if(NOT EXISTS "${_comet_cef_candidate_root}/include/cef_app.h")
      file(GLOB _comet_cef_children LIST_DIRECTORIES true "${_comet_cef_extract_dir}/*")
      foreach(_comet_cef_child IN LISTS _comet_cef_children)
        if(EXISTS "${_comet_cef_child}/include/cef_app.h")
          set(_comet_cef_candidate_root "${_comet_cef_child}")
          break()
        endif()
      endforeach()
    endif()

    set(_comet_cef_root "${_comet_cef_candidate_root}")
    set(COMET_CEF_ROOT "${_comet_cef_root}" CACHE PATH "Path to an unpacked CEF binary distribution" FORCE)
  endif()

  if(NOT _comet_cef_root)
    message(FATAL_ERROR
      "CEF browsing was enabled but no CEF binary distribution was provided. "
      "Set COMET_CEF_ROOT to an unpacked distribution or COMET_CEF_DOWNLOAD_URL to an archive URL.")
  endif()

  if(NOT EXISTS "${_comet_cef_root}/include/cef_app.h")
    message(FATAL_ERROR "COMET_CEF_ROOT does not look like a valid CEF binary distribution: ${_comet_cef_root}")
  endif()
  if(NOT EXISTS "${_comet_cef_root}/Release/libcef.so")
    message(FATAL_ERROR "CEF binary distribution is missing Release/libcef.so: ${_comet_cef_root}")
  endif()

  add_library(comet-cef SHARED IMPORTED GLOBAL)
  set_target_properties(
    comet-cef
    PROPERTIES
      IMPORTED_LOCATION "${_comet_cef_root}/Release/libcef.so"
      INTERFACE_INCLUDE_DIRECTORIES "${_comet_cef_root}"
  )

  file(
    GLOB_RECURSE
    _comet_cef_wrapper_sources
    CONFIGURE_DEPENDS
    "${_comet_cef_root}/libcef_dll/*.cc"
    "${_comet_cef_root}/libcef_dll/*.cpp"
  )
  if(NOT _comet_cef_wrapper_sources)
    message(FATAL_ERROR "CEF binary distribution is missing libcef_dll wrapper sources: ${_comet_cef_root}")
  endif()

  add_library(comet-cef-wrapper STATIC ${_comet_cef_wrapper_sources})
  target_include_directories(
    comet-cef-wrapper
    PUBLIC
      "${_comet_cef_root}"
      "${_comet_cef_root}/libcef_dll"
  )
  target_compile_definitions(comet-cef-wrapper PRIVATE WRAPPING_CEF_SHARED=1)
  target_link_libraries(comet-cef-wrapper PUBLIC comet-cef)

  set(COMET_WITH_CEF ON PARENT_SCOPE)
  set(COMET_CEF_ROOT_RESOLVED "${_comet_cef_root}" PARENT_SCOPE)
endfunction()

function(comet_stage_cef_runtime target_name)
  if(NOT COMET_WITH_CEF)
    return()
  endif()

  set(_comet_cef_root "${COMET_CEF_ROOT_RESOLVED}")
  if(NOT _comet_cef_root)
    message(FATAL_ERROR "COMET_WITH_CEF is ON but COMET_CEF_ROOT_RESOLVED is empty")
  endif()

  add_custom_command(
    TARGET ${target_name}
    POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
            "${_comet_cef_root}/Release/libcef.so"
            "$<TARGET_FILE_DIR:${target_name}>/libcef.so"
  )

  foreach(_cef_release_file chrome-sandbox libEGL.so libGLESv2.so vk_swiftshader_icd.json)
    if(EXISTS "${_comet_cef_root}/Release/${_cef_release_file}")
      add_custom_command(
        TARGET ${target_name}
        POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
                "${_comet_cef_root}/Release/${_cef_release_file}"
                "$<TARGET_FILE_DIR:${target_name}>/${_cef_release_file}"
      )
    endif()
  endforeach()

  foreach(_cef_release_dir swiftshader)
    if(EXISTS "${_comet_cef_root}/Release/${_cef_release_dir}")
      add_custom_command(
        TARGET ${target_name}
        POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_directory
                "${_comet_cef_root}/Release/${_cef_release_dir}"
                "$<TARGET_FILE_DIR:${target_name}>/${_cef_release_dir}"
      )
    endif()
  endforeach()

  foreach(_cef_resource_entry icudtl.dat snapshot_blob.bin v8_context_snapshot.bin resources.pak chrome_100_percent.pak chrome_200_percent.pak locales)
    if(EXISTS "${_comet_cef_root}/Resources/${_cef_resource_entry}")
      if(IS_DIRECTORY "${_comet_cef_root}/Resources/${_cef_resource_entry}")
        add_custom_command(
          TARGET ${target_name}
          POST_BUILD
          COMMAND ${CMAKE_COMMAND} -E copy_directory
                  "${_comet_cef_root}/Resources/${_cef_resource_entry}"
                  "$<TARGET_FILE_DIR:${target_name}>/${_cef_resource_entry}"
        )
      else()
        add_custom_command(
          TARGET ${target_name}
          POST_BUILD
          COMMAND ${CMAKE_COMMAND} -E copy_if_different
                  "${_comet_cef_root}/Resources/${_cef_resource_entry}"
                  "$<TARGET_FILE_DIR:${target_name}>/${_cef_resource_entry}"
        )
      endif()
    endif()
  endforeach()

  foreach(_cef_release_resource snapshot_blob.bin v8_context_snapshot.bin)
    if(EXISTS "${_comet_cef_root}/Release/${_cef_release_resource}")
      add_custom_command(
        TARGET ${target_name}
        POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
                "${_comet_cef_root}/Release/${_cef_release_resource}"
                "$<TARGET_FILE_DIR:${target_name}>/${_cef_release_resource}"
      )
    endif()
  endforeach()
endfunction()
