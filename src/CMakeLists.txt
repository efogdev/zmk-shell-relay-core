target_sources_ifdef(CONFIG_ZMK_SHELL_RELAY_CORE app PRIVATE relay.c)
set_source_files_properties(relay.c PROPERTIES COMPILE_FLAGS "-Os")
