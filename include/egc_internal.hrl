-define(WARN_EVERY, 900).
-define(warn(F, A), error_logger:warning_msg("[egc] " ++ F, A)).