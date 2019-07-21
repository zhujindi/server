--disable_query_log
--disable_warnings
drop procedure if exists verify_trt;
drop procedure if exists verify_trt_dummy;
drop function if exists current_row;
drop function if exists check_row;
set @@global.time_zone= default;
--enable_warnings
--enable_query_log
