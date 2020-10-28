namespace go admin 

struct create_app_options
{
    1:i32              partition_count;
    2:i32              replica_count;
    3:bool             success_if_exist;
    4:string           app_type;
    5:bool             is_stateful;
    6:map<string, string>  envs;
}

struct create_app_request
{
    1:string                   app_name;
    2:create_app_options       options;
}

struct drop_app_options
{
    1:bool             success_if_not_exist;
    2:optional i64     reserve_seconds;
}

struct drop_app_request
{
    1:string                   app_name;
    2:drop_app_options         options;
}
