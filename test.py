from langgraph.checkpoint.mysql.pymysql import PyMySQLSaver

write_config = {"configurable": {"thread_id": "1", "checkpoint_ns": ""}}
read_config = {"configurable": {"thread_id": "1"}}

DB_URI = "mysql://root:mysql123@172.16.1.223:3306/db_zeus?charset=utf8mb4"
with PyMySQLSaver.from_conn_string(DB_URI) as checkpointer:
    # call .setup() the first time you're using the checkpointer
    checkpointer.setup()
    checkpoint = {
        "v": 4,
        "ts": "2024-07-31T20:14:19.804150+00:00",
        "id": "1ef4f797-8335-6428-8001-8a1503f9b875",
        "channel_values": {
            "my_key": "meow",
            "node": "node"
        },
        "channel_versions": {
            "__start__": 2,
            "my_key": 3,
            "start:node": 3,
            "node": 3
        },
        "versions_seen": {
            "__input__": {},
            "__start__": {
            "__start__": 1
            },
            "node": {
            "start:node": 2
            }
        },
    }
    import pdb; pdb.set_trace()
    # store checkpoint
    a = checkpointer.put(write_config, checkpoint, {}, {})

    # load checkpoint
    b = checkpointer.get(read_config)

    # list checkpoints
    c = list(checkpointer.list(read_config))