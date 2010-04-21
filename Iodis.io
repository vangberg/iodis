Iodis := Object clone do(
  version   := "0.1"

  debug     ::= false

  host      ::= "localhost"
  port      ::= 6379
  password  ::= nil

  ServerTimeout := Error clone

  connect := method(
    self socket := Socket clone setHost(host) setPort(port) connect
    if(socket isError, writeln("Error connection to Redis."); exit)
    if(password, callCommand("auth", password))
    self
  )

  callCommand := method(
    args      := call evalArgs flatten
    command   := args removeFirst
    request   := multiBulkRequest(command, args)

    if(debug, ("C: " .. request) print)

    socket write(request)
    responseProcessor perform(command) call(readResponse)
  )

  multiBulkRequest := method(command, args,
    args prepend(command asUppercase)

    request := ("*" .. args size .. "\r\n") asMutable

    args foreach(arg,
      bytes := arg asString size
      request appendSeq("$" .. bytes .. "\r\n")
      request appendSeq(arg .. "\r\n")
    )

    request
  )

  readResponse := method(
    response      := socket readUntilSeq("\r\n")
    responseType  := response exSlice(0, 1)
    line          := response exSlice(1)

    responseType switch(
      "+",  line,
      ":",  line asNumber,
      "$",  line asNumber compare(0) switch(
                -1, nil,
                 0, socket readBytes(2)
                    "",
                 1, data := socket readBytes(line asNumber)
                    socket readBytes(2)
                    data),
      "*",  if (line asNumber < 0, nil,
                values := list()
                line asNumber repeat(
                  values push(readResponse)
                )
                values),
      "-", Exception raise("-" .. line)
    ) 
  )

  commands := list(
    "auth", "exists", "del", "type", "keys", "randomkey", "rename", "renamenx",
    "dbsize", "expire", "expireat", "ttl", "select", "move", "flushdb",
    "flushall", "quit",

    "get", "mget", "incr", "incrby", "decr", "decrby",

    "lrange", "llen", "ltrim", "lindex", "lpop", "rpop", "blpop", "brpop",
    "rpoplpush",

    "spop", "scard", "sinter", "sinterstore", "sunion", "sunionstore",
    "sdiff", "sdiffstore", "smembers", "srandmember",

    "set", "getset", "setnx",

    "rpush", "lpush", "lset", "lrem",

    "sadd", "srem", "smove", "sismember",

    "mset", "msetnx"
  )

  commands foreach(command,
    if(hasSlot(command), continue)

    newSlot(command, method(
      callCommand(call message name, call evalArgs)
    ))
  )

  # We don't want to override Io's `type`, so the Redis-command `type` is
  # wrapped in this method instead.
  typeOf := method(key,
    callCommand("type", key)
  )

  responseProcessor := Object clone do(
    Boolean   := block(r, r == 1)

    flushdb   := Boolean
    exists    := Boolean
    keys      := block(r, r split(" "))
    renamenx  := Boolean
    expire    := Boolean
    expireat  := Boolean
    ttl       := block(r, if(r < 0, nil, r))
    move      := Boolean
    setnx     := Boolean
    msetnx    := Boolean
    sadd      := Boolean
    srem      := Boolean
    smove     := Boolean
    sismember := Boolean
    type      := block(r, r)

    forward   := block(r, r)
  )
)
